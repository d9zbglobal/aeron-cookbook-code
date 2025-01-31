package com.global.mdc;

import io.aeron.Aeron;
import io.aeron.ChannelUriStringBuilder;
import io.aeron.Publication;
import io.aeron.archive.Archive;
import io.aeron.archive.ArchiveThreadingMode;
import io.aeron.archive.ArchivingMediaDriver;
import io.aeron.archive.checksum.Checksum;
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.codecs.SourceLocation;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import org.agrona.CloseHelper;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;

import static io.aeron.CommonContext.MDC_CONTROL_MODE_DYNAMIC;
import static io.aeron.CommonContext.UDP_MEDIA;

public class MultiDestinationPublisherAgent implements Agent
{
    private static final String SNAPSHOT_CHANNEL = "aeron:ipc"; // For state snapshots
    private static final int MESSAGE_STREAM_ID = 100;  // Outgoing messages to peer/clients
    private static final int SNAPSHOT_STREAM_ID = 101; // Internal stream for archiving snapshots
    private static final int RECORDING_EVENTS_STREAM_ID = 102; // Stream for informing standby of new snapshot

    private static final EpochClock CLOCK = SystemEpochClock.INSTANCE;
    private static final Logger LOGGER = LoggerFactory.getLogger(MultiDestinationPublisherAgent.class);

    private final Aeron aeron;
    private final AeronArchive archive;
    private final MutableDirectBuffer mutableDirectBuffer;
    private final Publication messageMDCPublication;
    private final Publication snapshotPublication;

    private final ShutdownSignalBarrier barrier;
    private long nextAppend = 0;
    private long lastSeq = 0;
    private long totalSum = 1_000_000;
    private long snapshotNumber = 0;

    public MultiDestinationPublisherAgent(final String publisherControlHost, final int publisherControlPort,
        final ShutdownSignalBarrier barrier)
    {
        this.mutableDirectBuffer = new UnsafeBuffer(ByteBuffer.allocateDirect(Long.BYTES));

        LOGGER.info("Launching ArchivingMediaDriver");

        final MediaDriver.Context mediaDriverContext = new MediaDriver.Context()
            .aeronDirectoryName("/Users/dan.scarborough/dev/aeron-files/driver")
            .dirDeleteOnStart(true)
            .threadingMode(ThreadingMode.SHARED)
            .sharedIdleStrategy(new SleepingMillisIdleStrategy());
        final String snapshotAvailablePublicationChannel = new ChannelUriStringBuilder().media(UDP_MEDIA)
            .controlEndpoint(publisherControlHost + ":" + (publisherControlPort+1))
            .controlMode(MDC_CONTROL_MODE_DYNAMIC)
            .minFlowControl(1, null)
            .build();
        final Archive.Context archiveContext = new Archive.Context()
            .aeronDirectoryName(mediaDriverContext.aeronDirectoryName())
            .archiveDirectoryName("/Users/dan.scarborough/dev/aeron-files/archive")
            .recordingEventsEnabled(true)
            .recordingEventsChannel(snapshotAvailablePublicationChannel)
            .recordingEventsStreamId(RECORDING_EVENTS_STREAM_ID)
            .deleteArchiveOnStart(false)
            .threadingMode(ArchiveThreadingMode.SHARED)
            .idleStrategySupplier(SleepingMillisIdleStrategy::new)
            .controlChannel("aeron:udp?endpoint=localhost:8010")
            .replicationChannel("aeron:udp?endpoint=localhost:8012");

        ArchivingMediaDriver.launch(mediaDriverContext, archiveContext);

        this.aeron = Aeron.connect(new Aeron.Context()
            .aeronDirectoryName(mediaDriverContext.aeronDirectoryName())
            .idleStrategy(new SleepingMillisIdleStrategy()));

        final String mdcMessagePublicationChannel = new ChannelUriStringBuilder().media(UDP_MEDIA)
            .controlEndpoint(publisherControlHost + ":" + publisherControlPort)
            .controlMode(MDC_CONTROL_MODE_DYNAMIC)
            .minFlowControl(2, null)
            .channelSendTimestampOffset("reserved")
            .receiverWindowLength(1024 * 64)
            .build();

        LOGGER.info("Creating publication {}", mdcMessagePublicationChannel);
        messageMDCPublication = aeron.addPublication(mdcMessagePublicationChannel, MESSAGE_STREAM_ID);
        snapshotPublication = aeron.addPublication(SNAPSHOT_CHANNEL, SNAPSHOT_STREAM_ID);

        LOGGER.info("Term length {}, available window {}",
            messageMDCPublication.termBufferLength(), messageMDCPublication.availableWindow());

        // Step 4: Connect to Archive Client and Start Recording
        archive = AeronArchive.connect(new AeronArchive.Context()
            .aeronDirectoryName(mediaDriverContext.aeronDirectoryName())
            .recordingEventsChannel(snapshotAvailablePublicationChannel)
            .recordingEventsStreamId(RECORDING_EVENTS_STREAM_ID)
            .controlRequestChannel(archiveContext.controlChannel()) // Sends commands
            .controlResponseChannel("aeron:udp?endpoint=localhost:8011"));

        LOGGER.info("Archive directory: {}", archiveContext.archiveDirectoryName());
        LOGGER.info("Requesting recording for channel: {}, streamId: {}", mdcMessagePublicationChannel,
            MESSAGE_STREAM_ID);
        archive.startRecording(mdcMessagePublicationChannel, MESSAGE_STREAM_ID, SourceLocation.LOCAL);
        archive.startRecording(SNAPSHOT_CHANNEL, SNAPSHOT_STREAM_ID, SourceLocation.LOCAL);
        LOGGER.info("Recording started for messages and snapshots.");

        this.barrier = barrier;
    }

    private void errorHandler(final Throwable throwable)
    {
        LOGGER.error("Unexpected failure {}", throwable.getMessage(), throwable);
    }

    @Override
    public void onStart()
    {
        LOGGER.info("Starting up");
        Agent.super.onStart();
    }

    @Override
    public int doWork()
    {
        if (CLOCK.time() >= nextAppend)
        {
            if (messageMDCPublication.isConnected())
            {
                lastSeq += 1;
                totalSum += 2; // Update total sum

                if (lastSeq % 250 == 0)
                {
                    archiveSnapshot();
                }

                mutableDirectBuffer.putLong(0, lastSeq);
                final long offer = messageMDCPublication.offer(mutableDirectBuffer, 0, Long.BYTES);
                if (offer < 0)
                {
                    LOGGER.error("Unexpected failure {}", Publication.errorString(offer));
                    barrier.signal();
                }

                LOGGER.info("Appended {}, total sum: {}", lastSeq, totalSum);
            }
            else
            {
                if (nextAppend % 1000 == 0)
                {
                    LOGGER.info("Awaiting subscribers...");
                }
            }
            nextAppend = CLOCK.time();
            if (lastSeq == 10000)
            {
                barrier.signal();
            }
        }
        return 0;
    }

    private void archiveSnapshot()
    {
        // ✅ Serialize snapshot data correctly at offset 0
        this.mutableDirectBuffer.putLong(0, totalSum);

        // ✅ Ensure snapshotPublication is connected before writing
        if (!snapshotPublication.isConnected())
        {
            LOGGER.warn("Snapshot publication is not connected, skipping snapshot.");
            return;
        }

        // ✅ Offer snapshot data to the archive at EXACTLY offset 0
        long result;
        do
        {
            result = snapshotPublication.offer(this.mutableDirectBuffer, 0, Long.BYTES); // 🔥 Ensure offset = 0
        }
        while (result < 0);
        snapshotNumber++;
        LOGGER.info("SP: Snapshot stored - totalSum {}", totalSum);
    }


    @Override
    public String roleName()
    {
        return "mdc-publisher";
    }

    @Override
    public void onClose()
    {
        LOGGER.info("Shutting down");
        CloseHelper.quietClose(messageMDCPublication);
        CloseHelper.quietClose(snapshotPublication);
        CloseHelper.quietClose(aeron);
    }
}
