package com.global.mdc;

import io.aeron.Aeron;
import io.aeron.ChannelUriStringBuilder;
import io.aeron.Subscription;
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.client.RecordingEventsAdapter;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import org.agrona.CloseHelper;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.Agent;
import org.agrona.concurrent.SleepingMillisIdleStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static io.aeron.CommonContext.MDC_CONTROL_MODE_DYNAMIC;
import static io.aeron.CommonContext.UDP_MEDIA;

public class BackupSubscriberAgent implements Agent
{
    private static final Logger LOGGER = LoggerFactory.getLogger(BackupSubscriberAgent.class);
    private final MediaDriver mediaDriver;
    private final Aeron aeron;
    private final Subscription mdcSubscription;
    private int notificationCounter = 0;
//    private final AeronArchive archive;
    private final RecordingEventsAdapter recordingEventsAdapter;

    public BackupSubscriberAgent(final String publisherControlHost, final int publisherControlPort,
        final int streamId)
    {
        LOGGER.info("Launching media driver");
        this.mediaDriver = MediaDriver.launch(new MediaDriver.Context()
            .dirDeleteOnStart(true)
            .threadingMode(ThreadingMode.SHARED)
            .sharedIdleStrategy(new SleepingMillisIdleStrategy()));

        this.aeron = Aeron.connect(new Aeron.Context()
            .aeronDirectoryName(mediaDriver.aeronDirectoryName())
            .idleStrategy(new SleepingMillisIdleStrategy()));


//        // ✅ Connect to the local archive
//        this.archive = AeronArchive.connect(new AeronArchive.Context()
//            .controlRequestChannel("aeron:ipc")
//            .controlResponseChannel("aeron:ipc"));

        final String channel = new ChannelUriStringBuilder().media(UDP_MEDIA)
            .endpoint("0.0.0.0:0") // an ephemeral port on the local machine
            .controlEndpoint(publisherControlHost + ":" + publisherControlPort)
            .controlMode(MDC_CONTROL_MODE_DYNAMIC).build();

        LOGGER.info("Adding the subscription to channel: {}", channel);
        this.mdcSubscription = aeron.addSubscription(channel, streamId);

        final ArchiveProgressListener progressListener = new ArchiveProgressListener();
        recordingEventsAdapter = new RecordingEventsAdapter(progressListener, mdcSubscription, 10);
    }

    @Override
    public int doWork()
    {
//        return mdcSubscription.poll(this::handleSnapshotAdvertisement, 10);
        return recordingEventsAdapter.poll();
    }

    private void handleSnapshotAdvertisement(DirectBuffer buffer, int offset, int length, io.aeron.logbuffer.Header header)
    {
        if (length < Long.BYTES * 2)
        {
            LOGGER.warn("Received invalid snapshot advertisement, skipping.");
            return;
        }

        long recordingId = buffer.getLong(offset);
        LOGGER.info("Received snapshot advertisement: recordingId={}", recordingId);
//        replicateSnapshot(recordingId);
    }

//    private void replicateSnapshot(long recordingId)
//    {
//        LOGGER.info("Starting snapshot replication for recordingId={}", recordingId);
//
//        long replicationId = archive.replicate(recordingId,
//            "aeron:udp?endpoint=localhost:8010", // Publisher's archive source
//            "aeron:ipc"); // Local archive destination
//
//        while (!archive.getReplicationProgress(replicationId).isDone())
//        {
//            new SleepingMillisIdleStrategy().idle();
//        }
//
//        LOGGER.info("Snapshot replication complete for recordingId={}", recordingId);
//    }

    @Override
    public String roleName()
    {
        return "backup-subscriber";
    }

    @Override
    public void onStart()
    {
        Agent.super.onStart();
        LOGGER.info("Backup subscriber started");
    }

    @Override
    public void onClose()
    {
        Agent.super.onClose();
        LOGGER.info("Shutting down backup subscriber");
        CloseHelper.quietClose(mdcSubscription);
//        CloseHelper.quietClose(archive);
        CloseHelper.quietClose(aeron);
        CloseHelper.quietClose(mediaDriver);
    }
}
