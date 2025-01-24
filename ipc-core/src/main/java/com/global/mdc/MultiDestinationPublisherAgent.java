package com.global.mdc;

import io.aeron.Aeron;
import io.aeron.ChannelUriStringBuilder;
import io.aeron.Publication;
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
    private static final EpochClock CLOCK = SystemEpochClock.INSTANCE;
    private static final int STREAM_ID = 100;
    private static final Logger LOGGER = LoggerFactory.getLogger(MultiDestinationPublisherAgent.class);
    private final Aeron aeron;
    private final MutableDirectBuffer mutableDirectBuffer;
    private final Publication publication;
    private final ShutdownSignalBarrier barrier;
    private long nextAppend = Long.MIN_VALUE;
    private long lastSeq = 0;

    public MultiDestinationPublisherAgent(final String publisherControlHost, final int publisherControlPort,
        final ShutdownSignalBarrier barrier)
    {
        this.mutableDirectBuffer = new UnsafeBuffer(ByteBuffer.allocateDirect(Long.BYTES));

        LOGGER.info("launching aeron");
        final MediaDriver.Context ctx = new MediaDriver.Context()
            .aeronDirectoryName("/Users/dan.scarborough/dev/tmp")
            .dirDeleteOnStart(true)
            .threadingMode(ThreadingMode.SHARED)
            .sharedIdleStrategy(new SleepingMillisIdleStrategy());
        MediaDriver mediaDriver = MediaDriver.launch(ctx);

        // Connect an Aeron client using the internal MediaDriver
        this.aeron = Aeron.connect(new Aeron.Context()
            .aeronDirectoryName(mediaDriver.aeronDirectoryName())
            .idleStrategy(new SleepingMillisIdleStrategy()));
        String publicationChannel = new ChannelUriStringBuilder().media(UDP_MEDIA)
            .controlEndpoint(publisherControlHost + ":" + publisherControlPort)
            .controlMode(MDC_CONTROL_MODE_DYNAMIC)
            .minFlowControl(2, null)
            .channelSendTimestampOffset("reserved")
            .receiverWindowLength(1024 * 64)
            .build();

        LOGGER.info("creating publication {}", publicationChannel);
        publication = aeron.addPublication(publicationChannel, STREAM_ID);
        LOGGER.info("term length {}, available window {}", publication.termBufferLength(),
            publication.availableWindow());
        this.barrier = barrier;
    }

    private void errorHandler(final Throwable throwable)
    {
        LOGGER.error("unexpected failure {}", throwable.getMessage(), throwable);
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
            if (publication.isConnected())
            {
                lastSeq += 1;
                mutableDirectBuffer.putLong(0, lastSeq);
                final long offer = publication.offer(mutableDirectBuffer, 0, Long.BYTES);
                if (offer < 0)
                {
                    LOGGER.error("unexpected failure {}", Publication.errorString(offer));
                    barrier.signal();
                }

                LOGGER.info("appended {}", lastSeq);
            }
            else
            {
                if (nextAppend % 1000 == 0)
                {
                    LOGGER.info("awaiting subscribers");
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

    @Override
    public void onClose()
    {
        Agent.super.onClose();
        LOGGER.info("Shutting down");
        CloseHelper.quietClose(publication);
        CloseHelper.quietClose(aeron);
    }

    @Override
    public String roleName()
    {
        return "mdc-publisher";
    }

}
