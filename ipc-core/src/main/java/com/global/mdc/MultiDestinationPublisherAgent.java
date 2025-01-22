package com.global.mdc;

import io.aeron.Aeron;
import io.aeron.Publication;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import org.agrona.CloseHelper;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;

public class MultiDestinationPublisherAgent implements Agent
{
    private static final EpochClock CLOCK = SystemEpochClock.INSTANCE;
    private static final int STREAM_ID = 100;
    private static final Logger LOGGER = LoggerFactory.getLogger(MultiDestinationPublisherAgent.class);
    private final Aeron aeron;
    private final MutableDirectBuffer mutableDirectBuffer;
    private final Publication publication;
    private long nextAppend = Long.MIN_VALUE;
    private long lastSeq = 0;
    private final ShutdownSignalBarrier barrier;

    public MultiDestinationPublisherAgent(final String publisherControlHost, final int publisherControlPort,
        final ShutdownSignalBarrier barrier)
    {
        this.mutableDirectBuffer = new UnsafeBuffer(ByteBuffer.allocateDirect(Long.BYTES));

        LOGGER.info("launching aeron");
         MediaDriver mediaDriver = MediaDriver.launch(new MediaDriver.Context()
            .dirDeleteOnStart(true)
            .threadingMode(ThreadingMode.SHARED)
            .sharedIdleStrategy(new SleepingMillisIdleStrategy()));

        // Connect an Aeron client using the internal MediaDriver
        this.aeron = Aeron.connect(new Aeron.Context()
            .aeronDirectoryName(mediaDriver.aeronDirectoryName())
            .idleStrategy(new SleepingMillisIdleStrategy()));
        final String publicationChannel = "aeron:udp?control-mode=dynamic|control=" + publisherControlHost +
            ":" + publisherControlPort + "|fc=min,g:/2";
//            ":" + publisherControlPort + "|fc=min";
        LOGGER.info("creating publication {}", publicationChannel);
        publication = aeron.addPublication(publicationChannel, STREAM_ID);
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
                if(offer < 0)
                {
                   LOGGER.error("unexpected failure {}", offer);
                   barrier.signal();
                }

                LOGGER.info("appended {}", lastSeq);
            }
            else
            {
                LOGGER.info("awaiting subscribers");
            }
            nextAppend = CLOCK.time();
            if(lastSeq == 10000)
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
