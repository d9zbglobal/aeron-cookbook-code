package com.global.mdc;

import io.aeron.Aeron;
import io.aeron.Subscription;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import org.agrona.CloseHelper;
import org.agrona.concurrent.Agent;
import org.agrona.concurrent.SleepingMillisIdleStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MultiDestinationSubscriberAgent implements Agent
{
    private static final int STREAM_ID = 100;
    private static final Logger LOGGER = LoggerFactory.getLogger(MultiDestinationSubscriberAgent.class);
    private final MultiDestinationSubscriberFragmentHandler fragmentHandler;
    private final MediaDriver mediaDriver;
    private final Aeron aeron;
    private final Subscription mdcSubscription;
    private final int delay;

    public MultiDestinationSubscriberAgent(final String publisherControlHost, final int publisherControlPort,
        final String subscriberHost, final int subscriberPort, final int delay)
    {
        this.fragmentHandler = new MultiDestinationSubscriberFragmentHandler();

        // Start an internal MediaDriver
        LOGGER.info("launching media driver");
        //launch a media driver
        this.mediaDriver = MediaDriver.launch(new MediaDriver.Context()
            .dirDeleteOnStart(true)
            .threadingMode(ThreadingMode.SHARED)
            .sharedIdleStrategy(new SleepingMillisIdleStrategy()));

        // Connect an Aeron client using the internal MediaDriver
        this.aeron = Aeron.connect(new Aeron.Context()
            .aeronDirectoryName(mediaDriver.aeronDirectoryName())
            .idleStrategy(new SleepingMillisIdleStrategy()));

        // Add the MDC subscription
        final String channel = "aeron:udp?endpoint=" + subscriberHost +
            ":" + subscriberPort + "|control=" + publisherControlHost + ":" +
            publisherControlPort + "|control-mode=dynamic";
        LOGGER.info("Adding the subscription to channel: {}", channel);
        this.mdcSubscription = aeron.addSubscription(channel, STREAM_ID);

        this.delay = delay * 1000; // Convert delay to milliseconds
    }

    @Override
    public int doWork()
    {
        mdcSubscription.poll(fragmentHandler, 100);
        try
        {
            Thread.sleep(delay);
        }
        catch (final InterruptedException e)
        {
            throw new RuntimeException(e);
        }
        return 0;
    }

    @Override
    public String roleName()
    {
        return "mdc-subscriber";
    }

    @Override
    public void onStart()
    {
        Agent.super.onStart();
        LOGGER.info("Starting");
    }

    @Override
    public void onClose()
    {
        Agent.super.onClose();
        LOGGER.info("Shutting down");
        CloseHelper.quietClose(mdcSubscription);
        CloseHelper.quietClose(aeron);
        CloseHelper.quietClose(mediaDriver);
    }
}
