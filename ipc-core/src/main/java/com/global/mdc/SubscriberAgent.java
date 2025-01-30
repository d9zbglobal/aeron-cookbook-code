package com.global.mdc;

import io.aeron.Aeron;
import io.aeron.ChannelUriStringBuilder;
import io.aeron.Subscription;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import org.agrona.CloseHelper;
import org.agrona.concurrent.Agent;
import org.agrona.concurrent.SleepingMillisIdleStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static io.aeron.CommonContext.MDC_CONTROL_MODE_DYNAMIC;
import static io.aeron.CommonContext.UDP_MEDIA;

public class SubscriberAgent implements Agent
{
    private static final Logger LOGGER = LoggerFactory.getLogger(SubscriberAgent.class);
    private final SubscriberFragmentHandler fragmentHandler;
    private final MediaDriver mediaDriver;
    private final Aeron aeron;
    private final Subscription mdcSubscription;
    private final int delay;

    public SubscriberAgent(final String publisherControlHost, final int publisherControlPort,
        final int delay, final int streamId)
    {
        this.fragmentHandler = new SubscriberFragmentHandler();

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
        final String channel = new ChannelUriStringBuilder().media(UDP_MEDIA)
            .endpoint("0.0.0.0:0") // an ephemeral post on the local machine
            .controlEndpoint(publisherControlHost + ":" + publisherControlPort)
            .channelReceiveTimestampOffset("reserved")
//            .receiverWindowLength(1024)
            .controlMode(MDC_CONTROL_MODE_DYNAMIC).build();


        LOGGER.info("Adding the subscription to channel: {}", channel);
        this.mdcSubscription = aeron.addSubscription(channel, streamId);
        LOGGER.info(mdcSubscription.resolvedEndpoint());

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
