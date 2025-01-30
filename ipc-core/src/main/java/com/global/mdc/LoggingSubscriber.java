package com.global.mdc;

import org.agrona.CloseHelper;
import org.agrona.concurrent.AgentRunner;
import org.agrona.concurrent.ShutdownSignalBarrier;
import org.agrona.concurrent.SleepingMillisIdleStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LoggingSubscriber
{
    private static final Logger LOGGER = LoggerFactory.getLogger(LoggingSubscriber.class);

    public static void main(final String[] args)
    {
        if (args == null || args.length == 0 || args[0] == null)
        {
            throw new IllegalArgumentException("Subscriber port argument is required and cannot be null.");
        }

        final int subscriberPort;
        try
        {
            subscriberPort = Integer.parseInt(args[0]);
        }
        catch (final NumberFormatException e)
        {
            throw new IllegalArgumentException("Invalid subscriber port: must be a valid integer.", e);
        }
        // Optional second argument: Delay in milliseconds
        int delay = 0; // Default delay is 0
        if (args.length > 1)
        {
            try
            {
                delay = Integer.parseInt(args[1]);
                if (delay < 0)
                {
                    throw new IllegalArgumentException("Delay must be a positive integer.");
                }
            }
            catch (final NumberFormatException e)
            {
                throw new IllegalArgumentException("Invalid delay value. Please provide a positive integer.");
            }
        }
        final String publisherControlHost = "localhost";
        final int publisherControlPort = 13000;
        final String subscriberHost = "localhost";
        final ShutdownSignalBarrier barrier = new ShutdownSignalBarrier();
        final SubscriberAgent hostAgent =
            new SubscriberAgent(publisherControlHost, publisherControlPort,
                delay, 100);
        final AgentRunner runner =
            new AgentRunner(new SleepingMillisIdleStrategy(), LoggingSubscriber::errorHandler,
            null, hostAgent);
        AgentRunner.startOnThread(runner);

        barrier.await();

        CloseHelper.quietClose(runner);
    }

    private static void errorHandler(final Throwable throwable)
    {
        LOGGER.error("agent error {}", throwable.getMessage(), throwable);
    }
}
