package com.global.mdc;

import org.agrona.CloseHelper;
import org.agrona.concurrent.AgentRunner;
import org.agrona.concurrent.BackoffIdleStrategy;
import org.agrona.concurrent.ShutdownSignalBarrier;
import org.agrona.concurrent.SleepingMillisIdleStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MultiDestinationPublisher
{
    private static final Logger LOGGER = LoggerFactory.getLogger(MultiDestinationPublisher.class);

    public static void main(final String[] args)
    {
        final String publisherControlHost = "localhost";
        final int publisherControlPort;
        if (args == null || args.length == 0 || args[0] == null)
        {
            throw new IllegalArgumentException("Publisher control port argument is required and cannot be null.");
        }

        try
        {
            publisherControlPort = Integer.parseInt(args[0]);
        }
        catch (final NumberFormatException e)
        {
            throw new IllegalArgumentException("Invalid subscriber port: must be a valid integer.", e);
        }
        final ShutdownSignalBarrier barrier = new ShutdownSignalBarrier();
        final MultiDestinationPublisherAgent publisherAgent =
            new MultiDestinationPublisherAgent(publisherControlHost, publisherControlPort, barrier);
        final AgentRunner runner =
            new AgentRunner(new BackoffIdleStrategy(),
            MultiDestinationPublisher::errorHandler, null, publisherAgent);

        AgentRunner.startOnThread(runner);

        barrier.await();

        CloseHelper.quietClose(runner);
    }

    private static void errorHandler(final Throwable throwable)
    {
        LOGGER.error("agent failure {}", throwable.getMessage(), throwable);
    }
}
