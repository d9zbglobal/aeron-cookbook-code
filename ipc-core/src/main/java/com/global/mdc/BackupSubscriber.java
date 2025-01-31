package com.global.mdc;

import org.agrona.CloseHelper;
import org.agrona.concurrent.AgentRunner;
import org.agrona.concurrent.ShutdownSignalBarrier;
import org.agrona.concurrent.SleepingMillisIdleStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BackupSubscriber
{
    private static final Logger LOGGER = LoggerFactory.getLogger(BackupSubscriber.class);

    public static void main(final String[] args)
    {
        final String publisherControlHost = "localhost";
        final int publisherControlPort = 13001;
        final ShutdownSignalBarrier barrier = new ShutdownSignalBarrier();
        final BackupSubscriberAgent hostAgent =
            new BackupSubscriberAgent(publisherControlHost, publisherControlPort,
                102);
        final AgentRunner runner =
            new AgentRunner(new SleepingMillisIdleStrategy(), BackupSubscriber::errorHandler,
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
