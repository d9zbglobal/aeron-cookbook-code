package com.global.mdc;

import io.aeron.logbuffer.FragmentHandler;
import io.aeron.logbuffer.Header;
import org.agrona.DirectBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

public class MultiDestinationSubscriberFragmentHandler implements FragmentHandler
{
    private static final Logger LOGGER = LoggerFactory.getLogger(MultiDestinationSubscriberFragmentHandler.class);

    @Override
    public void onFragment(final DirectBuffer buffer, final int offset, final int length, final Header header)
    {
        final long read = buffer.getLong(offset);
        long channelRcvTimestamp;
        String readableTimestamp = null;
        if (0 != header.reservedValue())
        {
            channelRcvTimestamp = header.reservedValue();
            Instant instant = Instant.ofEpochMilli(channelRcvTimestamp/1000000);
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS")
                .withZone(ZoneId.systemDefault());
            readableTimestamp = formatter.format(instant);
        }

        LOGGER.info("received {}, offset {}, timestamp {}", read, offset, readableTimestamp);
    }
}
