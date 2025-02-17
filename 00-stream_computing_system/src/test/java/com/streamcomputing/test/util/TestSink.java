package com.streamcomputing.test.util;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class TestSink<T> implements Serializable {
    private final AtomicInteger messageCount = new AtomicInteger(0);
    private final CountDownLatch latch;
    private final List<T> messages = Collections.synchronizedList(new ArrayList<>());
    private int expectedCount;
    private final TestLogger logger;
    private final String sinkId;

    public TestSink(String sinkId, TestLogger logger) {
        this.latch = new CountDownLatch(1);
        this.logger = logger;
        this.sinkId = sinkId;
        logger.info("Created TestSink with ID: " + sinkId);
    }

    public void write(T message) {
        messages.add(message);
        int currentCount = messageCount.incrementAndGet();
        logger.debug(String.format("TestSink[%s] received message %d/%d: %s", 
            sinkId, currentCount, expectedCount, message));

        if (currentCount >= expectedCount) {
            logger.info(String.format("TestSink[%s] received all expected messages (%d)", 
                sinkId, expectedCount));
            latch.countDown();
        }
    }

    public boolean waitForMessages(int expectedCount, int timeoutSeconds) throws InterruptedException {
        this.expectedCount = expectedCount;
        logger.info(String.format("TestSink[%s] waiting for %d messages with %ds timeout", 
            sinkId, expectedCount, timeoutSeconds));

        boolean result = latch.await(timeoutSeconds, TimeUnit.SECONDS);
        
        if (result) {
            logger.info(String.format("TestSink[%s] successfully received %d messages", 
                sinkId, expectedCount));
        } else {
            logger.error(String.format("TestSink[%s] timeout waiting for messages. Received %d/%d", 
                sinkId, messageCount.get(), expectedCount));
        }
        
        return result;
    }

    public List<T> getMessages() {
        return messages;
    }

    public int getMessageCount() {
        return messageCount.get();
    }

    public void reset() {
        messages.clear();
        messageCount.set(0);
        logger.info(String.format("TestSink[%s] reset", sinkId));
    }
} 