package com.streamcomputing.test.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.function.Supplier;

public class TestLogger {
    private static final DateTimeFormatter TIME_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");
    private final Logger logger;
    private final String testName;

    public TestLogger(Class<?> testClass) {
        this.logger = LoggerFactory.getLogger(testClass);
        this.testName = testClass.getSimpleName();
    }

    public void info(String message) {
        logger.info("[{}] {} - {}", testName, getCurrentTime(), message);
    }

    public void debug(String message) {
        logger.debug("[{}] {} - {}", testName, getCurrentTime(), message);
    }

    public void error(String message) {
        logger.error("[{}] {} - {}", testName, getCurrentTime(), message);
    }

    public void error(String message, Throwable throwable) {
        logger.error("[{}] {} - {}: {}", testName, getCurrentTime(), message, throwable.getMessage());
    }

    public void warn(String message) {
        logger.warn("[{}] {} - {}", testName, getCurrentTime(), message);
    }

    public void testStarted(String testMethodName) {
        logger.info("[{}] {} - Starting test: {}", testName, getCurrentTime(), testMethodName);
    }

    public void testFinished(String testMethodName) {
        logger.info("[{}] {} - Finished test: {}", testName, getCurrentTime(), testMethodName);
    }

    public void operatorCreated(String operatorType, int parallelism) {
        logger.info("[{}] {} - Created {} operator with parallelism {}", 
            testName, getCurrentTime(), operatorType, parallelism);
    }

    public void graphNodeAdded(String nodeId, String operatorType) {
        logger.info("[{}] {} - Added {} node '{}' to graph", 
            testName, getCurrentTime(), operatorType, nodeId);
    }

    public void graphEdgeAdded(String fromId, String toId) {
        logger.info("[{}] {} - Added edge from '{}' to '{}'", 
            testName, getCurrentTime(), fromId, toId);
    }

    public void jobStarted(int numTaskManagers) {
        logger.info("[{}] {} - Started job with {} task managers", 
            testName, getCurrentTime(), numTaskManagers);
    }

    public void jobFinished() {
        logger.info("[{}] {} - Job finished", testName, getCurrentTime());
    }

    public void messageProcessed(String operatorType, String message) {
        logger.debug("[{}] {} - {} processed message: {}", 
            testName, getCurrentTime(), operatorType, message);
    }

    public void windowTriggered(String key, int count, long windowStart, long windowEnd) {
        logger.info("[{}] {} - Window triggered for key '{}' with {} messages [{} - {}]", 
            testName, getCurrentTime(), key, count, windowStart, windowEnd);
    }

    public void assertionPassed(String assertion) {
        logger.info("[{}] {} - Assertion passed: {}", testName, getCurrentTime(), assertion);
    }

    public void assertionFailed(String assertion, String expected, String actual) {
        logger.error("[{}] {} - Assertion failed: {} (expected: {}, actual: {})", 
            testName, getCurrentTime(), assertion, expected, actual);
    }

    private String getCurrentTime() {
        return LocalDateTime.now().format(TIME_FORMATTER);
    }
} 