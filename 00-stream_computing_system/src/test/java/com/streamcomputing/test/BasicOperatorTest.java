package com.streamcomputing.test;

import com.streamcomputing.api.operators.*;
import com.streamcomputing.runtime.*;
import com.streamcomputing.test.util.*;
import org.junit.jupiter.api.*;
import java.io.Serializable;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

public class BasicOperatorTest {
    private static final String TOPIC = "operator-test-topic";
    private StreamGraph graph;
    private JobManager jobManager;
    private TestLogger logger;

    @BeforeEach
    void setUp() {
        logger = new TestLogger(BasicOperatorTest.class);
        logger.info("Setting up test environment");
        graph = new StreamGraph();
    }

    @AfterEach
    void tearDown() {
        logger.info("Tearing down test environment");
        if (jobManager != null) {
            jobManager.shutdown();
            logger.info("Job manager shutdown completed");
        }
    }

    @Test
    void testMapOperator() throws InterruptedException {
        logger.testStarted("testMapOperator");

        // Set up Kafka source
        logger.info("Configuring Kafka source");
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "localhost:9092");
        props.setProperty("group.id", "test-map-group");
        KafkaSource<String> source = new KafkaSource<>(TOPIC, props);
        logger.operatorCreated("KafkaSource", 1);
        
        // Create map operator
        logger.info("Creating map operator");
        MapOperator<String, String> mapOp = new MapOperator<>(
            line -> line.split(",")[1]
        );
        mapOp.setParallelism(2);
        logger.operatorCreated("MapOperator", 2);

        // Set up test sink
        TestSink<String> sink = new TestSink<>("map-test-sink", logger);
        
        // Build graph
        logger.info("Building operator graph");
        graph.addNode("source", source);
        graph.addNode("map", mapOp);
        graph.addNode("sink", sink);
        graph.addEdge("source", "map");
        graph.addEdge("map", "sink");

        logger.graphNodeAdded("source", "KafkaSource");
        logger.graphNodeAdded("map", "MapOperator");
        logger.graphNodeAdded("sink", "TestSink");
        logger.graphEdgeAdded("source", "map");
        logger.graphEdgeAdded("map", "sink");

        // Start processing
        logger.info("Starting job manager");
        jobManager = new JobManager(graph, 2);
        jobManager.start();
        logger.jobStarted(2);

        // Wait for results
        logger.info("Waiting for test results");
        boolean received = sink.waitForMessages(50, 30);
        assertTrue(received, "Should receive mapped messages");
        logger.assertionPassed("Received expected number of messages");

        boolean allValuesValid = sink.getMessages().stream()
            .allMatch(msg -> msg.startsWith("value"));
        assertTrue(allValuesValid, "All messages should be values");
        logger.assertionPassed("All messages have correct format");

        logger.testFinished("testMapOperator");
    }

    @Test
    void testKeyByAndReduceOperators() throws InterruptedException {
        logger.testStarted("testKeyByAndReduceOperators");

        // Set up Kafka source
        logger.info("Configuring Kafka source");
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "localhost:9092");
        props.setProperty("group.id", "test-keyby-reduce-group");
        KafkaSource<String> source = new KafkaSource<>(TOPIC, props);
        logger.operatorCreated("KafkaSource", 1);

        // Create operators
        logger.info("Creating keyBy operator");
        KeyByOperator<String, String> keyByOp = new KeyByOperator<>(
            line -> line.split(",")[0]
        );
        keyByOp.setParallelism(2);
        logger.operatorCreated("KeyByOperator", 2);

        logger.info("Creating reduce operator");
        ReduceOperator<String> reduceOp = new ReduceOperator<>((a, b) -> {
            String[] aParts = a.split(",");
            String[] bParts = b.split(",");
            return aParts[0] + "," + (Integer.parseInt(aParts[1]) + Integer.parseInt(bParts[1]));
        });
        reduceOp.setParallelism(2);
        logger.operatorCreated("ReduceOperator", 2);

        // Set up test sink
        TestSink<String> sink = new TestSink<>("keyby-reduce-test-sink", logger);

        // Build graph
        logger.info("Building operator graph");
        graph.addNode("source", source);
        graph.addNode("keyBy", keyByOp);
        graph.addNode("reduce", reduceOp);
        graph.addNode("sink", sink);
        graph.addEdge("source", "keyBy");
        graph.addEdge("keyBy", "reduce");
        graph.addEdge("reduce", "sink");

        logger.graphNodeAdded("source", "KafkaSource");
        logger.graphNodeAdded("keyBy", "KeyByOperator");
        logger.graphNodeAdded("reduce", "ReduceOperator");
        logger.graphNodeAdded("sink", "TestSink");
        logger.graphEdgeAdded("source", "keyBy");
        logger.graphEdgeAdded("keyBy", "reduce");
        logger.graphEdgeAdded("reduce", "sink");

        // Start processing
        logger.info("Starting job manager");
        jobManager = new JobManager(graph, 2);
        jobManager.start();
        logger.jobStarted(2);

        // Wait for results
        logger.info("Waiting for test results");
        boolean received = sink.waitForMessages(2, 30);
        assertTrue(received, "Should receive reduced messages");
        logger.assertionPassed("Received expected number of messages");

        assertEquals(2, sink.getMessages().size(), "Should have one result per key");
        logger.assertionPassed("Correct number of results per key");

        logger.testFinished("testKeyByAndReduceOperators");
    }

    private static class TestSink<T> implements Sink<T> {
        private final AtomicInteger messageCount = new AtomicInteger(0);
        private final CountDownLatch latch;
        private final List<T> messages = Collections.synchronizedList(new ArrayList<>());
        private int expectedCount;
        private final TestLogger logger;

        public TestSink(String name, TestLogger logger) {
            this.latch = new CountDownLatch(1);
            this.expectedCount = 0;
            this.logger = logger;
        }

        public void write(T message) {
            messages.add(message);
            int count = messageCount.incrementAndGet();
            logger.info("TestSink received message " + count + ": " + message);
            if (count >= expectedCount) {
                latch.countDown();
            }
        }

        public boolean waitForMessages(int expectedCount, int timeoutSeconds) throws InterruptedException {
            this.expectedCount = expectedCount;
            logger.info("TestSink waiting for " + expectedCount + " messages with " + timeoutSeconds + "s timeout");
            boolean result = latch.await(timeoutSeconds, TimeUnit.SECONDS);
            if (!result) {
                logger.error("TestSink timeout waiting for messages. Received " + messageCount.get() + "/" + expectedCount);
            } else {
                logger.info("TestSink received all expected messages: " + messageCount.get());
            }
            return result;
        }

        public List<T> getMessages() {
            return new ArrayList<>(messages);
        }
    }
} 