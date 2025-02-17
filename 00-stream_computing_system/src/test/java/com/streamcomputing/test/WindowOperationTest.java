package com.streamcomputing.test;

import com.streamcomputing.api.operators.*;
import com.streamcomputing.runtime.*;
import org.junit.jupiter.api.*;
import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.List;
import java.util.ArrayList;
import java.util.Collections;
import java.io.Serializable;

import static org.junit.jupiter.api.Assertions.*;

public class WindowOperationTest {
    private static final String TOPIC = "window-test-topic";
    private StreamGraph graph;
    private JobManager jobManager;

    @BeforeEach
    void setUp() {
        graph = new StreamGraph();
    }

    @AfterEach
    void tearDown() {
        if (jobManager != null) {
            jobManager.shutdown();
        }
    }

    @Test
    void testProcessingTimeWindow() throws InterruptedException {
        // Set up Kafka source
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "localhost:9092");
        props.setProperty("group.id", "test-window-group");
        KafkaSource<String> source = new KafkaSource<>(TOPIC, props);

        // Create key extractor
        KeyByOperator<String, String> keyByOp = new KeyByOperator<>(
            line -> line.split(",")[1]  // key is the second element
        );
        keyByOp.setParallelism(2);

        // Create window operator (5-second window for testing)
        WindowOperator<String, String> windowOp = new WindowOperator<>(
            Duration.ofSeconds(5)
        );
        windowOp.setParallelism(2);

        // Create reduce operator to sum counts within window
        ReduceOperator<String> reduceOp = new ReduceOperator<>((a, b) -> {
            String[] aParts = a.split(",");
            String[] bParts = b.split(",");
            int sum = Integer.parseInt(aParts[2]) + Integer.parseInt(bParts[2]);
            return aParts[0] + "," + aParts[1] + "," + sum;
        });
        reduceOp.setParallelism(2);

        // Set up test sink
        TestSink<String> sink = new TestSink<>();

        // Build graph
        graph.addNode("source", source);
        graph.addNode("keyBy", keyByOp);
        graph.addNode("window", windowOp);
        graph.addNode("reduce", reduceOp);
        graph.addNode("sink", sink);
        
        graph.addEdge("source", "keyBy");
        graph.addEdge("keyBy", "window");
        graph.addEdge("window", "reduce");
        graph.addEdge("reduce", "sink");

        // Start processing
        jobManager = new JobManager(graph, 2);
        jobManager.start();

        // Wait for results (expect at least one window to trigger)
        assertTrue(sink.waitForMessages(1, 10), "Should receive windowed results");
        
        // Verify window results
        List<String> results = sink.getMessages();
        assertFalse(results.isEmpty(), "Should have window results");
        
        // Parse and verify window results
        for (String result : results) {
            String[] parts = result.split(",");
            assertEquals(3, parts.length, "Result should have timestamp, key, and count");
            assertTrue(Integer.parseInt(parts[2]) > 0, "Count should be positive");
        }
    }

    @Test
    void testWindowWithLateData() throws InterruptedException {
        // Set up Kafka source
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "localhost:9092");
        props.setProperty("group.id", "test-window-late-group");
        KafkaSource<String> source = new KafkaSource<>(TOPIC, props);

        // Create window operator with 3-second window
        WindowOperator<String, String> windowOp = new WindowOperator<>(
            Duration.ofSeconds(3)
        );
        windowOp.setParallelism(2);

        // Set up test sink
        TestSink<String> sink = new TestSink<>();

        // Build graph
        graph.addNode("source", source);
        graph.addNode("window", windowOp);
        graph.addNode("sink", sink);
        
        graph.addEdge("source", "window");
        graph.addEdge("window", "sink");

        // Start processing
        jobManager = new JobManager(graph, 2);
        jobManager.start();

        // Wait for results
        assertTrue(sink.waitForMessages(2, 10), "Should receive multiple window results");
        
        // Verify that windows are properly separated
        List<String> results = sink.getMessages();
        assertTrue(results.size() >= 2, "Should have at least two windows");
        
        // Get timestamps of consecutive windows
        long firstWindowTime = Long.parseLong(results.get(0).split(",")[0]);
        long secondWindowTime = Long.parseLong(results.get(1).split(",")[0]);
        
        // Verify window separation
        assertTrue(secondWindowTime - firstWindowTime >= 3000,
                "Windows should be at least 3 seconds apart");
    }

    private static class TestSink<T> implements Serializable {
        private final AtomicInteger messageCount = new AtomicInteger(0);
        private final CountDownLatch latch;
        private final List<T> messages = Collections.synchronizedList(new ArrayList<>());
        private int expectedCount;

        public TestSink() {
            this.latch = new CountDownLatch(1);
        }

        public void write(T message) {
            messages.add(message);
            if (messageCount.incrementAndGet() >= expectedCount) {
                latch.countDown();
            }
        }

        public boolean waitForMessages(int expectedCount, int timeoutSeconds) throws InterruptedException {
            this.expectedCount = expectedCount;
            return latch.await(timeoutSeconds, TimeUnit.SECONDS);
        }

        public List<T> getMessages() {
            return messages;
        }
    }
} 