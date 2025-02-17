package com.streamcomputing.test;

import com.streamcomputing.api.operators.*;
import com.streamcomputing.runtime.*;
import org.junit.jupiter.api.*;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.List;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Set;
import java.util.HashSet;
import java.io.Serializable;

import static org.junit.jupiter.api.Assertions.*;

public class ParallelismTest {
    private static final String TOPIC = "operator-test-topic";
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
    void testOperatorParallelism() throws InterruptedException {
        // Set up Kafka source with 2 partitions
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "localhost:9092");
        props.setProperty("group.id", "test-parallel-group");
        KafkaSource<String> source = new KafkaSource<>(TOPIC, props);
        source.setParallelism(2);

        // Create map operator with 3 parallel instances
        MapOperator<String, String> mapOp = new MapOperator<>(
            line -> {
                String threadName = Thread.currentThread().getName();
                return line + "," + threadName;
            }
        );
        mapOp.setParallelism(3);

        // Create keyBy operator with 4 parallel instances
        KeyByOperator<String, String> keyByOp = new KeyByOperator<>(
            line -> line.split(",")[0]
        );
        keyByOp.setParallelism(4);

        // Set up test sink to collect thread information
        TestSink<String> sink = new TestSink<>();

        // Build graph
        graph.addNode("source", source);
        graph.addNode("map", mapOp);
        graph.addNode("keyBy", keyByOp);
        graph.addNode("sink", sink);

        graph.addEdge("source", "map");
        graph.addEdge("map", "keyBy");
        graph.addEdge("keyBy", "sink");

        // Start processing with multiple task managers
        jobManager = new JobManager(graph, 4);
        jobManager.start();

        // Wait for results
        assertTrue(sink.waitForMessages(50, 30), "Should receive processed messages");

        // Verify parallelism by checking unique thread names
        List<String> results = sink.getMessages();
        Set<String> mapThreads = new HashSet<>();
        Set<String> keyByThreads = new HashSet<>();

        for (String result : results) {
            String[] parts = result.split(",");
            mapThreads.add(parts[2]);  // Thread name from map operator
            keyByThreads.add(parts[3]); // Thread name from keyBy operator
        }

        // Verify that we see the expected number of parallel threads
        assertTrue(mapThreads.size() >= 2, "Should see at least 2 map threads");
        assertTrue(keyByThreads.size() >= 3, "Should see at least 3 keyBy threads");
    }

    @Test
    void testLoadBalancing() throws InterruptedException {
        // Set up map operator with high parallelism
        MapOperator<String, String> mapOp = new MapOperator<>(
            value -> {
                // Simulate some work
                try {
                    Thread.sleep(10);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                return value + "," + Thread.currentThread().getName();
            }
        );
        mapOp.setParallelism(4);

        // Set up source
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "localhost:9092");
        props.setProperty("group.id", "test-loadbalance-group");
        KafkaSource<String> source = new KafkaSource<>(TOPIC, props);

        // Set up test sink
        TestSink<String> sink = new TestSink<>();

        // Build graph
        graph.addNode("source", source);
        graph.addNode("map", mapOp);
        graph.addNode("sink", sink);

        graph.addEdge("source", "map");
        graph.addEdge("map", "sink");

        // Start processing
        jobManager = new JobManager(graph, 4);
        jobManager.start();

        // Wait for results
        assertTrue(sink.waitForMessages(100, 30), "Should receive processed messages");

        // Analyze thread distribution
        List<String> results = sink.getMessages();
        Map<String, Integer> threadCounts = new HashMap<>();

        for (String result : results) {
            String threadName = result.split(",")[2];
            threadCounts.merge(threadName, 1, Integer::sum);
        }

        // Calculate load distribution metrics
        int maxCount = Collections.max(threadCounts.values());
        int minCount = Collections.min(threadCounts.values());
        double ratio = (double) minCount / maxCount;

        // Verify reasonable load distribution (at least 60% balanced)
        assertTrue(ratio > 0.6, "Load should be reasonably balanced across threads");
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