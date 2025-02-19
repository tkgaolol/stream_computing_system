package com.streamcomputing.test;

import com.streamcomputing.api.operators.*;
import com.streamcomputing.runtime.*;
import org.junit.jupiter.api.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.List;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Map;
import java.util.HashMap;
import java.io.Serializable;

import static org.junit.jupiter.api.Assertions.*;

public class WordCountTest {
    private static final String TOPIC = "wordcount-test-topic";
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
    void testWordCount() throws InterruptedException {
        // Set up Kafka source
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "localhost:9092");
        props.setProperty("group.id", "test-wordcount-group");
        KafkaSource<String> source = new KafkaSource<>(TOPIC, props);

        // Create map operator to split lines into words
        MapOperator<String, String[]> splitOp = new MapOperator<>(
            line -> line.toLowerCase().split("\\W+")
        );
        splitOp.setParallelism(2);

        // Create flatMap operator
        MapOperator<String[], String> flatMapOp = new MapOperator<>(
            words -> Arrays.stream(words)
                         .map(word -> word + ",1")
                         .findFirst()
                         .orElse("")
        );
        flatMapOp.setParallelism(2);

        // Create keyBy operator
        KeyByOperator<String, String> keyByOp = new KeyByOperator<>(
            word -> word.split(",")[0]
        );
        keyByOp.setParallelism(2);

        // Create window operator (5-second window for testing)
        WindowOperator<String, String> windowOp = new WindowOperator<>(
            Duration.ofSeconds(5)
        );
        windowOp.setParallelism(2);

        // Create reduce operator
        ReduceOperator<String> reduceOp = new ReduceOperator<>((a, b) -> {
            String[] aParts = a.split(",");
            String[] bParts = b.split(",");
            int sum = Integer.parseInt(aParts[1]) + Integer.parseInt(bParts[1]);
            return aParts[0] + "," + sum;
        });
        reduceOp.setParallelism(2);

        // Set up test sink
        TestSink<String> sink = new TestSink<>();

        // Build graph
        graph.addNode("source", source);
        graph.addNode("split", splitOp);
        graph.addNode("flatMap", flatMapOp);
        graph.addNode("keyBy", keyByOp);
        graph.addNode("window", windowOp);
        graph.addNode("reduce", reduceOp);
        graph.addNode("sink", sink);

        graph.addEdge("source", "split");
        graph.addEdge("split", "flatMap");
        graph.addEdge("flatMap", "keyBy");
        graph.addEdge("keyBy", "window");
        graph.addEdge("window", "reduce");
        graph.addEdge("reduce", "sink");

        // Start processing
        jobManager = new JobManager(graph, 3);
        jobManager.start();

        // Wait for results
        assertTrue(sink.waitForMessages(3, 30), "Should receive word counts");

        // Verify results
        List<String> results = sink.getMessages();
        Map<String, Integer> wordCounts = new HashMap<>();
        
        for (String result : results) {
            String[] parts = result.split(",");
            wordCounts.put(parts[0], Integer.parseInt(parts[1]));
        }

        // Verify expected word counts
        assertTrue(wordCounts.containsKey("hello"), "Should count 'hello'");
        assertTrue(wordCounts.containsKey("world"), "Should count 'world'");
        assertTrue(wordCounts.containsKey("computing"), "Should count 'computing'");
        
        assertEquals(3, wordCounts.get("hello"), "Should count 3 occurrences of 'hello'");
        assertEquals(2, wordCounts.get("world"), "Should count 2 occurrences of 'world'");
        assertEquals(3, wordCounts.get("computing"), "Should count 3 occurrences of 'computing'");
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