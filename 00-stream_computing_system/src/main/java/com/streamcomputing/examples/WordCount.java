package com.streamcomputing.examples;

import com.streamcomputing.api.operators.*;
import com.streamcomputing.runtime.*;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * Example WordCount application using the stream computing system.
 */
public class WordCount {
    public static void main(String[] args) throws Exception {
        // Create stream graph
        StreamGraph graph = new StreamGraph();

        // 1. Add Kafka source operator
        Properties kafkaProps = new Properties();
        kafkaProps.setProperty("bootstrap.servers", "localhost:9092");
        kafkaProps.setProperty("group.id", "word-count-group");
        kafkaProps.setProperty("auto.offset.reset", "latest");
        kafkaProps.setProperty("enable.auto.commit", "false"); // Disable auto commit for better control
        
        KafkaSource<String> source = new KafkaSource<>("wordcount-test-topic", kafkaProps);
        source.setParallelism(1);
        graph.addNode("source", source);
        
        // 2. Add map operator to split lines into words and normalize
        MapOperator<String, String[]> splitOp = new MapOperator<>(
            line -> line.toLowerCase().trim().split("\\W+")
        );
        splitOp.setParallelism(2);
        graph.addNode("split", splitOp);
        
        // 3. Add flatMap operator to convert arrays to individual word counts
        MapOperator<String[], String> flatMapOp = new MapOperator<>(
            words -> Arrays.stream(words)
                          .filter(word -> !word.isEmpty()) // Filter out empty words
                          .map(word -> word + ",1")
                          .findFirst()
                          .orElse(null)
        );
        flatMapOp.setParallelism(3);
        graph.addNode("flatMap", flatMapOp);
        
        // 4. Add keyBy operator to group by word
        KeyByOperator<String, String> keyByOp = new KeyByOperator<>(
            word -> word.split(",")[0]
        );
        keyByOp.setParallelism(4);
        graph.addNode("keyBy", keyByOp);
        
        // 5. Add window operator with error handling
        WindowOperator<String, String> windowOp = new WindowOperator<>(
            Duration.ofMinutes(5)
        );
        windowOp.setParallelism(2);
        graph.addNode("window", windowOp);
        
        // 6. Add reduce operator to sum counts with error handling
        ReduceOperator<String> reduceOp = new ReduceOperator<>((a, b) -> {
            try {
                if (a == null || b == null) {
                    return a == null ? b : a;
                }
                String[] aParts = a.split(",");
                String[] bParts = b.split(",");
                if (aParts.length != 2 || bParts.length != 2) {
                    throw new IllegalArgumentException("Invalid format: " + a + " or " + b);
                }
                int sum = Integer.parseInt(aParts[1]) + Integer.parseInt(bParts[1]);
                return aParts[0] + "," + sum;
            } catch (Exception e) {
                throw new RuntimeException("Error reducing word counts: " + e.getMessage(), e);
            }
        });
        reduceOp.setParallelism(2);
        graph.addNode("reduce", reduceOp);
        
        // 7. Add file sink with timestamp
        FileSink<String> sink = new FileSink<>("word-counts.txt");
        sink.setFormatter(record -> {
            if (record == null) return null;
            return System.currentTimeMillis() + "," + record;
        });
        sink.setParallelism(1);
        graph.addNode("sink", sink);
        
        // Connect operators
        graph.addEdge("source", "split");
        graph.addEdge("split", "flatMap");
        graph.addEdge("flatMap", "keyBy");
        graph.addEdge("keyBy", "window");
        graph.addEdge("window", "reduce");
        graph.addEdge("reduce", "sink");
        
        // Create and start job manager with error handling
        JobManager jobManager = new JobManager(graph, 3);
        try {
            jobManager.start();
            
            // Keep the application running
            while (!Thread.currentThread().isInterrupted()) {
                TimeUnit.SECONDS.sleep(1);
            }
        } catch (Exception e) {
            System.err.println("Error in word count job: " + e.getMessage());
            e.printStackTrace();
        } finally {
            jobManager.shutdown();
        }
    }
} 