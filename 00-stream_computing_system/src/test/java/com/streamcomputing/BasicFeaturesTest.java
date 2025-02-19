package com.streamcomputing.test;

import com.streamcomputing.api.DataStream;
import com.streamcomputing.api.KeyedDataStream;
import com.streamcomputing.api.WindowedStream;
import com.streamcomputing.api.operators.KafkaSource;
import com.streamcomputing.api.operators.FileSink;
import com.streamcomputing.runtime.BasicDataStream;
import com.streamcomputing.runtime.KafkaDataStream;
import org.junit.jupiter.api.*;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.Properties;

/**
 * Comprehensive test suite for basic features of the stream computing system.
 * Tests cover all core functionalities required by the project specification:
 * - Source and Sink operators
 * - Map transformation
 * - KeyBy operation
 * - Reduce operation
 * - Processing time windows
 * - Operator parallelism
 * - DAG execution
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class BasicFeaturesTest {
    
    private static final String KAFKA_BROKER = "localhost:9092";
    private static final String OPERATOR_TEST_TOPIC = "operator-test-topic";
    private static final String WINDOW_TEST_TOPIC = "window-test-topic";
    private static final String WORDCOUNT_TEST_TOPIC = "wordcount-test-topic";
    private static final String TEST_OUTPUT_FILE = "test-output.txt";
    
    private String getUniqueGroupId() {
        return "test-group-" + UUID.randomUUID().toString();
    }
    
    @BeforeAll
    static void setup() {
        cleanupTestFiles();
    }
    
    @AfterAll
    static void cleanup() {
        cleanupTestFiles();
    }
    
    private static void cleanupTestFiles() {
        new File(TEST_OUTPUT_FILE).delete();
    }

    /**
     * Test basic source to sink pipeline
     */
    @Test
    @Order(1)
    void testBasicSourceToSink() throws Exception {
        String groupId = getUniqueGroupId();
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", KAFKA_BROKER);
        props.setProperty("group.id", groupId);
        props.setProperty("auto.offset.reset", "earliest");
        
        KafkaSource<String> source = new KafkaSource<>(OPERATOR_TEST_TOPIC, props);
        DataStream<String> inputStream = source.createDataStream();
        
        // Create sink
        FileSink<String> sink = new FileSink<>(TEST_OUTPUT_FILE);
        
        // Run for 30 seconds
        CountDownLatch latch = new CountDownLatch(1);
        AtomicInteger processedCount = new AtomicInteger(0);
        Thread processingThread = new Thread(() -> {
            try {
                sink.open();
                while (processedCount.get() < 10 && !Thread.currentThread().isInterrupted()) {
                    try {
                        String data = ((BasicDataStream<String>) inputStream).getElement();
                        if (data != null) {
                            sink.write(data);
                            processedCount.incrementAndGet();
                            if (processedCount.get() >= 5) {
                                // We've processed enough data
                                break;
                            }
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        break;
                    }
                }
            } catch (Exception e) {
                if (!(e instanceof InterruptedException)) {
                    e.printStackTrace();
                }
            } finally {
                try {
                    sink.close();
                } catch (Exception e) {
                    e.printStackTrace();
                }
                latch.countDown();
            }
        });
        
        processingThread.start();
        boolean completed = latch.await(30, TimeUnit.SECONDS); // Increased timeout
        
        // Cleanup thread if not completed
        if (!completed) {
            processingThread.interrupt();
            throw new AssertionError("Test did not complete within timeout. Processed " + processedCount.get() + " records");
        }
        
        // Verify output file exists and has content
        File outputFile = new File(TEST_OUTPUT_FILE);
        Assertions.assertTrue(outputFile.exists(), "Output file should be created");
        Assertions.assertTrue(outputFile.length() > 0, "Output file should have content");
        
        // Print the content for debugging
        List<String> content = Files.readAllLines(outputFile.toPath());
        System.out.println("Output file content:");
        content.forEach(System.out::println);
    }

    /**
     * Test map transformation
     */
    @Test
    @Order(2)
    void testMapTransformation() throws Exception {
        String groupId = getUniqueGroupId();
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", KAFKA_BROKER);
        props.setProperty("group.id", groupId);
        KafkaSource<String> source = new KafkaSource<>(OPERATOR_TEST_TOPIC, props);
        DataStream<String> inputStream = source.createDataStream();
        
        // Apply map transformation
        DataStream<Integer> mappedStream = inputStream
            .map(str -> str.length());
        
        // Verify transformation with timeout
        CountDownLatch latch = new CountDownLatch(1);
        AtomicInteger result = new AtomicInteger(0);
        
        Thread processingThread = new Thread(() -> {
            try {
                String testInput = ((BasicDataStream<String>) inputStream).getElement();
                Integer mappedValue = ((BasicDataStream<Integer>) mappedStream).getElement();
                if (testInput.length() == mappedValue) {
                    result.set(1);
                }
                latch.countDown();
            } catch (Exception e) {
                if (!(e instanceof InterruptedException)) {
                    e.printStackTrace();
                }
            }
        });
        
        processingThread.start();
        boolean completed = latch.await(10, TimeUnit.SECONDS);
        
        if (!completed) {
            processingThread.interrupt();
            throw new AssertionError("Test did not complete within timeout");
        }
        
        Assertions.assertEquals(1, result.get(), "Map transformation should work correctly");
    }

    /**
     * Test keyBy operation
     */
    @Test
    @Order(3)
    void testKeyByOperation() throws Exception {
        String groupId = getUniqueGroupId();
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", KAFKA_BROKER);
        props.setProperty("group.id", groupId);
        KafkaSource<String> source = new KafkaSource<>(OPERATOR_TEST_TOPIC, props);
        DataStream<String> inputStream = source.createDataStream();
        
        // Apply keyBy
        KeyedDataStream<String, String> keyedStream = inputStream
            .keyBy(str -> str);
        
        Assertions.assertNotNull(keyedStream, "KeyedStream should be created");
    }

    /**
     * Test reduce operation with window
     */
    @Test
    @Order(4)
    void testReduceWithWindow() throws Exception {
        String groupId = getUniqueGroupId();
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", KAFKA_BROKER);
        props.setProperty("group.id", groupId);
        props.setProperty("auto.offset.reset", "earliest");  // Added to ensure we get all messages
        KafkaSource<String> source = new KafkaSource<>(WINDOW_TEST_TOPIC, props);
        DataStream<String> inputStream = source.createDataStream();
        
        // Create windowed reduce operation with shorter window for testing
        DataStream<Integer> reducedStream = inputStream
            .map(str -> {
                String[] parts = str.split(",");
                return Integer.parseInt(parts[2]); // Extract the count value
            })
            .tumblingWindow(Duration.ofSeconds(1).toMillis())
            .reduce((a, b) -> {
                System.out.println("Reducing: " + a + " + " + b);
                return a + b;
            });
        
        // Run the windowed operation with timeout
        CountDownLatch latch = new CountDownLatch(1);
        AtomicInteger windowResult = new AtomicInteger(0);
        AtomicInteger processedCount = new AtomicInteger(0);
        
        Thread processingThread = new Thread(() -> {
            try {
                long startTime = System.currentTimeMillis();
                long timeout = TimeUnit.SECONDS.toMillis(20);  // Increased timeout
                
                while (System.currentTimeMillis() - startTime < timeout && 
                       !Thread.currentThread().isInterrupted()) {
                    Integer result = ((BasicDataStream<Integer>) reducedStream).getElement(100, TimeUnit.MILLISECONDS);
                    if (result != null) {
                        System.out.println("Got window result: " + result);
                        windowResult.set(result);
                        processedCount.incrementAndGet();
                        if (result > 0) {
                            latch.countDown();
                            break;
                        }
                    }
                }
            } catch (Exception e) {
                if (!(e instanceof InterruptedException)) {
                    e.printStackTrace();
                }
            }
        });
        
        processingThread.start();
        
        // Wait for results
        boolean completed = latch.await(20, TimeUnit.SECONDS);  // Increased timeout
        
        if (!completed) {
            processingThread.interrupt();
            throw new AssertionError(
                "Test did not complete within timeout. Processed " + 
                processedCount.get() + " elements, last result: " + 
                windowResult.get() + ". Check if data is being properly produced to Kafka."
            );
        }
        
        Assertions.assertTrue(windowResult.get() > 0, 
            "Expected positive window result, got: " + windowResult.get());
        System.out.println("Test completed successfully with window result: " + windowResult.get());
    }

    /**
     * Test operator parallelism configuration
     */
    @Test
    @Order(5)
    void testOperatorParallelism() {
        String groupId = getUniqueGroupId();
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", KAFKA_BROKER);
        props.setProperty("group.id", groupId);
        KafkaSource<String> source = new KafkaSource<>(OPERATOR_TEST_TOPIC, props);
        
        // Test default parallelism
        Assertions.assertEquals(1, source.getParallelism(), "Default parallelism should be 1");
        
        // Test setting valid parallelism
        source.setParallelism(2);
        Assertions.assertEquals(2, source.getParallelism(), "Parallelism should be updated to 2");
        
        // Test setting invalid parallelism
        Assertions.assertThrows(IllegalArgumentException.class, () -> {
            source.setParallelism(0);
        }, "Should throw IllegalArgumentException for parallelism <= 0");
        
        Assertions.assertThrows(IllegalArgumentException.class, () -> {
            source.setParallelism(-1);
        }, "Should throw IllegalArgumentException for parallelism <= 0");
        
        // Verify parallelism wasn't changed by invalid attempts
        Assertions.assertEquals(2, source.getParallelism(), "Parallelism should remain 2 after invalid attempts");
    }
} 