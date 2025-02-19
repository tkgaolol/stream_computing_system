package com.streamcomputing.api.operators;

import com.streamcomputing.api.DataStream;
import com.streamcomputing.runtime.KafkaDataStream;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Collections;
import java.util.Properties;

/**
 * Source operator that reads data from Kafka.
 * @param <T> The type of elements to be read from Kafka
 */
public class KafkaSource<T> {
    private org.apache.kafka.clients.consumer.KafkaConsumer<String, T> consumer;
    private final Properties properties;
    private final String topic;
    private int parallelism = 1;

    public KafkaSource(String topic, Properties properties) {
        this.topic = topic;
        this.properties = properties;
        
        // Set default Kafka consumer properties if not provided
        if (!properties.containsKey(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG)) {
            properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        }
        if (!properties.containsKey(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG)) {
            properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        }
        if (!properties.containsKey(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG)) {
            properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        }
        if (!properties.containsKey(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG)) {
            properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        }
        if (!properties.containsKey(ConsumerConfig.GROUP_ID_CONFIG)) {
            properties.put(ConsumerConfig.GROUP_ID_CONFIG, "stream-computing-group");
        }

        // Initialize the Kafka consumer
        this.consumer = new org.apache.kafka.clients.consumer.KafkaConsumer<>(properties);
        this.consumer.subscribe(Collections.singletonList(topic));
    }

    /**
     * Sets the parallelism level for this source operator.
     * @param parallelism The number of parallel instances to create
     * @return This KafkaSource instance for method chaining
     * @throws IllegalArgumentException if parallelism is less than or equal to 0
     */
    public KafkaSource<T> setParallelism(int parallelism) {
        if (parallelism <= 0) {
            throw new IllegalArgumentException("Parallelism must be greater than 0");
        }
        this.parallelism = parallelism;
        return this;
    }

    /**
     * Gets the current parallelism level of this source operator.
     * @return The number of parallel instances
     */
    public int getParallelism() {
        return parallelism;
    }

    public DataStream<T> createDataStream() {
        return new KafkaDataStream<>(properties, topic, parallelism);
    }

    /**
     * Gets the next element from the Kafka topic.
     * @return The next element from the topic, or null if no element is available
     */
    @SuppressWarnings("unchecked")
    public T getElement() {
        try {
            org.apache.kafka.clients.consumer.ConsumerRecords<String, T> records = consumer.poll(java.time.Duration.ofMillis(1000));
            if (!records.isEmpty()) {
                // Process all records and return the first one
                for (org.apache.kafka.clients.consumer.ConsumerRecord<String, T> record : records) {
                    if (record.value() != null) {
                        // Commit the offset to ensure we don't reprocess messages
                        consumer.commitSync();
                        return record.value();
                    }
                }
            }
            return null;
        } catch (Exception e) {
            // Log the error and return null
            System.err.println("Error polling Kafka: " + e.getMessage());
            e.printStackTrace();
            return null;
        }
    }

    /**
     * Closes the Kafka consumer when it's no longer needed.
     */
    public void close() {
        if (consumer != null) {
            consumer.close();
        }
    }
} 