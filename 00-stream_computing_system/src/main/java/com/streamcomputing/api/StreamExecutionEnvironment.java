package com.streamcomputing.api;

import com.streamcomputing.runtime.KafkaSourceStream;
import java.util.Properties;

/**
 * The main entry point for creating data stream jobs.
 */
public class StreamExecutionEnvironment {
    private final Properties kafkaProperties;
    
    private StreamExecutionEnvironment(Properties kafkaProperties) {
        this.kafkaProperties = kafkaProperties;
    }
    
    /**
     * Creates a new StreamExecutionEnvironment.
     * @param kafkaProperties Kafka connection properties
     * @return A new StreamExecutionEnvironment instance
     */
    public static StreamExecutionEnvironment create(Properties kafkaProperties) {
        return new StreamExecutionEnvironment(kafkaProperties);
    }
    
    /**
     * Creates a DataStream from a Kafka topic.
     * @param topic The Kafka topic to read from
     * @param valueClass The class of the values in the topic
     * @param <T> The type of the values
     * @return A DataStream of the topic's values
     */
    public <T> DataStream<T> createKafkaSource(String topic, Class<T> valueClass) {
        return new KafkaSourceStream<>(kafkaProperties, topic, valueClass);
    }
} 