package com.streamcomputing.runtime;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.function.Function;

/**
 * Implementation of DataStream that reads from a Kafka topic.
 * @param <T> The type of elements in the stream
 */
public class KafkaSourceStream<T> extends BaseStream<T> {
    private static final Logger logger = LoggerFactory.getLogger(KafkaSourceStream.class);
    private final Properties kafkaProperties;
    private final String topic;
    private final Class<T> valueClass;
    private volatile boolean running = true;

    public KafkaSourceStream(Properties kafkaProperties, String topic, Class<T> valueClass) {
        this.kafkaProperties = kafkaProperties;
        this.topic = topic;
        this.valueClass = valueClass;
    }

    @Override
    public <R> void process(Function<T, R> processor) {
        try (KafkaConsumer<String, T> consumer = new KafkaConsumer<>(kafkaProperties)) {
            consumer.subscribe(Collections.singletonList(topic));
            
            while (running) {
                consumer.poll(Duration.ofMillis(100))
                    .forEach(record -> processRecord(record, processor));
            }
        } catch (Exception e) {
            logger.error("Error processing Kafka stream", e);
        }
    }

    private <R> void processRecord(ConsumerRecord<String, T> record, Function<T, R> processor) {
        try {
            processor.apply(record.value());
        } catch (Exception e) {
            logger.error("Error processing record: {}", record, e);
        }
    }

    public void stop() {
        running = false;
    }
} 