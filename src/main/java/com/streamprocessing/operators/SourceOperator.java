package com.streamprocessing.operators;

import com.streamprocessing.api.Operator;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Properties;

public class SourceOperator implements Operator {
    private final String name;
    private int parallelism;
    private final String topic;
    private final Properties kafkaProps;

    public SourceOperator(String name, String bootstrapServers, String topic, String groupId) {
        this.name = name;
        this.parallelism = 1;
        this.topic = topic;
        
        this.kafkaProps = new Properties();
        kafkaProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        kafkaProps.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        kafkaProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, 
                      "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, 
                      "org.apache.kafka.common.serialization.StringDeserializer");
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public int getParallelism() {
        return parallelism;
    }

    @Override
    public void setParallelism(int parallelism) {
        this.parallelism = parallelism;
    }

    public String getTopic() {
        return topic;
    }

    public Properties getKafkaProps() {
        return kafkaProps;
    }

    public KafkaConsumer<String, String> createConsumer() {
        return new KafkaConsumer<>(kafkaProps);
    }
} 