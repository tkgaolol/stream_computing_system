package com.streamcomputing.examples;

import com.streamcomputing.api.StreamExecutionEnvironment;
import org.apache.kafka.common.serialization.StringDeserializer;
import java.util.Properties;

public class WordCount {
    public static void main(String[] args) {
        // Configure Kafka properties
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("group.id", "word-count-group");
        props.put("key.deserializer", StringDeserializer.class.getName());
        props.put("value.deserializer", StringDeserializer.class.getName());
        props.put("auto.offset.reset", "earliest");
        
        // Create execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.create(props);
        
        // Create word count pipeline
        env.createKafkaSource("word-count-input", String.class)
            .map(String::toLowerCase)
            .map(word -> new WordWithCount(word, 1))
            .keyBy(wc -> wc.word)
            .reduce((wc1, wc2) -> new WordWithCount(wc1.word, wc1.count + wc2.count))
            .sink("word-count-output.txt");
    }

    public static class WordWithCount {
        public final String word;
        public final int count;

        public WordWithCount(String word, int count) {
            this.word = word;
            this.count = count;
        }

        @Override
        public String toString() {
            return word + ": " + count;
        }
    }
} 