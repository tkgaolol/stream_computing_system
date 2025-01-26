package com.streamprocessing.examples;

import com.streamprocessing.api.DataStream;
import com.streamprocessing.runtime.StreamExecutionEnvironment;

public class WordCount {
    public static void main(String[] args) {
        if (args.length != 4) {
            System.err.println("Usage: WordCount <bootstrap-servers> <topic> <group-id> <output-file>");
            System.exit(1);
        }

        String bootstrapServers = args[0];
        String topic = args[1];
        String groupId = args[2];
        String outputFile = args[3];

        // Create execution environment
        StreamExecutionEnvironment env = new StreamExecutionEnvironment("word-count");

        // Create source stream from Kafka
        DataStream<String> source = env.addKafkaSource("kafka-source", bootstrapServers, topic, groupId);

        // Split input into words and convert to lowercase
        DataStream<String> words = env.map(source, "word-extractor", 
            line -> line.toLowerCase().split(",")[0]);

        // Key by word
        DataStream<String> keyedWords = env.keyBy(words, "word-keyer", word -> word);

        // Count words
        DataStream<WordCount.WordWithCount> counts = env.reduce(keyedWords, "word-counter",
            (word1, word2) -> word1);

        // Add sink to write results
        env.addFileSink(counts, "file-sink", outputFile, 
            count -> String.format("%s,%d", count.word, count.count));

        // Execute the job
        env.execute();
    }

    public static class WordWithCount {
        public String word;
        public long count;

        public WordWithCount(String word, long count) {
            this.word = word;
            this.count = count;
        }
    }
} 