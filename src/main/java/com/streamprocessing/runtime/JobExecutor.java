package com.streamprocessing.runtime;

import com.streamprocessing.api.DataStream;
import com.streamprocessing.api.Operator;
import com.streamprocessing.operators.*;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class JobExecutor {
    private static final Logger LOG = LoggerFactory.getLogger(JobExecutor.class);
    private final String jobName;
    private final List<DataStream<?>> streams;
    private final ExecutorService executorService;
    private volatile boolean running;

    public JobExecutor(String jobName, List<DataStream<?>> streams) {
        this.jobName = jobName;
        this.streams = streams;
        this.executorService = Executors.newCachedThreadPool();
        this.running = true;
    }

    public void execute() {
        LOG.info("Starting job: {}", jobName);

        // Start source operators
        for (DataStream<?> stream : streams) {
            for (Operator operator : stream.getOperators()) {
                if (operator instanceof SourceOperator) {
                    startSourceOperator((SourceOperator) operator);
                }
            }
        }

        // Wait for termination
        try {
            executorService.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            LOG.error("Job execution interrupted", e);
            Thread.currentThread().interrupt();
        }
    }

    private void startSourceOperator(SourceOperator source) {
        for (int i = 0; i < source.getParallelism(); i++) {
            executorService.submit(() -> {
                try (KafkaConsumer<String, String> consumer = source.createConsumer()) {
                    consumer.subscribe(Collections.singletonList(source.getTopic()));
                    
                    while (running) {
                        ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                        for (ConsumerRecord<String, String> record : records) {
                            processRecord(record.value());
                        }
                    }
                }
                return null;
            });
        }
    }

    @SuppressWarnings("unchecked")
    private <T> void processRecord(T record) {
        Object currentValue = record;

        for (DataStream<?> stream : streams) {
            for (Operator operator : stream.getOperators()) {
                if (operator instanceof MapOperator) {
                    MapOperator<Object, Object> mapOp = (MapOperator<Object, Object>) operator;
                    currentValue = mapOp.map(currentValue);
                } else if (operator instanceof KeyByOperator) {
                    KeyByOperator<Object, Object> keyByOp = (KeyByOperator<Object, Object>) operator;
                    Object key = keyByOp.getKey(currentValue);
                    int partition = keyByOp.getPartition(key, keyByOp.getParallelism());
                    // In a real implementation, we would route to different tasks based on partition
                } else if (operator instanceof ReduceOperator) {
                    ReduceOperator<Object, Object> reduceOp = (ReduceOperator<Object, Object>) operator;
                    // In a real implementation, we would maintain state per key
                    currentValue = reduceOp.reduce(null, currentValue);
                } else if (operator instanceof SinkOperator) {
                    SinkOperator<Object> sinkOp = (SinkOperator<Object>) operator;
                    try {
                        sinkOp.open();
                        sinkOp.write(currentValue);
                        sinkOp.close();
                    } catch (IOException e) {
                        LOG.error("Error writing to sink", e);
                    }
                }
            }
        }
    }

    public void shutdown() {
        running = false;
        executorService.shutdown();
        try {
            if (!executorService.awaitTermination(60, TimeUnit.SECONDS)) {
                executorService.shutdownNow();
            }
        } catch (InterruptedException e) {
            executorService.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }
} 