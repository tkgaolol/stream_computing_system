package com.streamcomputing.runtime;

import java.util.function.Function;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.CountDownLatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation of DataStream that applies a map transformation.
 * @param <T> The type of input elements
 * @param <R> The type of output elements
 */
public class MapStream<T, R> extends BaseStream<R> {
    private static final Logger logger = LoggerFactory.getLogger(MapStream.class);
    private final BaseStream<T> inputStream;
    private final Function<T, R> mapper;
    
    public MapStream(BaseStream<T> inputStream, Function<T, R> mapper, int parallelism) {
        this.inputStream = inputStream;
        this.mapper = mapper;
        this.parallelism = parallelism;
    }
    
    @Override
    public <U> void process(Function<R, U> processor) {
        ExecutorService executor = Executors.newFixedThreadPool(parallelism);
        CountDownLatch latch = new CountDownLatch(parallelism);
        
        for (int i = 0; i < parallelism; i++) {
            executor.submit(() -> {
                try {
                    inputStream.process(value -> {
                        try {
                            R mappedValue = mapper.apply(value);
                            return processor.apply(mappedValue);
                        } catch (Exception e) {
                            logger.error("Error in map operation", e);
                            return null;
                        }
                    });
                } finally {
                    latch.countDown();
                }
            });
        }
        
        try {
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.error("Interrupted while waiting for map operations to complete", e);
        } finally {
            executor.shutdown();
        }
    }
} 