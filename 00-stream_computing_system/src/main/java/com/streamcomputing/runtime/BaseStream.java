package com.streamcomputing.runtime;

import com.streamcomputing.api.DataStream;
import com.streamcomputing.api.KeyedDataStream;
import java.util.function.Function;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base implementation of DataStream interface.
 * @param <T> The type of elements in the stream
 */
public abstract class BaseStream<T> implements DataStream<T> {
    private static final Logger logger = LoggerFactory.getLogger(BaseStream.class);
    protected int parallelism = 1;
    
    @Override
    public <R> DataStream<R> map(Function<T, R> mapper) {
        return new MapStream<>(this, mapper, parallelism);
    }
    
    @Override
    public <K> KeyedDataStream<K, T> keyBy(Function<T, K> keyExtractor) {
        return new KeyedStream<>(this, keyExtractor, parallelism);
    }
    
    @Override
    public DataStream<T> setParallelism(int parallelism) {
        this.parallelism = parallelism;
        return this;
    }
    
    @Override
    public void sink(String filePath) {
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(filePath))) {
            process(value -> {
                try {
                    writer.write(value.toString());
                    writer.newLine();
                    writer.flush();
                } catch (IOException e) {
                    logger.error("Error writing to sink file", e);
                }
                return null;
            });
        } catch (IOException e) {
            logger.error("Error creating sink file", e);
        }
    }
    
    /**
     * Process the stream with the given processor function.
     * @param processor The function to process each element
     * @param <R> The type of the result
     */
    public abstract <R> void process(Function<T, R> processor);
} 