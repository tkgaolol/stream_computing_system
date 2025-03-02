package com.streamcomputing.runtime;

import com.streamcomputing.api.KeyedDataStream;
import com.streamcomputing.api.DataStream;

import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation of KeyedDataStream that groups elements by key.
 * @param <K> The type of the key
 * @param <V> The type of the values
 */
public class KeyedStream<K, V> implements KeyedDataStream<K, V> {
    private static final Logger logger = LoggerFactory.getLogger(KeyedStream.class);
    private final BaseStream<V> inputStream;
    private final Function<V, K> keyExtractor;
    private int parallelism;
    
    public KeyedStream(BaseStream<V> inputStream, Function<V, K> keyExtractor, int parallelism) {
        this.inputStream = inputStream;
        this.keyExtractor = keyExtractor;
        this.parallelism = parallelism;
    }
    
    @Override
    public DataStream<V> reduce(BinaryOperator<V> reducer) {
        return new ReduceStream<>(this, reducer, parallelism);
    }
    
    @Override
    public KeyedDataStream<K, V> setParallelism(int parallelism) {
        this.parallelism = parallelism;
        return this;
    }
    
    /**
     * Process the keyed stream with the given processor function.
     * @param processor The function to process each element
     * @param <R> The type of the result
     */
    public <R> void process(Function<V, R> processor) {
        ConcurrentHashMap<K, V> state = new ConcurrentHashMap<>();
        
        inputStream.process(value -> {
            try {
                K key = keyExtractor.apply(value);
                state.put(key, value);
                return processor.apply(value);
            } catch (Exception e) {
                logger.error("Error processing keyed stream", e);
                return null;
            }
        });
    }
    
    public Function<V, K> getKeyExtractor() {
        return keyExtractor;
    }
    
    public BaseStream<V> getInputStream() {
        return inputStream;
    }
} 