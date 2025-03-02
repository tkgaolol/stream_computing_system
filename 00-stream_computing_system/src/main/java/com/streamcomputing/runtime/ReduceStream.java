package com.streamcomputing.runtime;

import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation of DataStream that performs reduce operations on keyed streams.
 * @param <K> The type of the key
 * @param <V> The type of the values
 */
public class ReduceStream<K, V> extends BaseStream<V> {
    private static final Logger logger = LoggerFactory.getLogger(ReduceStream.class);
    private final KeyedStream<K, V> keyedStream;
    private final BinaryOperator<V> reducer;
    private final ConcurrentHashMap<K, V> state;
    
    public ReduceStream(KeyedStream<K, V> keyedStream, BinaryOperator<V> reducer, int parallelism) {
        this.keyedStream = keyedStream;
        this.reducer = reducer;
        this.parallelism = parallelism;
        this.state = new ConcurrentHashMap<>();
    }
    
    @Override
    public <R> void process(Function<V, R> processor) {
        keyedStream.getInputStream().process(value -> {
            try {
                K key = keyedStream.getKeyExtractor().apply(value);
                V currentValue = state.merge(key, value, reducer);
                return processor.apply(currentValue);
            } catch (Exception e) {
                logger.error("Error in reduce operation", e);
                return null;
            }
        });
    }
} 