package com.streamcomputing.api;

import java.util.function.BinaryOperator;

/**
 * Interface representing a keyed data stream.
 * @param <K> The type of the key
 * @param <V> The type of the values
 */
public interface KeyedDataStream<K, V> {
    
    /**
     * Applies a reduce function to the keyed stream.
     * @param reducer The reduce function to apply
     * @return A DataStream with the reduced values
     */
    DataStream<V> reduce(BinaryOperator<V> reducer);
    
    /**
     * Sets the parallelism for the stream operations.
     * @param parallelism The number of parallel instances
     * @return The KeyedDataStream with configured parallelism
     */
    KeyedDataStream<K, V> setParallelism(int parallelism);
} 