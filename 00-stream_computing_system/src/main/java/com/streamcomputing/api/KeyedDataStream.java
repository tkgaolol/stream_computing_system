package com.streamcomputing.api;

import java.util.function.BinaryOperator;

/**
 * Represents a data stream that has been keyed by some property.
 * @param <K> The type of the key
 * @param <T> The type of elements in the stream
 */
public interface KeyedDataStream<K, T> {
    /**
     * Applies a reduce function to the keyed stream.
     * @param reducer The reduce function to apply
     * @return A new DataStream with reduced elements
     */
    DataStream<T> reduce(BinaryOperator<T> reducer);

    /**
     * Sets the parallelism for the current operation.
     * @param parallelism The number of parallel instances
     * @return The KeyedDataStream with set parallelism
     */
    KeyedDataStream<K, T> setParallelism(int parallelism);

    /**
     * Creates a tumbling window based on processing time.
     * @param windowSizeMillis The size of the window in milliseconds
     * @return A WindowedStream for further operations
     */
    WindowedStream<T> tumblingWindow(long windowSizeMillis);
} 