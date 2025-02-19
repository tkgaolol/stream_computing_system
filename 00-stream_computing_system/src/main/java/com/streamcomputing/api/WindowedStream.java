package com.streamcomputing.api;

import java.util.function.BinaryOperator;

/**
 * Represents a windowed view of a data stream.
 * @param <T> The type of elements in the stream
 */
public interface WindowedStream<T> {
    /**
     * Applies a reduce function to the elements in each window.
     * @param reducer The reduce function to apply
     * @return A new DataStream with reduced elements
     */
    DataStream<T> reduce(BinaryOperator<T> reducer);

    /**
     * Sets the parallelism for the current operation.
     * @param parallelism The number of parallel instances
     * @return The WindowedStream with set parallelism
     */
    WindowedStream<T> setParallelism(int parallelism);
} 