package com.streamcomputing.api;

import java.util.function.Function;

/**
 * Interface representing an unbounded stream of data elements.
 * @param <T> The type of elements in the stream
 */
public interface DataStream<T> {
    
    /**
     * Applies a map transformation on the stream.
     * @param mapper The mapping function to apply to each element
     * @param <R> The type of the resulting elements
     * @return A new DataStream with the mapped elements
     */
    <R> DataStream<R> map(Function<T, R> mapper);
    
    /**
     * Groups the stream by a key.
     * @param keyExtractor Function to extract the key from elements
     * @param <K> The type of the key
     * @return A KeyedDataStream grouped by the extracted key
     */
    <K> KeyedDataStream<K, T> keyBy(Function<T, K> keyExtractor);
    
    /**
     * Sets the parallelism for the stream operations.
     * @param parallelism The number of parallel instances
     * @return The DataStream with configured parallelism
     */
    DataStream<T> setParallelism(int parallelism);
    
    /**
     * Writes the stream to a file.
     * @param filePath The path to write the output
     */
    void sink(String filePath);
} 