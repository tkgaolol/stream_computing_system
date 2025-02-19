package com.streamcomputing.api;

import java.util.function.Function;

/**
 * Represents an abstract data stream that supports various stream operations.
 * This is the main abstraction for working with continuous data flows in the stream computing system.
 * DataStream supports transformations, keying, windowing, and parallel processing capabilities.
 *
 * <p>Example usage:
 * <pre>{@code
 * DataStream<String> input = source.createDataStream();
 * DataStream<Integer> mapped = input
 *     .map(String::length)
 *     .setParallelism(2);
 * }</pre>
 *
 * @param <T> The type of elements in the stream
 */
public interface DataStream<T> {
    /**
     * Applies a map transformation to each element in the stream.
     * The map transformation creates a new stream by transforming each element of the input stream
     * into exactly one element of the output stream using the provided mapping function.
     *
     * <p>Example usage:
     * <pre>{@code
     * DataStream<String> input = ...;
     * DataStream<Integer> lengths = input.map(String::length);
     * }</pre>
     *
     * @param mapper The mapping function to apply to each element
     * @param <R> The type of the output elements
     * @return A new DataStream containing the transformed elements
     * @throws IllegalArgumentException if the mapper function is null
     */
    <R> DataStream<R> map(Function<T, R> mapper);

    /**
     * Groups the stream by a key extracted from each element.
     * The keyBy operation partitions the stream based on the key value returned by the keySelector function.
     * Elements with the same key are guaranteed to be processed by the same downstream operator instance.
     *
     * <p>Example usage:
     * <pre>{@code
     * DataStream<String> words = ...;
     * KeyedDataStream<String, String> keyedWords = words.keyBy(word -> word);
     * }</pre>
     *
     * @param keySelector Function to extract the key from elements
     * @param <K> The type of the key
     * @return A KeyedDataStream grouped by the specified key
     * @throws IllegalArgumentException if the keySelector function is null
     */
    <K> KeyedDataStream<K, T> keyBy(Function<T, K> keySelector);

    /**
     * Sets the parallelism for the current operation.
     * The parallelism defines how many parallel instances of the operation will be created.
     * This allows for horizontal scaling of the computation.
     *
     * <p>Example usage:
     * <pre>{@code
     * DataStream<String> input = ...;
     * DataStream<String> parallelStream = input.setParallelism(4);
     * }</pre>
     *
     * @param parallelism The number of parallel instances to create
     * @return The DataStream with set parallelism
     * @throws IllegalArgumentException if parallelism is less than or equal to 0
     */
    DataStream<T> setParallelism(int parallelism);

    /**
     * Creates a tumbling window based on processing time.
     * A tumbling window assigns elements to fixed-size, non-overlapping windows
     * based on the current processing time. When the window closes, all accumulated
     * elements are processed together.
     *
     * <p>Example usage:
     * <pre>{@code
     * DataStream<String> input = ...;
     * WindowedStream<String> windowed = input.tumblingWindow(5000); // 5 second window
     * }</pre>
     *
     * @param windowSizeMillis The size of the window in milliseconds
     * @return A WindowedStream for further operations
     * @throws IllegalArgumentException if windowSizeMillis is less than or equal to 0
     */
    WindowedStream<T> tumblingWindow(long windowSizeMillis);
} 