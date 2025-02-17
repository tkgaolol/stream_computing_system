package com.streamcomputing.runtime;

import com.streamcomputing.api.DataStream;
import com.streamcomputing.api.KeyedDataStream;
import com.streamcomputing.api.WindowedStream;

import java.util.HashMap;
import java.util.Map;
import java.util.function.BinaryOperator;
import java.util.function.Function;

/**
 * Basic implementation of the KeyedDataStream interface.
 * @param <K> The type of the key
 * @param <T> The type of elements in the stream
 */
public class BasicKeyedDataStream<K, T> implements KeyedDataStream<K, T> {
    private final BasicDataStream<T> input;
    private final Function<T, K> keySelector;
    private int parallelism = 1;

    public BasicKeyedDataStream(BasicDataStream<T> input, Function<T, K> keySelector) {
        this.input = input;
        this.keySelector = keySelector;
    }

    @Override
    public DataStream<T> reduce(BinaryOperator<T> reducer) {
        BasicDataStream<T> result = new BasicDataStream<>();
        Map<K, T> state = new HashMap<>();

        Thread reduceThread = new Thread(() -> {
            try {
                while (true) {
                    T element = input.getElement();
                    K key = keySelector.apply(element);
                    
                    T currentValue = state.get(key);
                    if (currentValue == null) {
                        state.put(key, element);
                    } else {
                        T reduced = reducer.apply(currentValue, element);
                        state.put(key, reduced);
                    }
                    result.addElement(state.get(key));
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        reduceThread.start();
        return result;
    }

    @Override
    public KeyedDataStream<K, T> setParallelism(int parallelism) {
        this.parallelism = parallelism;
        return this;
    }

    @Override
    public WindowedStream<T> tumblingWindow(long windowSizeMillis) {
        return new BasicWindowedStream<>(input, windowSizeMillis);
    }
} 