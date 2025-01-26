package com.streamprocessing.operators;

import com.streamprocessing.api.Operator;
import java.util.function.Function;

public class KeyByOperator<T, K> implements Operator {
    private final String name;
    private int parallelism;
    private final Function<T, K> keySelector;

    public KeyByOperator(String name, Function<T, K> keySelector) {
        this.name = name;
        this.parallelism = 1;
        this.keySelector = keySelector;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public int getParallelism() {
        return parallelism;
    }

    @Override
    public void setParallelism(int parallelism) {
        this.parallelism = parallelism;
    }

    public K getKey(T value) {
        return keySelector.apply(value);
    }

    public Function<T, K> getKeySelector() {
        return keySelector;
    }

    public int getPartition(K key, int numPartitions) {
        return Math.abs(key.hashCode() % numPartitions);
    }
} 