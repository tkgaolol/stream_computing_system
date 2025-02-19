package com.streamcomputing.api.operators;

import java.io.Serializable;
import java.util.function.Function;

public class KeyByOperator<IN, K> implements Serializable {
    private final Function<IN, K> keySelector;
    private int parallelism = 1;

    public KeyByOperator(Function<IN, K> keySelector) {
        this.keySelector = keySelector;
    }

    public K getKey(IN value) {
        return keySelector.apply(value);
    }

    public void setParallelism(int parallelism) {
        this.parallelism = parallelism;
    }

    public int getParallelism() {
        return parallelism;
    }

    public Function<IN, K> getKeySelector() {
        return keySelector;
    }

    public int getPartition(K key, int numPartitions) {
        return Math.abs(key.hashCode() % numPartitions);
    }
} 