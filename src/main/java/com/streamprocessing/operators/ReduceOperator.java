package com.streamprocessing.operators;

import com.streamprocessing.api.Operator;
import java.util.HashMap;
import java.util.Map;
import java.util.function.BinaryOperator;

public class ReduceOperator<K, V> implements Operator {
    private final String name;
    private int parallelism;
    private final BinaryOperator<V> reduceFunction;
    private final Map<K, V> state;

    public ReduceOperator(String name, BinaryOperator<V> reduceFunction) {
        this.name = name;
        this.parallelism = 1;
        this.reduceFunction = reduceFunction;
        this.state = new HashMap<>();
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

    public V reduce(K key, V value) {
        V currentValue = state.get(key);
        V newValue = currentValue == null ? value : reduceFunction.apply(currentValue, value);
        state.put(key, newValue);
        return newValue;
    }

    public BinaryOperator<V> getReduceFunction() {
        return reduceFunction;
    }

    public Map<K, V> getState() {
        return state;
    }
} 