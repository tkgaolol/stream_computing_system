package com.streamcomputing.api.operators;

import java.io.Serializable;
import java.util.function.BinaryOperator;

public class ReduceOperator<T> implements Serializable {
    private final BinaryOperator<T> reduceFunction;
    private int parallelism = 1;

    public ReduceOperator(BinaryOperator<T> reduceFunction) {
        this.reduceFunction = reduceFunction;
    }

    public T reduce(T value1, T value2) {
        return reduceFunction.apply(value1, value2);
    }

    public void setParallelism(int parallelism) {
        this.parallelism = parallelism;
    }

    public int getParallelism() {
        return parallelism;
    }

    public BinaryOperator<T> getReduceFunction() {
        return reduceFunction;
    }
} 