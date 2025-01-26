package com.streamprocessing.api;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * Core abstraction representing an unbounded stream of data.
 */
public class DataStream<T> implements Serializable {
    private final String name;
    private final List<Operator> operators;
    private int parallelism;

    public DataStream(String name) {
        this.name = name;
        this.operators = new ArrayList<>();
        this.parallelism = 1;
    }

    public String getName() {
        return name;
    }

    public List<Operator> getOperators() {
        return operators;
    }

    public int getParallelism() {
        return parallelism;
    }

    public void setParallelism(int parallelism) {
        this.parallelism = parallelism;
    }

    public void addOperator(Operator operator) {
        operators.add(operator);
    }
} 