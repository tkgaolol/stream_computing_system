package com.streamprocessing.operators;

import com.streamprocessing.api.Operator;
import java.util.function.Function;

public class MapOperator<IN, OUT> implements Operator {
    private final String name;
    private int parallelism;
    private final Function<IN, OUT> mapFunction;

    public MapOperator(String name, Function<IN, OUT> mapFunction) {
        this.name = name;
        this.parallelism = 1;
        this.mapFunction = mapFunction;
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

    public OUT map(IN value) {
        return mapFunction.apply(value);
    }

    public Function<IN, OUT> getMapFunction() {
        return mapFunction;
    }
} 