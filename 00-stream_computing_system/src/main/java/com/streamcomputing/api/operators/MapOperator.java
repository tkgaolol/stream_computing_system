package com.streamcomputing.api.operators;

import java.io.Serializable;
import java.util.function.Function;

public class MapOperator<IN, OUT> implements Serializable {
    private final Function<IN, OUT> mapFunction;
    private int parallelism = 1;

    public MapOperator(Function<IN, OUT> mapFunction) {
        this.mapFunction = mapFunction;
    }

    public OUT map(IN value) {
        return mapFunction.apply(value);
    }

    public void setParallelism(int parallelism) {
        this.parallelism = parallelism;
    }

    public int getParallelism() {
        return parallelism;
    }

    public Function<IN, OUT> getMapFunction() {
        return mapFunction;
    }
} 