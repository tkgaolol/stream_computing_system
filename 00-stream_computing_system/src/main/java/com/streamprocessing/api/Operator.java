package com.streamprocessing.api;

import java.io.Serializable;

/**
 * Base interface for all stream operators.
 */
public interface Operator extends Serializable {
    String getName();
    int getParallelism();
    void setParallelism(int parallelism);
} 