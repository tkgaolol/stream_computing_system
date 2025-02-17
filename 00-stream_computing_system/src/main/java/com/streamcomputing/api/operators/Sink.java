package com.streamcomputing.api.operators;

import java.io.Serializable;

public interface Sink<T> extends Serializable {
    void write(T message);
} 