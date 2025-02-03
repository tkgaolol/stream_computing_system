package com.streamprocessing.runtime;

import com.streamprocessing.api.DataStream;
import com.streamprocessing.api.Operator;
import com.streamprocessing.operators.*;

import java.util.ArrayList;
import java.util.List;
import java.util.function.BinaryOperator;
import java.util.function.Function;

public class StreamExecutionEnvironment {
    private final List<DataStream<?>> streams;
    private final String jobName;

    public StreamExecutionEnvironment(String jobName) {
        this.jobName = jobName;
        this.streams = new ArrayList<>();
    }

    public DataStream<String> addKafkaSource(String name, String bootstrapServers, String topic, String groupId) {
        SourceOperator source = new SourceOperator(name, bootstrapServers, topic, groupId);
        DataStream<String> stream = new DataStream<>(name);
        stream.addOperator(source);
        streams.add(stream);
        return stream;
    }

    public <T, R> DataStream<R> map(DataStream<T> input, String name, Function<T, R> mapFunction) {
        MapOperator<T, R> mapOperator = new MapOperator<>(name, mapFunction);
        DataStream<R> result = new DataStream<>(name);
        result.addOperator(mapOperator);
        streams.add(result);
        return result;
    }

    public <T, K> DataStream<T> keyBy(DataStream<T> input, String name, Function<T, K> keySelector) {
        KeyByOperator<T, K> keyByOperator = new KeyByOperator<>(name, keySelector);
        DataStream<T> result = new DataStream<>(name);
        result.addOperator(keyByOperator);
        streams.add(result);
        return result;
    }

    public <K, V> DataStream<V> reduce(DataStream<V> input, String name, BinaryOperator<V> reduceFunction) {
        ReduceOperator<K, V> reduceOperator = new ReduceOperator<>(name, reduceFunction);
        DataStream<V> result = new DataStream<>(name);
        result.addOperator(reduceOperator);
        streams.add(result);
        return result;
    }

    public <T> void addFileSink(DataStream<T> input, String name, String filePath, Function<T, String> formatter) {
        SinkOperator<T> sinkOperator = new SinkOperator<>(name, filePath, formatter);
        input.addOperator(sinkOperator);
    }

    public void execute() {
        JobExecutor executor = new JobExecutor(jobName, streams);
        executor.execute();
    }
} 