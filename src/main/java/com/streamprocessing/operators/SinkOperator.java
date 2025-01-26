package com.streamprocessing.operators;

import com.streamprocessing.api.Operator;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.function.Function;

public class SinkOperator<T> implements Operator {
    private final String name;
    private int parallelism;
    private final String filePath;
    private final Function<T, String> formatter;
    private BufferedWriter writer;

    public SinkOperator(String name, String filePath, Function<T, String> formatter) {
        this.name = name;
        this.parallelism = 1;
        this.filePath = filePath;
        this.formatter = formatter;
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

    public void open() throws IOException {
        writer = new BufferedWriter(new FileWriter(filePath, true));
    }

    public void write(T value) throws IOException {
        if (writer != null) {
            writer.write(formatter.apply(value));
            writer.newLine();
            writer.flush();
        }
    }

    public void close() throws IOException {
        if (writer != null) {
            writer.close();
        }
    }

    public String getFilePath() {
        return filePath;
    }

    public Function<T, String> getFormatter() {
        return formatter;
    }
} 