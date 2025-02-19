package com.streamcomputing.api.operators;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

/**
 * Sink operator that writes data to a file.
 * @param <T> The type of elements to be written to the file
 */
public class FileSink<T> {
    private final String filePath;
    private final AtomicBoolean isRunning;
    private BufferedWriter writer;
    private int parallelism = 1;
    private Function<T, String> formatter;

    public FileSink(String filePath) {
        this.filePath = filePath;
        this.isRunning = new AtomicBoolean(false);
        this.formatter = Object::toString; // Default formatter
    }

    public FileSink<T> setParallelism(int parallelism) {
        if (parallelism < 1) {
            throw new IllegalArgumentException("Parallelism must be at least 1");
        }
        this.parallelism = parallelism;
        return this;
    }

    public FileSink<T> setFormatter(Function<T, String> formatter) {
        if (formatter == null) {
            throw new IllegalArgumentException("Formatter cannot be null");
        }
        this.formatter = formatter;
        return this;
    }

    public int getParallelism() {
        return parallelism;
    }

    public void open() throws IOException {
        if (isRunning.get()) {
            throw new IllegalStateException("Sink is already open");
        }
        writer = new BufferedWriter(new FileWriter(filePath, true));
        isRunning.set(true);
    }

    public void write(T record) throws IOException {
        if (!isRunning.get()) {
            throw new IllegalStateException("Sink is not open");
        }
        if (writer == null) {
            throw new IllegalStateException("Writer is not initialized");
        }
        
        if (record == null) {
            return; // Skip null records silently
        }
        
        try {
            String formattedRecord = formatter.apply(record);
            if (formattedRecord != null) {
                writer.write(formattedRecord);
                writer.newLine();
                writer.flush();
            }
        } catch (Exception e) {
            throw new IOException("Error writing record: " + e.getMessage(), e);
        }
    }

    public void close() throws IOException {
        isRunning.set(false);
        if (writer != null) {
            try {
                writer.flush();
            } finally {
                writer.close();
                writer = null;
            }
        }
    }
} 