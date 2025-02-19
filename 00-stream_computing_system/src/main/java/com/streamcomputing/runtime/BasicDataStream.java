package com.streamcomputing.runtime;

import com.streamcomputing.api.DataStream;
import com.streamcomputing.api.KeyedDataStream;
import com.streamcomputing.api.WindowedStream;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.function.Function;

/**
 * Basic implementation of the DataStream interface.
 * @param <T> The type of elements in the stream
 */
public class BasicDataStream<T> implements DataStream<T> {
    private static final int DEFAULT_BUFFER_CAPACITY = 1000;
    private final BlockingQueue<T> buffer;
    private int parallelism = 1;
    private final ExecutorService executorService;
    private volatile boolean isRunning = true;
    private final List<Thread> workerThreads;

    public BasicDataStream() {
        this(DEFAULT_BUFFER_CAPACITY);
    }

    public BasicDataStream(int bufferCapacity) {
        if (bufferCapacity <= 0) {
            throw new IllegalArgumentException("Buffer capacity must be greater than 0");
        }
        this.buffer = new ArrayBlockingQueue<>(bufferCapacity);
        this.executorService = Executors.newCachedThreadPool(r -> {
            Thread t = new Thread(r);
            t.setDaemon(true);
            return t;
        });
        this.workerThreads = new ArrayList<>();
    }

    @Override
    public <R> DataStream<R> map(Function<T, R> mapper) {
        if (mapper == null) {
            throw new IllegalArgumentException("Mapper function cannot be null");
        }

        BasicDataStream<R> result = new BasicDataStream<>();
        for (int i = 0; i < parallelism; i++) {
            Thread mapThread = new Thread(() -> {
                try {
                    while (isRunning && !Thread.currentThread().isInterrupted()) {
                        T element = buffer.poll(100, TimeUnit.MILLISECONDS);
                        if (element != null) {
                            try {
                                R mappedElement = mapper.apply(element);
                                if (mappedElement != null) {
                                    result.buffer.put(mappedElement);
                                }
                            } catch (Exception e) {
                                e.printStackTrace();
                                // Continue processing other elements
                            }
                        }
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
            mapThread.setDaemon(true);
            workerThreads.add(mapThread);
            mapThread.start();
        }
        return result;
    }

    @Override
    public <K> KeyedDataStream<K, T> keyBy(Function<T, K> keySelector) {
        if (keySelector == null) {
            throw new IllegalArgumentException("Key selector cannot be null");
        }
        return new BasicKeyedDataStream<>(this, keySelector);
    }

    @Override
    public DataStream<T> setParallelism(int parallelism) {
        if (parallelism <= 0) {
            throw new IllegalArgumentException("Parallelism must be greater than 0");
        }
        this.parallelism = parallelism;
        return this;
    }

    @Override
    public WindowedStream<T> tumblingWindow(long windowSizeMillis) {
        if (windowSizeMillis <= 0) {
            throw new IllegalArgumentException("Window size must be greater than 0");
        }
        return new BasicWindowedStream<>(this, windowSizeMillis);
    }

    public void addElement(T element) throws InterruptedException {
        if (!isRunning) {
            throw new IllegalStateException("Stream is not running");
        }
        if (element != null) {
            boolean added = false;
            while (!added && isRunning) {
                try {
                    added = buffer.offer(element, 100, TimeUnit.MILLISECONDS);
                } catch (InterruptedException e) {
                    if (!isRunning) {
                        throw e;
                    }
                }
            }
            if (!added && isRunning) {
                throw new InterruptedException("Failed to add element to buffer after retries");
            }
        }
    }

    public T getElement() throws InterruptedException {
        if (!isRunning) {
            throw new IllegalStateException("Stream is not running");
        }
        return buffer.take();
    }

    public T getElement(long timeout, TimeUnit unit) throws InterruptedException {
        if (!isRunning) {
            throw new IllegalStateException("Stream is not running");
        }
        T element = buffer.poll(timeout, unit);
        if (element == null && !isRunning) {
            throw new InterruptedException("Stream is shutting down");
        }
        return element;
    }

    public void shutdown() {
        isRunning = false;
        for (Thread thread : workerThreads) {
            thread.interrupt();
        }
        executorService.shutdown();
        try {
            if (!executorService.awaitTermination(1, TimeUnit.SECONDS)) {
                executorService.shutdownNow();
            }
        } catch (InterruptedException e) {
            executorService.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    public int getParallelism() {
        return parallelism;
    }

    public boolean isRunning() {
        return isRunning;
    }
} 