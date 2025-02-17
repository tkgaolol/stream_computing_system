package com.streamcomputing.runtime;

import com.streamcomputing.api.DataStream;
import com.streamcomputing.api.WindowedStream;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.BinaryOperator;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Basic implementation of the WindowedStream interface.
 * @param <T> The type of elements in the stream
 */
public class BasicWindowedStream<T> implements WindowedStream<T> {
    private final BasicDataStream<T> input;
    private final long windowSizeMillis;
    private int parallelism = 1;
    private final AtomicBoolean isRunning = new AtomicBoolean(true);
    private final AtomicLong processedCount = new AtomicLong(0);
    private static final long MIN_WINDOW_SIZE = 100; // Minimum window size in milliseconds
    private static final long POLL_TIMEOUT = 10; // Reduced poll timeout for faster processing
    private static final long MAX_BUFFER_TIME = 100; // Maximum time to buffer elements before forcing window trigger

    public BasicWindowedStream(BasicDataStream<T> input, long windowSizeMillis) {
        if (windowSizeMillis <= 0) {
            throw new IllegalArgumentException("Window size must be greater than 0");
        }
        this.input = input;
        this.windowSizeMillis = Math.max(windowSizeMillis, MIN_WINDOW_SIZE);
        System.out.println("Created window with size: " + this.windowSizeMillis + "ms");
    }

    @Override
    public DataStream<T> reduce(BinaryOperator<T> reducer) {
        if (reducer == null) {
            throw new IllegalArgumentException("Reducer function cannot be null");
        }

        BasicDataStream<T> result = new BasicDataStream<>();
        
        Thread windowThread = new Thread(() -> {
            try {
                List<T> window = new ArrayList<>();
                long windowStartTime = System.currentTimeMillis();
                // Align window to current time
                long nextWindowEnd = windowStartTime + windowSizeMillis;
                System.out.println("Starting window processing thread. First window ends at: " + nextWindowEnd);

                while (isRunning.get() && !Thread.currentThread().isInterrupted()) {
                    long currentTime = System.currentTimeMillis();
                    long bufferStartTime = currentTime;
                    
                    // Try to get elements until window is full or timeout
                    while (currentTime < nextWindowEnd && 
                           currentTime - bufferStartTime < MAX_BUFFER_TIME) {
                        T element = null;
                        try {
                            element = input.getElement(POLL_TIMEOUT, TimeUnit.MILLISECONDS);
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            break;
                        }

                        if (element != null) {
                            window.add(element);
                            processedCount.incrementAndGet();
                            System.out.println("Added element to window: " + element);
                            
                            // Process window immediately if we have enough elements
                            if (window.size() >= 10) {
                                break;
                            }
                        }
                        
                        currentTime = System.currentTimeMillis();
                    }
                    
                    // Process window when it's time or we have buffered elements
                    if (!window.isEmpty()) {
                        System.out.println("Processing window with " + window.size() + " elements at time: " + currentTime);
                        try {
                            T reduced = window.stream().reduce(reducer).orElse(null);
                            if (reduced != null) {
                                result.addElement(reduced);
                                System.out.println("Window result: " + reduced);
                            }
                        } catch (Exception e) {
                            System.err.println("Error reducing window: " + e.getMessage());
                            e.printStackTrace();
                        }
                        window.clear();
                        
                        // Align next window end time to current time
                        nextWindowEnd = currentTime + windowSizeMillis;
                        System.out.println("Next window ends at: " + nextWindowEnd);
                    } else {
                        // Only advance window if we have no buffered elements
                        System.out.println("Skipping empty window at time: " + currentTime);
                        nextWindowEnd = currentTime + windowSizeMillis;
                        System.out.println("Next window ends at: " + nextWindowEnd);
                    }
                }

                // Process any remaining elements in the final window
                if (!window.isEmpty()) {
                    System.out.println("Processing final window with " + window.size() + " elements");
                    try {
                        T reduced = window.stream().reduce(reducer).orElse(null);
                        if (reduced != null) {
                            result.addElement(reduced);
                            System.out.println("Final window result: " + reduced);
                        }
                    } catch (Exception e) {
                        System.err.println("Error reducing final window: " + e.getMessage());
                        e.printStackTrace();
                    }
                }
            } catch (Exception e) {
                System.err.println("Error in window processing: " + e.getMessage());
                e.printStackTrace();
            } finally {
                isRunning.set(false);
                System.out.println("Window processing thread finished. Total processed: " + processedCount.get());
            }
        }, "WindowProcessor-" + windowSizeMillis + "ms");

        windowThread.setDaemon(true);  // Make it a daemon thread
        windowThread.start();
        return result;
    }

    @Override
    public WindowedStream<T> setParallelism(int parallelism) {
        if (parallelism <= 0) {
            throw new IllegalArgumentException("Parallelism must be greater than 0");
        }
        this.parallelism = parallelism;
        return this;
    }

    public void stop() {
        System.out.println("Stopping window processing");
        isRunning.set(false);
    }

    public long getProcessedCount() {
        return processedCount.get();
    }
} 