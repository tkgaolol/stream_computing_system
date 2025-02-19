package com.streamcomputing.api.operators;

import java.io.Serializable;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.atomic.AtomicLong;

public class WindowOperator<T, K> implements Serializable {
    private final Duration windowSize;
    private int parallelism = 1;
    private final ConcurrentHashMap<K, WindowBuffer<T>> windows;
    private final AtomicLong lastTriggerTime;
    private transient ScheduledExecutorService scheduler;
    private volatile boolean isRunning = true;
    private final ReentrantLock triggerLock = new ReentrantLock();

    public WindowOperator(Duration windowSize) {
        if (windowSize == null || windowSize.toMillis() <= 0) {
            throw new IllegalArgumentException("Window size must be positive");
        }
        this.windowSize = windowSize;
        this.windows = new ConcurrentHashMap<>();
        this.lastTriggerTime = new AtomicLong(System.currentTimeMillis());
        startWindowTriggerScheduler();
    }

    private void startWindowTriggerScheduler() {
        scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "WindowTriggerThread");
            t.setDaemon(true);
            return t;
        });
        
        long windowMillis = windowSize.toMillis();
        scheduler.scheduleAtFixedRate(
            () -> {
                if (isRunning) {
                    try {
                        triggerWindows();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            },
            windowMillis,
            windowMillis,
            TimeUnit.MILLISECONDS
        );
    }

    public void process(T element, K key) {
        if (!isRunning) {
            throw new IllegalStateException("Window operator has been stopped");
        }
        if (element == null || key == null) {
            return; // Skip null elements or keys
        }
        
        try {
            windows.computeIfAbsent(key, k -> new WindowBuffer<>()).add(element);
        } catch (Exception e) {
            throw new RuntimeException("Error processing element in window", e);
        }
    }

    public Map<K, List<T>> triggerWindows() {
        if (!triggerLock.tryLock()) {
            return new ConcurrentHashMap<>(); // Another trigger is in progress
        }
        
        try {
            long currentTime = System.currentTimeMillis();
            long lastTrigger = lastTriggerTime.get();
            
            if (currentTime - lastTrigger >= windowSize.toMillis()) {
                Map<K, List<T>> result = new ConcurrentHashMap<>();
                
                // Process each window buffer
                windows.forEach((key, buffer) -> {
                    List<T> elements = buffer.getAndClearElements();
                    if (!elements.isEmpty()) {
                        result.put(key, elements);
                    }
                });
                
                lastTriggerTime.set(currentTime);
                return result;
            }
            return new ConcurrentHashMap<>();
        } finally {
            triggerLock.unlock();
        }
    }

    public void setParallelism(int parallelism) {
        if (parallelism < 1) {
            throw new IllegalArgumentException("Parallelism must be at least 1");
        }
        this.parallelism = parallelism;
    }

    public int getParallelism() {
        return parallelism;
    }

    public Duration getWindowSize() {
        return windowSize;
    }

    public void stop() {
        isRunning = false;
        if (scheduler != null && !scheduler.isShutdown()) {
            try {
                scheduler.shutdown();
                if (!scheduler.awaitTermination(1, TimeUnit.SECONDS)) {
                    scheduler.shutdownNow();
                }
            } catch (InterruptedException e) {
                scheduler.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
    }

    private static class WindowBuffer<T> implements Serializable {
        private final List<T> elements;
        private final ReentrantLock lock;

        public WindowBuffer() {
            this.elements = new ArrayList<>();
            this.lock = new ReentrantLock();
        }

        public void add(T element) {
            lock.lock();
            try {
                elements.add(element);
            } finally {
                lock.unlock();
            }
        }

        public List<T> getAndClearElements() {
            lock.lock();
            try {
                List<T> result = new ArrayList<>(elements);
                elements.clear();
                return result;
            } finally {
                lock.unlock();
            }
        }

        public List<T> getElements() {
            lock.lock();
            try {
                return new ArrayList<>(elements);
            } finally {
                lock.unlock();
            }
        }
    }
} 