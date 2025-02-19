package com.streamcomputing.runtime;

import com.streamcomputing.api.operators.*;
import java.io.Serializable;
import java.util.*;
import java.util.concurrent.*;

public class JobManager implements Serializable {
    private final StreamGraph streamGraph;
    private final Map<String, ExecutorService> executors;
    private final Map<String, List<TaskManager>> taskManagers;
    private final int numTaskManagers;
    private volatile boolean isRunning = true;

    public JobManager(StreamGraph streamGraph, int numTaskManagers) {
        if (streamGraph == null) {
            throw new IllegalArgumentException("StreamGraph cannot be null");
        }
        if (numTaskManagers <= 0) {
            throw new IllegalArgumentException("Number of task managers must be positive");
        }
        this.streamGraph = streamGraph;
        this.executors = new ConcurrentHashMap<>();
        this.taskManagers = new ConcurrentHashMap<>();
        this.numTaskManagers = numTaskManagers;
    }

    public void start() {
        try {
            // Create executors for each operator based on parallelism
            for (String sourceNode : streamGraph.getSourceNodes()) {
                initializeOperator(sourceNode);
            }
            System.out.println("Job started with " + executors.size() + " operators");
        } catch (Exception e) {
            System.err.println("Error starting job: " + e.getMessage());
            e.printStackTrace();
            shutdown();
            throw new RuntimeException("Failed to start job", e);
        }
    }

    private void initializeOperator(String nodeId) {
        try {
            int parallelism = streamGraph.getParallelism(nodeId);
            System.out.println("Initializing operator " + nodeId + " with parallelism " + parallelism);
            
            ExecutorService executor = Executors.newFixedThreadPool(parallelism, r -> {
                Thread t = new Thread(r, "Operator-" + nodeId + "-Thread");
                t.setDaemon(true);
                return t;
            });
            executors.put(nodeId, executor);

            // Create task managers for this operator
            List<TaskManager> managers = new ArrayList<>();
            for (int i = 0; i < parallelism; i++) {
                TaskManager taskManager = new TaskManager(
                    streamGraph.getNode(nodeId).getOperator(),
                    i,
                    parallelism
                );
                managers.add(taskManager);
                executor.submit(taskManager);
            }
            taskManagers.put(nodeId, managers);

            // Connect with downstream operators
            List<String> downstreamNodes = streamGraph.getDownstream(nodeId);
            for (String downstreamId : downstreamNodes) {
                if (!executors.containsKey(downstreamId)) {
                    initializeOperator(downstreamId);
                }
                
                // Create queues between this operator and downstream operator
                List<TaskManager> downstreamManagers = taskManagers.get(downstreamId);
                for (TaskManager sourceManager : managers) {
                    for (TaskManager targetManager : downstreamManagers) {
                        BlockingQueue<Object> queue = new LinkedBlockingQueue<>(1000);
                        sourceManager.addOutputQueue(targetManager.getSubtaskIndex(), queue);
                        targetManager.addInputQueue(queue);
                    }
                }
            }
        } catch (Exception e) {
            System.err.println("Error initializing operator " + nodeId + ": " + e.getMessage());
            e.printStackTrace();
            throw new RuntimeException("Failed to initialize operator: " + nodeId, e);
        }
    }

    public void shutdown() {
        isRunning = false;
        for (Map.Entry<String, ExecutorService> entry : executors.entrySet()) {
            try {
                ExecutorService executor = entry.getValue();
                executor.shutdown();
                if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
                    executor.shutdownNow();
                }
            } catch (InterruptedException e) {
                entry.getValue().shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
        
        // Stop all task managers
        for (List<TaskManager> managerList : taskManagers.values()) {
            for (TaskManager manager : managerList) {
                manager.stop();
            }
        }
    }

    private static class TaskManager implements Runnable, Serializable {
        private final Object operator;
        private final int subtaskIndex;
        private final int parallelism;
        private volatile boolean running;
        private final List<BlockingQueue<Object>> inputQueues;
        private final Map<Integer, BlockingQueue<Object>> outputQueues;

        public TaskManager(Object operator, int subtaskIndex, int parallelism) {
            if (operator == null) {
                throw new IllegalArgumentException("Operator cannot be null");
            }
            this.operator = operator;
            this.subtaskIndex = subtaskIndex;
            this.parallelism = parallelism;
            this.running = true;
            this.inputQueues = new ArrayList<>();
            this.outputQueues = new HashMap<>();
        }

        @Override
        public void run() {
            try {
                while (running) {
                    try {
                        // Process data based on operator type
                        if (operator instanceof MapOperator) {
                            processMapOperator();
                        } else if (operator instanceof ReduceOperator) {
                            processReduceOperator();
                        } else if (operator instanceof WindowOperator) {
                            processWindowOperator();
                        } else if (operator instanceof KeyByOperator) {
                            processKeyByOperator();
                        } else if (operator instanceof KafkaSource) {
                            processKafkaSource();
                        } else if (operator instanceof FileSink) {
                            processSink();
                        }
                    } catch (Exception e) {
                        System.err.println("Error processing operator: " + e.getMessage());
                        e.printStackTrace();
                        // Continue running unless interrupted
                        if (e instanceof InterruptedException) {
                            Thread.currentThread().interrupt();
                            break;
                        }
                    }
                    Thread.sleep(100); // Prevent CPU overload
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        private void processMapOperator() throws InterruptedException {
            MapOperator<Object, Object> mapOp = (MapOperator<Object, Object>) operator;
            Object input = getNextInput();
            if (input != null) {
                try {
                    Object result = mapOp.map(input);
                    if (result != null) {
                        sendToOutputQueues(result);
                    }
                } catch (Exception e) {
                    System.err.println("Error in map operation: " + e.getMessage());
                    e.printStackTrace();
                }
            }
        }

        private void processReduceOperator() throws InterruptedException {
            ReduceOperator<Object> reduceOp = (ReduceOperator<Object>) operator;
            Object input = getNextInput();
            if (input != null) {
                try {
                    String key = input.toString().split(",")[0];
                    Map<String, Object> state = new HashMap<>();
                    Object currentValue = state.get(key);
                    
                    if (currentValue == null) {
                        state.put(key, input);
                    } else {
                        Object result = reduceOp.reduce(currentValue, input);
                        state.put(key, result);
                        sendToOutputQueues(result);
                    }
                } catch (Exception e) {
                    System.err.println("Error in reduce operation: " + e.getMessage());
                    e.printStackTrace();
                }
            }
        }

        private void processKeyByOperator() throws InterruptedException {
            KeyByOperator<Object, Object> keyByOp = (KeyByOperator<Object, Object>) operator;
            Object input = getNextInput();
            if (input != null) {
                try {
                    Object key = keyByOp.getKey(input);
                    int targetPartition = keyByOp.getPartition(key, parallelism);
                    BlockingQueue<Object> targetQueue = outputQueues.get(targetPartition);
                    if (targetQueue != null) {
                        targetQueue.put(input);
                    }
                } catch (Exception e) {
                    System.err.println("Error in keyBy operation: " + e.getMessage());
                    e.printStackTrace();
                }
            }
        }

        private void processWindowOperator() throws InterruptedException {
            WindowOperator<Object, Object> windowOp = (WindowOperator<Object, Object>) operator;
            Object input = getNextInput();
            if (input != null) {
                try {
                    String key = input.toString().split(",")[0];
                    windowOp.process(input, key);
                    
                    // Check if window should be triggered
                    Map<Object, List<Object>> windowResults = windowOp.triggerWindows();
                    if (!windowResults.isEmpty()) {
                        for (List<Object> results : windowResults.values()) {
                            for (Object result : results) {
                                sendToOutputQueues(result);
                            }
                        }
                    }
                } catch (Exception e) {
                    System.err.println("Error in window operation: " + e.getMessage());
                    e.printStackTrace();
                }
            }
        }

        private void processKafkaSource() throws InterruptedException {
            KafkaSource<Object> source = (KafkaSource<Object>) operator;
            try {
                Object record = source.getElement();
                if (record != null) {
                    sendToOutputQueues(record);
                }
            } catch (Exception e) {
                System.err.println("Error reading from Kafka: " + e.getMessage());
                e.printStackTrace();
            }
        }

        private void processSink() throws InterruptedException {
            FileSink<Object> sink = (FileSink<Object>) operator;
            Object input = getNextInput();
            if (input != null) {
                try {
                    sink.write(input);
                } catch (Exception e) {
                    System.err.println("Error writing to sink: " + e.getMessage());
                    e.printStackTrace();
                }
            }
        }

        private Object getNextInput() throws InterruptedException {
            for (BlockingQueue<Object> queue : inputQueues) {
                Object input = queue.poll(100, TimeUnit.MILLISECONDS);
                if (input != null) {
                    return input;
                }
            }
            return null;
        }

        private void sendToOutputQueues(Object data) throws InterruptedException {
            for (BlockingQueue<Object> queue : outputQueues.values()) {
                int retries = 3;
                while (retries > 0) {
                    if (queue.offer(data, 1, TimeUnit.SECONDS)) {
                        break;
                    }
                    retries--;
                }
                if (retries == 0) {
                    System.err.println("Failed to send data to output queue after 3 retries");
                }
            }
        }

        public void addInputQueue(BlockingQueue<Object> queue) {
            if (queue != null) {
                inputQueues.add(queue);
            }
        }

        public void addOutputQueue(int targetSubtask, BlockingQueue<Object> queue) {
            if (queue != null) {
                outputQueues.put(targetSubtask, queue);
            }
        }

        public int getSubtaskIndex() {
            return subtaskIndex;
        }

        public void stop() {
            running = false;
        }
    }
} 