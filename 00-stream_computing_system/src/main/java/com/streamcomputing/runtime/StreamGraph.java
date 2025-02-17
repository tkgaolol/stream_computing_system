package com.streamcomputing.runtime;

import com.streamcomputing.api.operators.*;
import java.io.Serializable;
import java.util.*;

public class StreamGraph implements Serializable {
    private final Map<String, StreamNode> nodes;
    private final Map<String, List<String>> edges;
    private final Map<String, Integer> parallelism;

    public StreamGraph() {
        this.nodes = new HashMap<>();
        this.edges = new HashMap<>();
        this.parallelism = new HashMap<>();
    }

    public void addNode(String id, Object operator) {
        nodes.put(id, new StreamNode(id, operator));
        edges.put(id, new ArrayList<>());
        
        // Set default parallelism based on operator type
        if (operator instanceof MapOperator) {
            parallelism.put(id, ((MapOperator<?, ?>) operator).getParallelism());
        } else if (operator instanceof ReduceOperator) {
            parallelism.put(id, ((ReduceOperator<?>) operator).getParallelism());
        } else if (operator instanceof KeyByOperator) {
            parallelism.put(id, ((KeyByOperator<?, ?>) operator).getParallelism());
        } else if (operator instanceof WindowOperator) {
            parallelism.put(id, ((WindowOperator<?, ?>) operator).getParallelism());
        } else {
            parallelism.put(id, 1);
        }
    }

    public void addEdge(String fromId, String toId) {
        if (!nodes.containsKey(fromId) || !nodes.containsKey(toId)) {
            throw new IllegalArgumentException("Both nodes must exist in the graph");
        }
        edges.get(fromId).add(toId);
    }

    public void setParallelism(String nodeId, int parallelism) {
        if (!nodes.containsKey(nodeId)) {
            throw new IllegalArgumentException("Node does not exist: " + nodeId);
        }
        this.parallelism.put(nodeId, parallelism);
    }

    public List<String> getDownstream(String nodeId) {
        return edges.getOrDefault(nodeId, Collections.emptyList());
    }

    public StreamNode getNode(String nodeId) {
        return nodes.get(nodeId);
    }

    public int getParallelism(String nodeId) {
        return parallelism.getOrDefault(nodeId, 1);
    }

    public List<String> getSourceNodes() {
        Set<String> allTargets = new HashSet<>();
        edges.values().forEach(allTargets::addAll);
        return nodes.keySet().stream()
                .filter(nodeId -> !allTargets.contains(nodeId))
                .toList();
    }

    public static class StreamNode implements Serializable {
        private final String id;
        private final Object operator;

        public StreamNode(String id, Object operator) {
            this.id = id;
            this.operator = operator;
        }

        public String getId() {
            return id;
        }

        public Object getOperator() {
            return operator;
        }
    }
} 