# Simplified Stream Processing System Design

## 1. Background

### 1.1 Project Introduction
Stream processing systems are widely used in big data scenarios such as terminal data, financial risk control, and real-time recommendations. Unlike traditional batch processing systems, stream processing systems offer real-time, continuous, and out-of-order processing capabilities. This project involves group collaboration to design and develop a simple stream processing system, enhancing skills in programming, data structures, networking, distributed systems, and big data components.

### 1.2 Project Objectives
The project is divided into three levels, each with specific functionalities and corresponding scores. Teamwork and coordination are crucial.

| Objective | Description |
|-----------|-------------|
| Basic Stream System (Mandatory) | Multi-node, end-to-end, sustainable stream system with basic functionalities |
| Intermediate Features (Optional) | Event-time window processing with limited out-of-order data, retract processing, and automatic fault recovery |
| Advanced Features (Not Required) | Exactly once processing semantics |

### 1.3 Project Requirements
- Java is recommended but not mandatory
- Self-provided development and testing machines
- Self-setup Kafka, version not specified

## 2. Detailed Design

### 2. Basic Features

#### 2.1 Design Basic Stream Job API
Design and implement a stream processing API to create applications. Define an abstract data structure for an unbounded data stream (DataStream) and an API that supports:

1. Five basic operators:
   - Source: Kafka to DataStream
   - Sink: DataStream to external file
   - Map: DataStream to DataStream
   - KeyBy: DataStream to partitioned DataStream
   - Reduce: Partitioned DataStream to DataStream
Note: A partitioned DataStream refers to a DataStream that has been divided into groups based on keys using the KeyBy operator. Each partition contains elements with the same key value.

2. Processing-time based rolling windows
3. Operator concurrency specification
4. DAG-based operator orchestration

#### 2.2 Implement Basic Distributed Stream Processing Engine
Design and implement a distributed engine supporting all API operators. It should include:

- CLI/script for application submission
- Logic distribution and operator scheduling across nodes
- Intermediate data shuffle between nodes

#### 2.3 Implement a Stream WordCount Application
Create a stream WordCount application using the designed API. Input is an infinite stream of {word, count} pairs, and output is word count in 5-minute rolling windows, case-insensitive.

### 3. Intermediate Features

#### 3.1 Event-Time Window Processing
Support event-time window processing with limited out-of-order data using watermark mechanisms.

#### 3.2 Retract Processing
Extend the system to support retract processing for applications like counting distinct word counts.

#### 3.3 Automatic Fault Recovery
Implement fault tolerance with at-least once semantics, including:

1. Automatic node process restart
2. Rebuilding operator connections
3. State recovery

### 4. Advanced Features

#### 4.1 Exactly Once Processing Semantics
Demonstrate exactly once semantics with a designed demo.