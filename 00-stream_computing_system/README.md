# Stream Computing System

A simple stream computing system that supports basic stream processing operations with Kafka integration.

## Features

- Basic stream processing operations (map, keyBy, reduce)
- Support for processing-time windows
- Kafka source integration
- File sink support
- Parallel processing capabilities

## Prerequisites

- Java 11 or later
- Maven 3.6 or later
- Docker and Docker Compose (for running Kafka)

## Getting Started

1. Start Kafka using Docker Compose:
```bash
docker-compose up -d
```

2. Create the input topic:
```bash
docker exec -it stream_computing_system-kafka-1 kafka-topics.sh --create \
    --topic word-count-input \
    --bootstrap-server localhost:9092 \
    --partitions 2 \
    --replication-factor 1
```

3. Build the project:
```bash
mvn clean package
```

4. Run the WordCount example:
```bash
java -cp target/stream-computing-system-1.0-SNAPSHOT.jar \
    com.streamcomputing.examples.WordCount
```

5. In another terminal, send test data to Kafka:
```bash
docker exec -it stream_computing_system-kafka-1 kafka-console-producer.sh \
    --topic word-count-input \
    --bootstrap-server localhost:9092
```

Then enter test data in the format `word,count`:
```
APPLE,1
pie,1
apple,1
```

6. Check the output in `word-counts.txt`. You should see output like:
```
2024-02-15T12:00:00,apple,2
2024-02-15T12:00:00,pie,1
```

## Architecture

The system consists of several key components:

1. Core API:
   - `DataStream`: Interface for stream operations
   - `KeyedDataStream`: Interface for keyed operations
   - `WindowedStream`: Interface for windowed operations

2. Operators:
   - `KafkaSource`: Reads data from Kafka topics
   - `FileSink`: Writes data to files

3. Runtime:
   - `BasicDataStream`: Basic stream implementation
   - `KafkaDataStream`: Kafka-specific stream implementation
   - `BasicKeyedDataStream`: Implementation for keyed operations
   - `BasicWindowedStream`: Implementation for windowed operations

## Example Applications

### WordCount

The WordCount example demonstrates:
- Reading from Kafka
- Parsing input data
- Converting to lowercase
- Grouping by word
- Counting in 5-minute windows
- Writing results to a file

## Contributing

1. Fork the repository
2. Create a feature branch
3. Commit your changes
4. Push to the branch
5. Create a Pull Request

## License

This project is licensed under the MIT License. 