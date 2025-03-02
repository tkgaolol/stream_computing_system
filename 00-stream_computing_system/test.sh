#!/bin/bash

# Build the project
echo "Building the project..."
mvn clean package

# Start Kafka and Zookeeper
echo "Starting Kafka and Zookeeper..."
docker compose up -d

# Wait for services to be ready
echo "Waiting for services to be ready..."
sleep 10

# Create Kafka topic
echo "Creating Kafka topic..."
docker exec kafka kafka-topics \
    --create \
    --topic word-count-input \
    --bootstrap-server kafka:9093 \
    --replication-factor 1 \
    --partitions 1

# Run the WordCount example in background
echo "Running WordCount example..."
java -jar target/stream-computing-system-1.0-SNAPSHOT-jar-with-dependencies.jar &
WORD_COUNT_PID=$!

# Wait for the application to start
sleep 5

# Send test data
echo "Sending test data..."
echo "Sending words to Kafka..."
docker exec -i kafka kafka-console-producer --broker-list kafka:9093 --topic word-count-input << EOF
hello
world
hello
stream
processing
world
EOF

# Wait for processing
sleep 5

# Check the output
echo "Checking output..."
if [ -f word-count-output.txt ]; then
    echo "Output file content:"
    cat word-count-output.txt
else
    echo "Error: Output file not found!"
fi

# Cleanup
echo "Cleaning up..."
kill $WORD_COUNT_PID
docker compose down

echo "Test completed!" 