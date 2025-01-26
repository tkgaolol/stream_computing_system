# Kafka 学习笔记

## 1. 什么是 Kafka

Kafka 是一个分布式流处理平台，用于构建实时数据管道和流应用。它提供了高吞吐量、低延迟、高可靠性的消息传递系统，适用于大规模数据处理和实时数据流处理。

Kafka 的核心组件包括：

- **Producer**：生产者，用于发送消息到 Kafka 集群。
- **Consumer**：消费者，用于从 Kafka 集群中消费消息。
- **Broker**：Kafka 集群中的一个节点，负责存储和处理消息。
- **Topic**：消息的逻辑容器，每个主题可以有多个分区。
- **Partition**：主题的分区，用于实现消息的并行处理。
- **Offset**：消息在分区中的唯一标识，用于记录消息的消费位置。

## 2. Kafka 咋用

- 第一步 start broker server
```bash
# Generate a Cluster UUID
KAFKA_CLUSTER_ID="$(bin/kafka-storage.sh random-uuid)"

# Format Log Directories
bin/kafka-storage.sh format --standalone -t $KAFKA_CLUSTER_ID -c config/kraft/reconfig-server.properties

# Start the Kafka Server
bin/kafka-server-start.sh config/kraft/reconfig-server.properties
```
- 第二步 创建topic
```bash
bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --topic streams-pipe-output --partitions 1 --replication-factor 1
```
- 中间可以加一个管道（如果有的话，即producer和consumer之间的pipe）
```bash
mvn clean package
mvn exec:java -Dexec.mainClass=myapps.LineSplit
```
- 第三步 创建producer 和 consumer
```bash
bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic streams-plaintext-input

bin/kafka-console-consumer.sh --topic quickstart-events --from-beginning --bootstrap-server localhost:9092

bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 \
    --topic streams-wordcount-output \
    --from-beginning \
    --property print.key=true \
    --property print.value=true \
    --property key.deserializer=org.apache.kafka.common.serialization.StringDeserializer \
    --property value.deserializer=org.apache.kafka.common.serialization.LongDeserializer
```
- 第四步 发送消息
- 查询topic
```bash
bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe

bin/kafka-topics.sh --bootstrap-server localhost:9092 --list
```
- 删除所有信息
```bash
rm -rf /tmp/kafka-logs /tmp/zookeeper /tmp/kraft-combined-logs
```



 


# 运行项目(docker)
docker pull ubuntu:latest
docker run -it ubuntu:latest /bin/bash

# 更新apt
apt update
apt upgrade

apt install wget
apt install vim
apt-get install openjdk-8-jdk
apt install maven

wget https://dlcdn.apache.org/kafka/3.9.0/kafka_2.13-3.9.0.tgz
tar -xzf kafka_2.13-3.9.0.tgz

mvn archetype:generate \
-DarchetypeGroupId=org.apache.kafka \
-DarchetypeArtifactId=streams-quickstart-java \
-DarchetypeVersion=3.9.0 \
-DgroupId=streams.examples \
-DartifactId=streams-quickstart \
-Dversion=0.1 \
-Dpackage=myapps