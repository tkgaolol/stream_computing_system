# Kafka & Flink 学习笔记

## 1. 架构

## 2. 咋用
### 2.1 requirements
- java 17
- maven 3.8.6
- docker
- docker-compose

### 2.2 单机模式(SocketWindowWordCount 需要三个terminal)
- 第一步 进入flink目录
```bash
wget https://dlcdn.apache.org/flink/flink-1.20.0/flink-1.20.0-bin-scala_2.12.tgz --no-check-certificate

tar -xzf flink-1.20.0-bin-scala_2.12.tgz
cd flink-1.20.0
```

- 第二步 启动集群
```bash
./bin/start-cluster.sh
```
我们启动了2个进程：JobManager的JVM 和 TaskManager的JVM。JobManager正在为Web界面提供可访问的Web界面： Localhost：8081 。

- 第三步 打开浏览器访问ip:8081

- 第四步 开启端口
```bash
nc -lk 9999
```

- 第五步 提交作业（job）
```bash
./bin/flink run examples/streaming/SocketWindowWordCount.jar --port 9999
```

- 第六步 发送信息并查看输出
```bash
tail -f log/flink-*-taskexecutor-*.out
```
- 第七步 停止集群
```bash
./bin/stop-cluster.sh
```

- 删除所有信息
```bash
rm log/*
```

### 2.3 docker集群 session_mode(SocketWindowWordCount 需要三个terminal)
- 第一步 在background启动集群
``` bash
cd 02-flink/application_mode
# or
cd 02-flink/session_mode

docker compose up

# Scale the cluster up or down to N TaskManagers
docker compose scale taskmanager=<N>
```

- 第二步 访问JobManager容器 并发送信息
```bash
docker exec -it $(docker ps --filter name=jobmanager --format={{.ID}}) /bin/sh
apt update && apt install netcat -y
nc -lk 9999
```

- 第三步 提交作业
```bash
docker exec -it $(docker ps --filter name=jobmanager --format={{.ID}}) /bin/sh
./bin/flink run examples/streaming/SocketWindowWordCount.jar --hostname jobmanager --port 9999
```

- 第四步 终结集群
```bash
docker compose down
```

- 可以通过localhost:8081 访问 web ui

### 2.4 docker集群 application_mode
- 第一步 进入application_mode目录
```bash
cd 02-flink/application_mode
FLINK_PROPERTIES="jobmanager.rpc.address: jobmanager"
```

- 第二步 创建网络
```bash
docker network create flink-network
```

- 第三步 启动监听端口并发送信息
```bash
nc -lk 9999
```

- 第四步 提交作业
```bash
docker compose up --build
```

- 第五步 终结集群并删除网络
```bash
docker compose down -v
docker network prune
```

## 参考网站
- https://github.com/apache/flink-connector-kafka/tree/main
- https://github.com/lydtechconsulting/flink-kafka-connector
- https://nightlies.apache.org/flink/flink-docs-master/zh/docs/connectors/datastream/kafka/
