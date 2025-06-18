# 流计算系统学习笔记

## 📚 项目概述

这是一个流计算系统的学习笔记仓库，包含了对 Kafka、Flink 以及流式计算概念的学习和实践。**注意：这不是一个完整的流计算系统实现，而是学习过程中的代码实践和笔记整理。**

## 📁 项目结构

```
stream_computing_system/
├── 00-stream_computing_system/    # 简化版流计算系统实现（学习用）
├── 01-kafka/                      # Kafka 学习笔记和示例
├── 02-flink/                      # Flink 学习笔记和示例  
├── 03-kafka&flink/               # Kafka + Flink 集成示例
├── media/                        # 图片和媒体文件
└── instructions.md               # Git 子模块管理说明
```

## 🚀 学习内容

### 1. 简化版流计算系统 (`00-stream_computing_system/`)

这是一个基于 Java 的简化版流计算系统实现，主要用于理解流计算的基本概念：

**已实现的功能：**
- ✅ 基础 DataStream API 设计
- ✅ Kafka Source 连接器
- ✅ Map、KeyBy、Reduce 基础算子
- ✅ 简单的文件 Sink
- ✅ WordCount 示例应用

**技术栈：**
- Java 17
- Apache Kafka 3.6.1
- Maven 构建
- Docker Compose 部署

**运行示例：**
```bash
cd 00-stream_computing_system
./test.sh
```

### 2. Kafka 学习 (`01-kafka/`)

包含 Kafka 的学习笔记和实践示例：

- Kafka 基础概念（Producer、Consumer、Topic、Partition）
- 单机模式和集群模式部署
- 使用 Docker 快速搭建 Kafka 环境
- 生产者和消费者示例代码

### 3. Flink 学习 (`02-flink/`)

包含 Apache Flink 的学习笔记：

- Flink 架构和核心概念
- Session Mode 和 Application Mode 部署
- SocketWindowWordCount 示例
- Docker 容器化部署

### 4. Kafka + Flink 集成 (`03-kafka&flink/`)

两个版本的 Kafka-Flink 连接器示例：

- **version1/**: Apache 官方连接器示例
- **version2/**: lydtechconsulting 社区版本示例


## 🔄 Git 子模块

本项目使用 Git 子模块管理外部依赖：

```bash
# 初始化子模块
git submodule update --init --recursive

# 更新子模块
git submodule update --remote
```

## ⚠️ 免责声明

- 这是一个**学习项目**，不适用于生产环境
- 代码实现以教学为目的，未考虑完整的容错和性能优化
- 部分功能可能不完整或存在简化

## 📚 参考资料

1. [MapReduce-百度百科](https://baike.baidu.com/item/MapReduce/133425)
2. [Streaming 101: The world beyond batch](https://www.oreilly.com/radar/the-world-beyond-batch-streaming-101/)
3. [Apache Flink: Stateful Computations over Data Streams](https://flink.apache.org/)
4. [Apache Storm](https://storm.apache.org/)
5. [Apache Kafka Documentation](https://kafka.apache.org/documentation/)