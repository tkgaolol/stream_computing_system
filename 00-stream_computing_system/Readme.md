# 简化流计算系统设计

### 基本功能


设计并实现流处理 API 来创建应用程序。定义无界数据流（DataStream）的抽象数据结构和支持以下操作的 API：

五个基本操作符：
- Source：Kafka 到 DataStream
- Sink：DataStream 到外部文件
- Map：DataStream 到 DataStream
- KeyBy：DataStream 到分区 DataStream
- Reduce：分区 DataStream 到 DataStream
注：分区 DataStream 是指使用 KeyBy 操作符根据键分成组的 DataStream。每个分区包含具有相同键值的元素。


流式 WordCount 应用程序<br>
输入是 {word, count} 对的无限流，输出是 5 分钟滚动窗口中的字数，不区分大小写。
