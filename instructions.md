# 小notes

## 建立子模块
```bash
git submodule add https://github.com/apache/kafka.git 01-kafka/kafka
git submodule add https://github.com/apache/flink.git 02-flink/flink
```

## 初始化子模块
```bash
git submodule update --init --recursive
```

## 切换java版本
```bash
sudo update-alternatives --config javac
```
