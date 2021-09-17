# Flink SQL Connector 开发示例

> 开发 Flink 官方未提供的 sql-connector，其中 MongoDB Connector 参考 **[Ververica](https://developer.aliyun.com/article/724113)** ，Redis Connector 参考 **[bahir-flink](https://github.com/apache/bahir-flink)** ，由于此 Connector 实现了弃用的接口，故做了重新实现。

## 参考

* **[bahir-flink](https://github.com/apache/bahir-flink)** 上维护了很多 Flink 官方没有的 Connector，如果需要自定义连接器开发，可以先参考此代码库。
* **[Ververica](https://developer.aliyun.com/article/724113)** 作为阿里云 Flink 企业版，也维护了大量的 Connector，可以通过查看 **[Ververica-Connector](https://mvnrepository.com/search?q=Ververica)** 的 maven 仓库，获取相应的 Connector。不过，此 Connector 会有一些自定义日志采集、运行 Metrics 采集等相关逻辑，需自行更改。


