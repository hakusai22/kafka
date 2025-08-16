一、KafkaProducer 发送消息的整体流程
    1. 拦截器链处理（ProducerInterceptors）
    2. 序列化（Serializer）
    3. 分区选择（Partitioner）
    4. 消息累加与批处理（RecordAccumulator）
    5. 异步发送（Sender 线程）
    6. 网络通信（NetworkClient & Selector）
    7. Broker 响应处理


## 二、各组件详细解读
### 2.1 ProducerInterceptors（拦截器链）
https://github.com/hakusai22/kafka/blob/ae771d73d119b5de94a6862422ad6bf3dcce292e/clients/src/main/java/org/apache/kafka/clients/producer/internals/ProducerInterceptors.java

https://github.com/hakusai22/kafka/blob/b5606e7de0bb85f2c92cbbae090fa7869bd38ea7/clients/src/main/java/org/apache/kafka/clients/producer/KafkaProducer.java

- **作用**：允许用户在消息发送前后进行自定义处理，比如埋点、数据预处理、监控等
- **流程**：每条消息在序列化前会依次经过所有拦截器的 onSend() 方法，发送完成后会回调 onAcknowledgement()

### 2.2 Serializer（序列化器）

- **作用**：将用户自定义的 key 和 value 对象转换为字节数组，便于网络传输
- **常见实现**：StringSerializer、ByteArraySerializer 等

### 2.3 Partitioner（分区器）

- **作用**：决定消息应该被发送到哪个分区（partition）
- **策略**：可自定义，默认根据 key 的 hash 取模分区；也可指定 partition

### 2.4 RecordAccumulator（消息累加器）

- **作用**：将消息按 topic 和 partition 进行分组、批量缓存，提升吞吐量
- **结构**：每个 topic 下维护一个 ConcurrentMap<partition, Deque<ProducerBatch>>，每个分区有自己的批次队列
- **好处**：批量发送，减少网络请求次数，提高性能

### 2.5 Sender 线程（异步发送）

- **作用**：后台线程，负责不断从 RecordAccumulator 中取出已准备好的 batch，组装成 ProduceRequest 并发送到 Kafka Broker
- **流程**：
  1. 调用 RecordAccumulator.ready() 判断哪些分区有可发送的 batch
  2. 调用 RecordAccumulator.drain() 取出这些 batch
  3. 组装成 ProduceRequest，封装为 ClientRequest
  4. 通过 NetworkClient 发送

### 2.6 NetworkClient & Selector（网络通信）

- **NetworkClient**：Kafka 客户端的网络通信核心，负责管理与 Broker 的连接、请求发送与响应接收
- **Selector**：底层 NIO 实现，负责实际的 socket 读写、事件轮询

### 2.7 Kafka Broker 响应处理

- **流程**：Selector 读取到 Broker 的响应后，NetworkClient 负责分发响应，Sender 线程根据响应结果回调用户的 Callback 或处理异常

