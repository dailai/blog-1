





## 客户端



### 请求类型

```
FindCoordinatorHandler


InitProducerIdHandler


AddPartitionsToTxnHandler
AddOffsetsToTxnHandler


TxnOffsetCommitHandler


EndTxnHandler
```







### 事务状态

TransactionStateManager 管理着 分区的事务信息，当事务的信息发生变化时，会先保存到 名称为 __transaction_state 的 topic 里，然后更新缓存。







TransactionCoordinator  负责定期检查超时事务。



TransactionMetadata 是每个事务对应着一个元数据

```scala
class TransactionMetadata(val transactionalId: String,
                                               var producerId: Long,
                                               var producerEpoch: Short,
                                               var txnTimeoutMs: Int,
                                               var state: TransactionState,
                                               val topicPartitions: mutable.Set[TopicPartition],
                                               @volatile var txnStartTimestamp: Long = -1,
                                               @volatile var txnLastUpdateTimestamp: Long) 
```



TransactionState分为下面几种 Empty， Ongoing， PrepareCommit， PrepareAbort， CompleteCommit， CompleteAbort， Dead。







TransactionCoordinator 的 handleEndTransaction 处理 commit 或者 abort 请求，









ProducerIdManager类负责生成 自增号 producer_id，它每次会从zookeeper批量的拉取 id 号，提高了效率。数据保存在zookeeper的 节点 /latest_producer_id_block，以json的格式保存。

transactional.id ，是transaction客户端的唯一 id 号，有客户端自己指定。当这个客户端重启后，仍然可以根据 transactional.id 来继续处理失败的事务。

loadTransactionsForTxnTopicPartition 方法负责从 __transaction_state里，加载事务数据。



每个 producer_id 都有一个 producerEpoch。producerEpoch是有范围的，当producerEpoch逐渐递增，超过Short.Max的时候，那么就会生成新的producer_id。









处理申请 producer id 请求，如果该 transaction id 之前不存在，那么会新建元数据，并且保存到 topic 里。并将producer epoch 增大。



处理 add partition 请求，会更新元数据的状态，并且保存在 topic 里。

保存到 topic 的请求，ack设为 -1， 表示必须保存到所有备份中。



处理 end transaction 请求后，会先持久化到 topic 里。然后将请求发送给该事物涉及到的那些partition 的 leader角色。



发送参与者的请求，会以节点分组，保存在各自的队列里。



后台线程会将队列的请求发送出去



请求正常完成后，会将此事务的元数据中删除对应的分区。并将 TxnLogAppend 写入 topic。





## 事务流程

Kafka的整个事务处理流程如下图：



上图中的 Transaction Coordinator 运行在 Kafka 服务端，下面简称 TC 服务。

__transaction_state 是 TC 服务持久化事务信息的 topic 名称，下面简称事务 topic。

Producer 向 TC 服务发送的 commit 消息，下面简称事务提交消息。

TC 服务向分区发送的消息，下面简称事务结果消息。

### 事务初始化

Producer 在使用事务功能，必须先自定义一个唯一的 transaction id。通过这个 transaction id，即使客户端挂掉了，也能保证重启后，继续处理未完成的事务。

Kafka 实现事务需要依靠幂等性，而幂等性需要指定 producer id 。所以Producer在启动事务之前，需要向 TC 服务申请 producer id。TC 服务在分配 producer id 后，会将信息持久化到事务 topic。

### 发送消息

Producer 在收到 producer id 后，就可以发送消息到对应的 topic。不过发送消息之前，需要先将这些消息发送的分区地址，上传到 TC 服务。TC 服务将这些分区地址持久化到事务 topic。然后 Producer 才会真正的发送消息，这些消息与普通消息不同，它们会有一个字段，表示自身是事务消息。

### 发送提交请求

Producer 发送完消息后，认为该事务可以提交了，就会发送提交请求到 TC 服务。Producer 的工作就完成了，接下来它只需要等待响应。

### 提交请求持久化

TC 服务收到提交请求后，会先将提交信息先持久化到 事务 topic 。持久化成功后，服务端就立即发送成功响应给 Producer。然后找到该事务涉及到的所有分区，为每个分区生成提交请求，存到队列里，等待发送。

读者可能有所疑问，在一般的二阶段提交中，协调者需要收到所有参与者的响应后，才能判断此事务是否成功，最后才将结果返回给客户。那如果 TC 服务在发送响应给 Producer 后，还没来及向分区发送请求就挂掉了，那么 Kafka 是如何保证事务完成。因为每次事务的信息都会持久化，所以 TC 服务挂掉重新启动后，会先从 事务 topic 加载事务信息，如果发现只有事务提交信息，却没有后来的事务完成信息，说明存在事务结果信息没有提交到分区。

### 发送事务结果信息给分区

后台线程会不停的从队列里，拉取请求并且发送到分区。当一个分区收到事务结果消息后，会将结果保存到分区里，并且返回成功响应到 TC服务。当 TC 服务收到所有分区的成功响应后，会持久化一条事务完成的消息到事务 topic。至此，一个完整的事务流程就完成了。





## 客户端原理



### 使用示例

下面代码实现，消费者读取消息，并且发送到多个分区的事务

```java
// 创建 Producer 实例，并且指定 transaction id
KafkaProducer producer = createKafkaProducer(
  “bootstrap.servers”, “localhost:9092”,
  “transactional.id”, “my-transactional-id”);

// 初始化事务，这里会向 TC 服务申请 producer id
producer.initTransactions();

// 创建 Consumer 实例，并且订阅 topic
KafkaConsumer consumer = createKafkaConsumer(
  “bootstrap.servers”, “localhost:9092”,
  “group.id”, “my-group-id”,
  "isolation.level", "read_committed");
consumer.subscribe(singleton(“inputTopic”));

while (true) {
  ConsumerRecords records = consumer.poll(Long.MAX_VALUE);
  // 开始新的事务
  producer.beginTransaction();
  for (ConsumerRecord record : records) {
    // 发送消息到分区
    producer.send(producerRecord(“outputTopic_1”, record));
    producer.send(producerRecord(“outputTopic_2”, record));
  }
  // 提交 offset
  producer.sendOffsetsToTransaction(currentOffsets(consumer), group);  
  // 提交事务
  producer.commitTransaction();
}
```



### 运行原理

 TransactionManager 类负责 Producer 与 TC 服务通信，并且它在请求之后，会变化自身的状态。

### Kafka 客户端的 TransactionManager的状态

```java
private enum State {
    UNINITIALIZED,
    INITIALIZING,
    READY,
    IN_TRANSACTION,
    COMMITTING_TRANSACTION,
    ABORTING_TRANSACTION,
    ABORTABLE_ERROR,
    FATAL_ERROR;
}
```



### 发送请求流程

TransactionManager 实例化的时候，状态为UNINITIALIZED。

当发送请求获取produce_id 时，状态变为INITIALIZING。

当成功获取到producer_id的响应时，状态变为READY。

当producer调用了beginTransaction方法时，状态变为IN_TRANSACTION。

之后producer进行一系列的操作，涉及到发送AddPartitionsToTxnRequest请求，和AddOffsetsToTxnRequest请求。

发送完AddPartitionsToTxnRequest之后，  producer才发送消息到分区里

producer最后会发送EndTxnRequest请求，提交本次事务。状态变为COMMITTING_TRANSACTION。

当收到提交事务的响应，状态变为READY。

有可能producer最后需要事务回滚，当它发送EndTxnRequest时，状态变为ABORTING_TRANSACTION。接收到响应后，状态变为READY。



当请求过程出错时，状态变为ABORTABLE_ERROR或FATAL_ERROR。



发送EndTxnRequest请求前，必须保证所有请求都已经发送完毕。







## 服务端原理



这里要额外说明下，Kafka 的事务消息还包括提交consumer的消费位置。

发送consumer 消费位置的提交消息，本质也是发送消息到 __consumer_offset topic。



 后，会去更新缓存的分区列表，从中把自己删除掉。当这个分区列表为空时，则表示所有的分区都已经成功响应了，那么就会持久化一条事务完成的消息到__consumer_offset topic。