



ProducerIdManager类负责生成 自增号 producer_id，它每次会从zookeeper批量的拉取 id 号，提高了效率。数据保存在zookeeper的 节点 /latest_producer_id_block，以json的格式保存。





TransactionStateManager 管理着 分区的事务信息



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



transactional.id ，是transaction客户端的唯一 id 号，有客户端自己指定。当这个客户端重启后，仍然可以根据 transactional.id 来继续处理失败的事务。



loadTransactionsForTxnTopicPartition 方法负责从 __transaction_state里，加载事务数据。



每个 producer_id 都有一个 producerEpoch。producerEpoch是有范围的，当producerEpoch逐渐递增，超过Short.Max的时候，那么就会生成新的producer_id。



请求

```
FindCoordinatorHandler


InitProducerIdHandler


AddPartitionsToTxnHandler
AddOffsetsToTxnHandler


TxnOffsetCommitHandler


EndTxnHandler
```





Kafka 客户端的 TransactionManager的状态

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



TransactionManager 实例化的时候，状态为UNINITIALIZED。

当发送请求获取produce_id 时，状态变为INITIALIZING。

当成功获取到producer_id的响应时，状态变为READY。

当producer调用了beginTransaction方法时，状态变为IN_TRANSACTION。

之后producer进行一系列的操作，涉及到发送AddPartitionsToTxnRequest请求，和AddOffsetsToTxnRequest请求。

producer最后会发送EndTxnRequest请求，提交本次事务。状态变为COMMITTING_TRANSACTION。

当收到提交事务的响应，状态变为READY。

有可能producer最后需要事务回滚，当它发送EndTxnRequest时，状态变为ABORTING_TRANSACTION。接收到响应后，状态变为READY。



当请求过程出错时，状态变为ABORTABLE_ERROR或FATAL_ERROR。



常用例子：

```java
KafkaProducer producer = createKafkaProducer(
  “bootstrap.servers”, “localhost:9092”,
  “transactional.id”, “my-transactional-id”);

producer.initTransactions();

KafkaConsumer consumer = createKafkaConsumer(
  “bootstrap.servers”, “localhost:9092”,
  “group.id”, “my-group-id”,
  "isolation.level", "read_committed");

consumer.subscribe(singleton(“inputTopic”));

while (true) {
  ConsumerRecords records = consumer.poll(Long.MAX_VALUE);
  producer.beginTransaction();
  for (ConsumerRecord record : records)
    producer.send(producerRecord(“outputTopic”, record));
  producer.sendOffsetsToTransaction(currentOffsets(consumer), group);  
  producer.commitTransaction();
}
```





发送EndTxnRequest请求前，必须保证所有请求都已经发送完毕。





参考资料： <https://www.confluent.io/blog/transactions-apache-kafka/>

<https://docs.google.com/document/d/11Jqy_GjUGtdXJK94XGsEIK7CP1SnQGdp2eF0wSw9ra8/edit#>

<https://docs.google.com/document/d/1Rlqizmk7QCDe8qAnVW5e5X8rGvn6m2DCR3JR2yqwVjc/edit>