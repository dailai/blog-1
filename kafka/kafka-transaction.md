



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



