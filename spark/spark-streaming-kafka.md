# Spark Streaming Kafka 原理 #

Spark Streaming支持Kafka数据源，Kafka作为一个消息队列，具有很高的吞吐量，和Spark Streaming结合起来，可以实现高速实时的处理数据。



以前的版本支持两种方式读取Kafka，一种是通过 receiver 读取的方式，另一种是直接读取的方式。基于 receiver 方式的读取，不太稳定，已经被最新版遗弃了，所以下面只讲直接读取的方式。



首先介绍下DirectKafkaInputDStream类，它表示Kafka数据流。它会生成分片数据的RDD。

它每次生成RDD的时候，都会从Kafka中获取该topic的所有分区的最新offset，生成RDD包括上次提交的offset一直到最新的offset。



DirectKafkaInputDStream还支持限速









KafkaRDD



KafkaRDD为compute生成的RDD，将Kafka的分区数目切断，



首先看KafkaRDD是如何划分分区的，

```scala
private[spark] class KafkaRDD[K, V](
    val offsetRanges: Array[OffsetRange] 
) extends RDD[ConsumerRecord[K, V]](sc, Nil) with Logging with HasOffsetRanges {

  override def getPartitions: Array[Partition] = {
    offsetRanges.zipWithIndex.map { case (o, i) =>
        new KafkaRDDPartition(i, o.topic, o.partition, o.fromOffset, o.untilOffset)
    }.toArray
  }
}
```



KafkaRDD的分区，对应着Kafka的一个topic partition 的一段数据，由KafkaRDDPartition表示。

每个KafkaRDDPartition对应这一个OffsetRange， OffsetRange保存了数据的位置。

```scala
final class OffsetRange private(
    val topic: String,   // Kafka的topic名称
    val partition: Int,  // 该topic的partition
    val fromOffset: Long,  // 起始offset
    val untilOffset: Long);  // 截至offset
```

接下来看看KafkaRDD是如何读取分区数据的

```scala
private[spark] class KafkaRDD[K, V] {
  override def compute(thePart: Partition, context: TaskContext): Iterator[ConsumerRecord[K, V]] =   {
    // 向下转型为KafkaRDDPartition
    val part = thePart.asInstanceOf[KafkaRDDPartition]
    if (part.fromOffset == part.untilOffset) {
      Iterator.empty
    } else {
      // 返回迭代器
      new KafkaRDDIterator(part, context)
    }
  }
  
  private class KafkaRDDIterator(
      part: KafkaRDDPartition,
      context: TaskContext) extends Iterator[ConsumerRecord[K, V]] {

    logInfo(s"Computing topic ${part.topic}, partition ${part.partition} " +
      s"offsets ${part.fromOffset} -> ${part.untilOffset}")

    // 获取kafka消费者的groupId
    val groupId = kafkaParams.get(ConsumerConfig.GROUP_ID_CONFIG).asInstanceOf[String]
    // 当整个spark streaming任务退出时，会调用closeIfNeeded方法关闭 kafka消费者
    context.addTaskCompletionListener{ context => closeIfNeeded() }
    // 获取 kafka消费者
    val consumer = if (useConsumerCache) {
      // 如果支持 kafka消费者缓存，那么实例化CachedKafkaConsumer
      CachedKafkaConsumer.init(cacheInitialCapacity, cacheMaxCapacity, cacheLoadFactor)
      if (context.attemptNumber >= 1) {
        // 如果此次任务失败过，那么删除以前的 kafka 消费者
        CachedKafkaConsumer.remove(groupId, part.topic, part.partition)
      }
      // 返回对应groupId，topic和partition的kafka消费者，如果有缓存则返回缓存的。
      CachedKafkaConsumer.get[K, V](groupId, part.topic, part.partition, kafkaParams)
    } else {
      // 新建对应groupId，topic和partition的kafka消费者
      CachedKafkaConsumer.getUncached[K, V](groupId, part.topic, part.partition, kafkaParams)
    }

    

    // 关闭kafka消费者
    def closeIfNeeded(): Unit = {
      if (!useConsumerCache && consumer != null) {
        consumer.close
      }
    }
    
    // 只返回offset大于fromOffset，小于untilOffset的数据
    var requestOffset = part.fromOffset
    override def hasNext(): Boolean = requestOffset < part.untilOffset

    override def next(): ConsumerRecord[K, V] = {
      assert(hasNext(), "Can't call getNext() once untilOffset has been reached")
      // 调用CachedKafkaConsumer的get方法返回一条数据
      val r = consumer.get(requestOffset, pollTimeout)
      requestOffset += 1
      r
    }
  }  
}
```

接下来看看CachedKafkaConsumer是如何读取kafka数据的

```scala
class CachedKafkaConsumer[K, V] private(
  val groupId: String,
  val topic: String,
  val partition: Int,
  val kafkaParams: ju.Map[String, Object]) extends Logging {

  val topicPartition = new TopicPartition(topic, partition)
  // 实例化 KafkaConsumer，使用assign模式。这种模式需要自己维护offset
  protected val consumer = {
    val c = new KafkaConsumer[K, V](kafkaParams)
    val tps = new ju.ArrayList[TopicPartition]()
    tps.add(topicPartition)
    c.assign(tps)
    c
  }

  // 从kafka一次读取的数据是多条的，这里用buffer缓存读取的数据
  protected var buffer = ju.Collections.emptyList[ConsumerRecord[K, V]]().iterator
  // 下一条数据的offset
  protected var nextOffset = -2L

  def close(): Unit = consumer.close()

  // 获取offset对应的数据
  def get(offset: Long, timeout: Long): ConsumerRecord[K, V] = {
    logDebug(s"Get $groupId $topic $partition nextOffset $nextOffset requested $offset")
    // 如果要获取数据的offset不等于下一条数据的offset，则调用seek移动KafKaConsumer的位置
    if (offset != nextOffset) {
      logInfo(s"Initial fetch for $groupId $topic $partition $offset")
      seek(offset)
      // 从kafka获取数据
      poll(timeout)
    }
    // 如果缓存的数据，已经读完，则调用poll从kafka中读取数据
    if (!buffer.hasNext()) { poll(timeout) }
    assert(buffer.hasNext(),
      s"Failed to get records for $groupId $topic $partition $offset after polling for $timeout")
    // 获取buffer的数据
    var record = buffer.next()

    if (record.offset != offset) {
      // 如果从buffer中获取的数据有问题，则需要重新从Kafka中读取数据
      logInfo(s"Buffer miss for $groupId $topic $partition $offset")
      seek(offset)
      poll(timeout)
      assert(buffer.hasNext(),
        s"Failed to get records for $groupId $topic $partition $offset after polling for $timeout")
      // 从buffer中读取数据
      record = buffer.next()
      // 检测该数据的offset是否等于预期
      assert(record.offset == offset,
        s"Got wrong record for $groupId $topic $partition even after seeking to offset $offset")
    }
    // 更新nextOffset为当前offset+1
    nextOffset = offset + 1
    record
  }

  private def seek(offset: Long): Unit = {
    // 调用KafKaConsumer的seek方法移动读取位置
    logDebug(s"Seeking to $topicPartition $offset")
    consumer.seek(topicPartition, offset)
  }

  private def poll(timeout: Long): Unit = {
    // 调用poll方法读取数据
    val p = consumer.poll(timeout)
    // 获取该topic partition的数据
    val r = p.records(topicPartition)
    logDebug(s"Polled ${p.partitions()}  ${r.size}")
    buffer = r.iterator
  }

}
```



从上面的代码可以看到，CachedKafkaConsumer会按照offset顺序的读取数据，并且offset还必须是连续的。如果Kafka开启了日志压缩功能，就会将相同key的数据压缩成一条，那么这样消息的offset就不会是连续的。这种情况下，spark streaming就会报错。上面的代码是spark 2.2版本的，后面的版本修复了这个问题，参见 pull request。