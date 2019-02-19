# Spark Streaming Kafka 原理 #

Spark Streaming支持Kafka数据源，Kafka作为一个消息队列，具有很高的吞吐量，和Spark Streaming结合起来，可以实现高速实时的处理数据。



以前的版本支持两种方式读取Kafka，一种是通过 receiver 读取的方式，另一种是直接读取的方式。基于 receiver 方式的读取，不太稳定，已经被最新版遗弃了，所以下面只讲直接读取的方式。



首先介绍下DirectKafkaInputDStream类，它表示Kafka数据流。它会生成分片数据的RDD。

它每次生成RDD的时候，都会从Kafka中获取该topic的所有分区的最新offset，生成RDD包括上次提交的offset一直到最新的offset。



DirectKafkaInputDStream还支持限速









KafkaRDD



KafkaRDD为compute生成的RDD，将Kafka的分区数目切断，



首先看KafkaRDD是如何划分分区的

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

```
private[spark] class KafkaRDD[K, V] {
  override def compute(thePart: Partition, context: TaskContext): Iterator[ConsumerRecord[K, V]] =   {
    // 向下转型
    val part = thePart.asInstanceOf[KafkaRDDPartition]
    if (part.fromOffset == part.untilOffset) {
      Iterator.empty
    } else {
      new KafkaRDDIterator(part, context)
    }
  }
  
  private class KafkaRDDIterator(
      part: KafkaRDDPartition,
      context: TaskContext) extends Iterator[ConsumerRecord[K, V]] {

    logInfo(s"Computing topic ${part.topic}, partition ${part.partition} " +
      s"offsets ${part.fromOffset} -> ${part.untilOffset}")

    val groupId = kafkaParams.get(ConsumerConfig.GROUP_ID_CONFIG).asInstanceOf[String]

    context.addTaskCompletionListener{ context => closeIfNeeded() }

    val consumer = if (useConsumerCache) {
      CachedKafkaConsumer.init(cacheInitialCapacity, cacheMaxCapacity, cacheLoadFactor)
      if (context.attemptNumber >= 1) {
        // just in case the prior attempt failures were cache related
        CachedKafkaConsumer.remove(groupId, part.topic, part.partition)
      }
      CachedKafkaConsumer.get[K, V](groupId, part.topic, part.partition, kafkaParams)
    } else {
      CachedKafkaConsumer.getUncached[K, V](groupId, part.topic, part.partition, kafkaParams)
    }

    var requestOffset = part.fromOffset

    def closeIfNeeded(): Unit = {
      if (!useConsumerCache && consumer != null) {
        consumer.close
      }
    }

    override def hasNext(): Boolean = requestOffset < part.untilOffset

    override def next(): ConsumerRecord[K, V] = {
      assert(hasNext(), "Can't call getNext() once untilOffset has been reached")
      val r = consumer.get(requestOffset, pollTimeout)
      requestOffset += 1
      r
    }
  }  
}


```

