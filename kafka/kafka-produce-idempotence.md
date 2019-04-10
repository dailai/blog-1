# Kafka Producer 幂等性原理 #



## 发送消息 ##

发送消息，都会附加一个序列号



KafkaProducer对于单个分区的发送，一次性最多发送5条。

这个在KafkaProducer初始化的时候，就会检测该配置项。

如果要使用幂等性，需要将enable.idempotence配置项设置为true。



当开启了幂等性，KafkaProducer发送消息时，会额外设置producer_id 和 baseSequence 字段。producer_id是从Kafka服务端请求获取的，baseSequence是该消息批次的序列号，由TransactionManager负责生成。

当Sender从RecordAccumulator中拉取消息时，会为消息设置上面了两个字段的值。

```java
public final class RecordAccumulator {
    
    public Map<Integer, List<ProducerBatch>> drain(Cluster cluster, Set<Node> nodes, int maxSize, long now) {
        if (nodes.isEmpty())
            return Collections.emptyMap();

        Map<Integer, List<ProducerBatch>> batches = new HashMap<>();
        for (Node node : nodes) {
            int size = 0;
            List<PartitionInfo> parts = cluster.partitionsForNode(node.id());
            List<ProducerBatch> ready = new ArrayList<>();
            int start = drainIndex = drainIndex % parts.size();
            do {
                PartitionInfo part = parts.get(drainIndex);
                TopicPartition tp = new TopicPartition(part.topic(), part.partition());
                // 当max.in.flight.requests.per.connection配置项为 1 时，Sender发送消息的时候，会暂时关闭此分区的请求发送。当完成响应时，才会开放请求发送。
                if (!isMuted(tp, now)) {
                    Deque<ProducerBatch> deque = getDeque(tp);
                    if (deque != null) {
                        synchronized (deque) {
                            ProducerBatch first = deque.peekFirst();
                            if (first != null) {
                                boolean backoff = first.attempts() > 0 && first.waitedTimeMs(now) < retryBackoffMs;
                                // Only drain the batch if it is not during backoff period.
                                if (!backoff) {
                                    if (size + first.estimatedSizeInBytes() > maxSize && !ready.isEmpty()) {
                                        break;
                                    } else {
                                        ProducerIdAndEpoch producerIdAndEpoch = null;
                                        boolean isTransactional = false;
                                        if (transactionManager != null) {
                                            // 判断是否可以向这个分区发送请求
                                            if (!transactionManager.isSendToPartitionAllowed(tp))
                                                break;
                                            // 获取producer_id 和 epoch
                                            producerIdAndEpoch = transactionManager.producerIdAndEpoch();
                                            if (!producerIdAndEpoch.isValid())
                                                // we cannot send the batch until we have refreshed the producer id
                                                break;
                                            // 这里判断是否开启了事务
                                            isTransactional = transactionManager.isTransactional();
                                            // 如果这个消息batch，已经设置了序列号，并且此分区连接有问题， 那么需要跳过这个消息batch
                                            if (!first.hasSequence() && transactionManager.hasUnresolvedSequence(first.topicPartition))
                                                break;
                                            // 查看消息batch是否重试，如果是，则跳过
                                            int firstInFlightSequence = transactionManager.firstInFlightSequence(first.topicPartition);
                                            if (firstInFlightSequence != RecordBatch.NO_SEQUENCE && first.hasSequence()
                                                    && first.baseSequence() != firstInFlightSequence)

                                                break;
                                        }

                                        ProducerBatch batch = deque.pollFirst();
                                        // 为新的消息batch，设置对应的字段值
                                        if (producerIdAndEpoch != null && !batch.hasSequence()) {
                                            // 这里调用了transactionManager生成序列号
                                            batch.setProducerState(producerIdAndEpoch, transactionManager.sequenceNumber(batch.topicPartition), isTransactional);
                                            // 更新序列号
                                            transactionManager.incrementSequenceNumber(batch.topicPartition, batch.recordCount);

                                            transactionManager.addInFlightBatch(batch);
                                        }
                                        batch.close();
                                        size += batch.records().sizeInBytes();
                                        ready.add(batch);
                                        batch.drained(now);
                                    }
                                }
                            }
                        }
                    }
                }
                this.drainIndex = (this.drainIndex + 1) % parts.size();
            } while (start != drainIndex);
            batches.put(node.id(), ready);
        }
        return batches;
    }
}


```



TransactionManager为每个发送的消息batch，都会生成序列号。每个分区都对应单独的序列号

```java
public class TransactionManager {
    // 为每个分区，维护一个消息序列号
    private final Map<TopicPartition, Integer> nextSequence;

    synchronized Integer sequenceNumber(TopicPartition topicPartition) {
        Integer currentSequenceNumber = nextSequence.get(topicPartition);
        if (currentSequenceNumber == null) {
            // 初始序列号为 0
            currentSequenceNumber = 0;
            nextSequence.put(topicPartition, currentSequenceNumber);
        }
        return currentSequenceNumber;
    }

    
    synchronized void incrementSequenceNumber(TopicPartition topicPartition, int increment) {
        Integer currentSequenceNumber = nextSequence.get(topicPartition);
        if (currentSequenceNumber == null)
            throw new IllegalStateException("Attempt to increment sequence number for a partition with no current sequence.");
        // 更新分区对应的序列号
        currentSequenceNumber += increment;
        nextSequence.put(topicPartition, currentSequenceNumber);
    }
}
```





## 服务端处理请求 ##



```scala
class Log(...) {
    
  private def analyzeAndValidateProducerState(records: MemoryRecords, isFromClient: Boolean):
  (mutable.Map[Long, ProducerAppendInfo], List[CompletedTxn], Option[BatchMetadata]) = {
    val updatedProducers = mutable.Map.empty[Long, ProducerAppendInfo]
    val completedTxns = ListBuffer.empty[CompletedTxn]
      // 遍历消息batch
    for (batch <- records.batches.asScala if batch.hasProducerId) {
      // 获取该producer_id对应的producer，发送的最近请求
      val maybeLastEntry = producerStateManager.lastEntry(batch.producerId)

      // 这里的请求有可能来自客户端，也有可能是从leader分区向follower分区发来的
      // if this is a client produce request, there will be up to 5 batches which could have been duplicated.
      // If we find a duplicate, we return the metadata of the appended batch to the client.
      if (isFromClient) {
        // 
        maybeLastEntry.flatMap(_.findDuplicateBatch(batch)).foreach { duplicate =>
          return (updatedProducers, completedTxns.toList, Some(duplicate))
        }
      }

      val maybeCompletedTxn = updateProducers(batch, updatedProducers, isFromClient = isFromClient)
      maybeCompletedTxn.foreach(completedTxns += _)
    }
    (updatedProducers, completedTxns.toList, None)
  }

  private def updateProducers(batch: RecordBatch,
                              producers: mutable.Map[Long, ProducerAppendInfo],
                              isFromClient: Boolean): Option[CompletedTxn] = {
    val producerId = batch.producerId
    val appendInfo = producers.getOrElseUpdate(producerId, producerStateManager.prepareUpdate(producerId, isFromClient))
    // 校检序列号
    appendInfo.append(batch)
  }    
}
```





```scala
class ProducerStateManager(val topicPartition: TopicPartition,
                           @volatile var logDir: File,
                           val maxProducerIdExpirationMs: Int = 60 * 60 * 1000) extends Logging {

  def prepareUpdate(producerId: Long, isFromClient: Boolean): ProducerAppendInfo = {
    val validationToPerform =
      if (!isFromClient)
        ValidationType.None
      else if (topicPartition.topic == Topic.GROUP_METADATA_TOPIC_NAME)
        // 如果是向__consumer_offsets内部topic写数据，则只检验epoch
        ValidationType.EpochOnly
      else
        // 检验 epoch 和 序列号
        ValidationType.Full

    val currentEntry = lastEntry(producerId).getOrElse(ProducerStateEntry.empty(producerId))
    // 生成ProducerAppendInfo实例，后面会执行校检
    new ProducerAppendInfo(producerId, currentEntry, validationToPerform)
  }
}    
    
```



```scala
private[log] class ProducerAppendInfo(val producerId: Long,
                                      val currentEntry: ProducerStateEntry,
                                      val validationType: ValidationType) {
                                      
  private def maybeValidateAppend(producerEpoch: Short, firstSeq: Int) = {
    validationType match {
      case ValidationType.None =>

      case ValidationType.EpochOnly =>
        checkProducerEpoch(producerEpoch)

      case ValidationType.Full =>
        checkProducerEpoch(producerEpoch)
        checkSequence(producerEpoch, firstSeq)
    }
  }
  
  private def checkProducerEpoch(producerEpoch: Short): Unit = {
    if (producerEpoch < updatedEntry.producerEpoch) {
      throw new ProducerFencedException(s"Producer's epoch is no longer valid. There is probably another producer " +
        s"with a newer epoch. $producerEpoch (request epoch), ${updatedEntry.producerEpoch} (server epoch)")
    }
  }    
    
  private def checkSequence(producerEpoch: Short, appendFirstSeq: Int): Unit = {
    if (producerEpoch != updatedEntry.producerEpoch) {
      if (appendFirstSeq != 0) {
        if (updatedEntry.producerEpoch != RecordBatch.NO_PRODUCER_EPOCH) {
          throw new OutOfOrderSequenceException(s"Invalid sequence number for new epoch: $producerEpoch " +
            s"(request epoch), $appendFirstSeq (seq. number)")
        } else {
          throw new UnknownProducerIdException(s"Found no record of producerId=$producerId on the broker. It is possible " +
            s"that the last message with the producerId=$producerId has been removed due to hitting the retention limit.")
        }
      }
    } else {
      val currentLastSeq = if (!updatedEntry.isEmpty)
        updatedEntry.lastSeq
      else if (producerEpoch == currentEntry.producerEpoch)
        currentEntry.lastSeq
      else
        RecordBatch.NO_SEQUENCE

      if (currentLastSeq == RecordBatch.NO_SEQUENCE && appendFirstSeq != 0) {
        // the epoch was bumped by a control record, so we expect the sequence number to be reset
        throw new OutOfOrderSequenceException(s"Out of order sequence number for producerId $producerId: found $appendFirstSeq " +
          s"(incoming seq. number), but expected 0")
      } else if (!inSequence(currentLastSeq, appendFirstSeq)) {
        throw new OutOfOrderSequenceException(s"Out of order sequence number for producerId $producerId: $appendFirstSeq " +
          s"(incoming seq. number), $currentLastSeq (current end sequence number)")
      }
    }
  }

  private def inSequence(lastSeq: Int, nextSeq: Int): Boolean = {
    nextSeq == lastSeq + 1L || (nextSeq == 0 && lastSeq == Int.MaxValue)
  }    
    
    
}
  
                                      
                                      
```

## 错误处理 ##

错误都是可重试的，会将消息重新插入到RecordAccumulator里



插入消息，需要按照消息的序列号插入





