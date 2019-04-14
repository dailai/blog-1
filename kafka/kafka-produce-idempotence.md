# Kafka Producer 幂等性原理 #

幂等性是指发送同样的请求，对系统资源的影响是一致的。结合 Kafka Producer，是指在多次发送同样的消息，Kafka做到消息的不丢失和不重复。实现幂等性服务，关键的一点是如何处理异常，因为一般请求发生异常，才会重复请求。比如Kafka Producer与服务端的网络异常：

* Producer向服务端发送消息，但是此时连接就断开了，发送的消息经过网络传输时，被丢失了。服务端没有接收到消息。

* 服务端收到Producer发送的消息，处理完毕后，向Producer发送响应，但是此时连接断开了。发送的响应经过网络传输时，被丢失了。Producer没有收到响应。

因为两种情况对于Producer而言，都是没有收到响应，Producer无法确定是哪种情况，所以它必须要重新发送消息，来确保服务端不会漏掉一条消息。但这样服务端有可能会收到重复的消息，所以服务端收到消息后，还要做一次去重操作。只有Producer和服务端的相互配合，才能保证消息不丢失也不重复，达到 Exactly One 的情景。

Kafka的幂等性只支持单个producer向单个分区发送。它会为每个Producer生成一个唯一id号，这样Kafka服务端就可以根据 produce_id来确定是哪个生产者发送的消息。然后Kafka还为每条消息生成了一个序列号，Kafka服务端会保留最近的消息，根据序列号就可以判断该消息，是否近期已经发送过，来达到去重的效果。

## Producer 发送消息 ##



### 幂等性配置 ###

KafkaProducer 如果要使用幂等性，需要将 enable.idempotence 配置项设置为true。并且它对单个分区的发送，一次性最多发送5条。通过KafkaProducer的 configureInflightRequests 方法，可以看到对max.in.flight.requests.per.connection的限制

```java
public class KafkaProducer<K, V> implements Producer<K, V> {

    private static int configureInflightRequests(ProducerConfig config, boolean idempotenceEnabled) {
        if (idempotenceEnabled && 5 < config.getInt(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION)) {
            throw new ConfigException("Must set " + ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION + " to at most 5" +
                    " to use the idempotent producer.");
        }
        return config.getInt(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION);
    }
    
}
    
```



### 请求获取 producer_id ###

Sender在发送消息之前，都会去检查是否已经获取到了 produce_id。如果没有，则向服务端发送请求。获取到的响应数据，保存在TransactionManager类里。

```java
public class Sender implements Runnable {

    private void maybeWaitForProducerId() {
        // 判断是否获取了produce_id
        while (!transactionManager.hasProducerId() && !transactionManager.hasError()) {
            try {
                // 选择负载最轻的一台节点
                Node node = awaitLeastLoadedNodeReady(requestTimeoutMs);
                if (node != null) {
                    // 发送获取请求，接收响应
                    ClientResponse response = sendAndAwaitInitProducerIdRequest(node);
                    // 处理响应
                    InitProducerIdResponse initProducerIdResponse = (InitProducerIdResponse) response.responseBody();
                    // 检查错误
                    Errors error = initProducerIdResponse.error();
                    if (error == Errors.NONE) {
                        ProducerIdAndEpoch producerIdAndEpoch = new ProducerIdAndEpoch(
                                initProducerIdResponse.producerId(), initProducerIdResponse.epoch());
                        // 保存produce_id 和 produce_epoch结果（produce_epoch是在开启事务才会用到，这里仅仅是开启了幂等性，没有用到事务）
                        transactionManager.setProducerIdAndEpoch(producerIdAndEpoch);
                        return;
                    } else if (error.exception() instanceof RetriableException) {
                        // 如果该错误是可以重试，那么就等待下次重试
                        log.debug("Retriable error from InitProducerId response", error.message());
                    } else {
                        // 如果发生严重错误，那么保存错误信息，并且退出
                        transactionManager.transitionToFatalError(error.exception());
                        break;
                    }
                } else {
                   .....
                }
            } catch (UnsupportedVersionException e) {
                // 发生版本不支持的错误，则退出
                transactionManager.transitionToFatalError(e);
                break;
            } catch (IOException e) {
                // 发生网络通信问题，则等待下次重试
                log.debug("Broker {} disconnected while awaiting InitProducerId response", e);
            }
            // 等待一段时间，然后重试
            time.sleep(retryBackoffMs);
            metadata.requestUpdate();
        }
    }
    
    private ClientResponse sendAndAwaitInitProducerIdRequest(Node node) throws IOException {
        String nodeId = node.idString();
        // 构建InitProducerIdRequest请求
        InitProducerIdRequest.Builder builder = new InitProducerIdRequest.Builder(null);
        ClientRequest request = client.newClientRequest(nodeId, builder, time.milliseconds(), true, requestTimeoutMs, null);
        // 发送请求并且等待响应
        return NetworkClientUtils.sendAndReceive(client, request, time);
    }    
}
```



### 发送消息 ###

当开启了幂等性，KafkaProducer发送消息时，会额外设置producer_id 和 序列号字段。producer_id是从Kafka服务端请求获取的，序列号是Producer自增生成的。这里需要说明下，Kafka发送消息都是以batch的格式发送，batch包含了多条消息。所以Producer发送消息batch的时候，只会设置该batch的第一个消息的序列号，后面的消息的序列号是依次递增的。

当Sender从RecordAccumulator中拉取消息时，会设置produce_id 和 baseSequence两个字段。

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
                // 这里的isMute方法，是用来判断次分区的请求是否被关闭
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



从上面可以看到，TransactionManager负责为每个发送的消息batch，都会生成序列号。每个分区都有独立的序列号。

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

Kafka服务端接收到请求后，会调用ReplicaManager的方法处理。ReplicaManager最后通过Log类，检测请求的有效性，然后存储起来。Log类的 analyzeAndValidateProducerState 方法，负责检测消息的序列号是否有效。

```scala
class Log(...) {
    
  private def analyzeAndValidateProducerState(records: MemoryRecords, isFromClient: Boolean):
  (mutable.Map[Long, ProducerAppendInfo], List[CompletedTxn], Option[BatchMetadata]) = {
    // 添加信息的表，Key值为produce_id，Value为添加信息，它包含了新添加的消息batch
    val updatedProducers = mutable.Map.empty[Long, ProducerAppendInfo]
    val completedTxns = ListBuffer.empty[CompletedTxn]
    // 遍历消息 batch
    for (batch <- records.batches.asScala if batch.hasProducerId) {
      // 根据producer_id找到，对应producer发送的最近请求
      val maybeLastEntry = producerStateManager.lastEntry(batch.producerId)

      // 这里的请求有可能来自客户端，也有可能是从leader分区向follower分区发来的
      // if this is a client produce request, there will be up to 5 batches which could have been duplicated.
      // If we find a duplicate, we return the metadata of the appended batch to the client.
      if (isFromClient) {
        // 检测是否有近期重复的请求，如果有则立马返回
        maybeLastEntry.flatMap(_.findDuplicateBatch(batch)).foreach { duplicate =>
          return (updatedProducers, completedTxns.toList, Some(duplicate))
        }
      }
      // 将消息batch的添加信息，添加到updatedProducers表里
      val maybeCompletedTxn = updateProducers(batch, updatedProducers, isFromClient = isFromClient)
      maybeCompletedTxn.foreach(completedTxns += _)
    }
    (updatedProducers, completedTxns.toList, None)
  }

  private def updateProducers(batch: RecordBatch,
                              producers: mutable.Map[Long, ProducerAppendInfo],
                              isFromClient: Boolean): Option[CompletedTxn] = {
    val producerId = batch.producerId
    // 获取该 produce 对应的AppendInfo，如果没有则新建一个
    val appendInfo = producers.getOrElseUpdate(producerId, producerStateManager.prepareUpdate(producerId, isFromClient))
    // 将消息batch添加appendInfo里，添加过程中包含了校检序列号
    appendInfo.append(batch)
  }    
}
```



ProducerStateManager保存所有producer最近添加的消息batch。一个ProducerStateManager只负责管理一个分区的producer信息。

```scala
class ProducerStateManager(val topicPartition: TopicPartition,   
                           @volatile var logDir: File,
                           val maxProducerIdExpirationMs: Int = 60 * 60 * 1000) extends Logging {
  // Key为produce_id，Value为对应的信息，由ProducerStateEntry类表示
  private val producers = mutable.Map.empty[Long, ProducerStateEntry]
        
  def lastEntry(producerId: Long): Option[ProducerStateEntry] = producers.get(producerId)

  def prepareUpdate(producerId: Long, isFromClient: Boolean): ProducerAppendInfo = {
    val validationToPerform =
      if (!isFromClient)
        // 如果是Kafka节点之间的请求，那么这里不做任何校检
        ValidationType.None
      else if (topicPartition.topic == Topic.GROUP_METADATA_TOPIC_NAME)
        // 如果是向__consumer_offsets内部topic写数据，则只检验epoch
        ValidationType.EpochOnly
      else
        // 检验 epoch 和 序列号
        ValidationType.Full
    // 如果有该producer的信息，则直接返回。否则调用 empty 方法新建一个
    val currentEntry = lastEntry(producerId).getOrElse(ProducerStateEntry.empty(producerId))
    // 生成ProducerAppendInfo实例
    new ProducerAppendInfo(producerId, currentEntry, validationToPerform)
  }
}    
    
```



ProducerStateEntry类表示了producer的所有信息，它主要包含了最近添加的消息batch。ProducerStateEntry最多只能包含5个消息batch，注意这里只是包含了batch的元数据。

```scala
private[log] object ProducerStateEntry {
  // 最大保存batch的数目
  private[log] val NumBatchesToRetain = 5
  def empty(producerId: Long) = new ProducerStateEntry(producerId, mutable.Queue[BatchMetadata](), RecordBatch.NO_PRODUCER_EPOCH, -1, None)
}  

private[log] class ProducerStateEntry(val producerId: Long,
                                      val batchMetadata: mutable.Queue[BatchMetadata],  // 消息batch的元数据列表
                                      var producerEpoch: Short,
                                      var coordinatorEpoch: Int,
                                      var currentTxnFirstOffset: Option[Long]) {
    
  // 添加消息batch
  def addBatch(producerEpoch: Short, lastSeq: Int, lastOffset: Long, offsetDelta: Int, timestamp: Long): Unit = {
    // 如果该消息batch的epoch大，则更新producerEpoch
    maybeUpdateEpoch(producerEpoch)
    // 添加消息batch的元数据
    addBatchMetadata(BatchMetadata(lastSeq, lastOffset, offsetDelta, timestamp))
  }
    
  private def addBatchMetadata(batch: BatchMetadata): Unit = {
    // 如果保存的batch数目，达到了5个，则将旧的剔除掉
    if (batchMetadata.size == ProducerStateEntry.NumBatchesToRetain)
      batchMetadata.dequeue()
    // 添加到队列里
    batchMetadata.enqueue(batch)
  }     
    
}    
    
```



注意到上面实例化ProducerAppendInfo对象后，最后调用了它的 append 方法，来校检消息batch的有效性。ProducerAppendInfo最后还会生成新的ProducerStateEntry对象，存储最新一次添加batch的信息，然后替换旧的信息。

ProducerStateEntry都会检查epoch 和 sequence，校检步骤如下

如果消息批次的epoch小，则会报错。如果消息批次的epoch大，ProducerStateEntry会将旧的消息批次清空，并且更新自己的epoch。



如果消息批次的epoch比较大，它的序列号必须从0开始。否则就会出错。

序列号的检查，通过检查上次消息批次的结束序列号，新添加的消息批次的开始序列号，两者的是否是连续的。



ProducerAppendInfo会初始化一个新的ProducerStateEntry，当 添加成功后，会将旧的替换掉。

```scala
private[log] class ProducerAppendInfo(val producerId: Long,
                                      val currentEntry: ProducerStateEntry,  // 上次添加的消息batch的元数据
                                      val validationType: ValidationType) {  // 校检策略
  // 初始化新的ProducerStateEntry，保存到updatedEntry属性                                     
  private val updatedEntry = ProducerStateEntry.empty(producerId)
  updatedEntry.producerEpoch = currentEntry.producerEpoch
  updatedEntry.coordinatorEpoch = currentEntry.coordinatorEpoch
  updatedEntry.currentTxnFirstOffset = currentEntry.currentTxnFirstOffset    
    
  private def maybeValidateAppend(producerEpoch: Short, firstSeq: Int) = {
    // 对于不同的校检策略，做不同的检查
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
    // 如果该消息batch的produce_epoch比之前的还要下，那么就抛出ProducerFencedException错误
    if (producerEpoch < updatedEntry.producerEpoch) {
      throw new ProducerFencedException(s"Producer's epoch is no longer valid. There is probably another producer " +
        s"with a newer epoch. $producerEpoch (request epoch), ${updatedEntry.producerEpoch} (server epoch)")
    }
  }    
    
  private def checkSequence(producerEpoch: Short, appendFirstSeq: Int): Unit = {
    // 因为之前已经检查过了produce_epoch，如果出现了不相等的情况，只能是该消息batch的produce_epoch大
    if (producerEpoch != updatedEntry.producerEpoch) {
      // 如果是新的produce_epoch，那么它发送过来的第一个消息batch的序列号只能从0开始
      if (appendFirstSeq != 0) {
        // 如果是旧的produce，那么就抛出OutOfOrderSequenceException异常，表示此消息发送的序列号有问题
        // 否则抛出UnknownProducerIdException异常
        if (updatedEntry.producerEpoch != RecordBatch.NO_PRODUCER_EPOCH) {
          throw new OutOfOrderSequenceException(s"Invalid sequence number for new epoch: $producerEpoch " +
            s"(request epoch), $appendFirstSeq (seq. number)")
        } else {
          throw new UnknownProducerIdException(s"Found no record of producerId=$producerId on the broker. It is possible " +
            s"that the last message with the producerId=$producerId has been removed due to hitting the retention limit.")
        }
      }
    } else {
      // 获取最后一次添加的消息batch的结束序列号，如果这次添加的
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

​                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  





ProducerStateEntry 类包含了消息批次的元数据队列。当有新的消息批次添加进来，会将旧的消息批次删除，来保持队列的长度始终为5。

ProducerStateEntry还包含了produce_id 和 producer_epoch。

消息批次的元数据包含，开始和结束的offset，开始和结束的序列号。









遇到重复的batch，怎么处理







消息批次的produce_epoch只有在启动了事务，才会产生变化。当事务完成时，会更新producer_epoch。











## 错误处理 ##

错误都是可重试的，会将消息重新插入到RecordAccumulator里



插入消息，需要按照消息的序列号插入





