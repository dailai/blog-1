# Kafka Producer 原理 #





KafkaProducer将消息发送给RecordAccumulator缓存，

Sender会从RecordAccumulator拉取消息，然后发送出去



KafkaProducer会序列化消息的key和value，确定好发往哪个分区。



RecordAccumulator会将消息，以ProducerBatch的格式存储起来

它为每一个分区都生成了一个ProducerBatch的队列



ProducerBatch包含了多条消息，最终可以转换为MemoryRecords的格式。



ProducerBatch提供了两个重要的方法：

* tryAppend方法，支持添加消息
* records方法，返回MemoryRecords

ProducerBatch使用MemoryRecordsBuilder类，负责生成MemoryRecords。MemoryRecords是Kafka中保存消息，非常常见的一种格式。这里需要说明下，Kafka发送消息，是以batch为单位的，每个batch包含了多条消息。MemoryRecords定义了batch的数据格式。

当Kafka发送一个batch后，会得到响应。这个响应又包含了batch里每个消息的响应。

batch的响应由ProduceRequestResult类表示

```java
public final class ProduceRequestResult {

    private final TopicPartition topicPartition;

    private volatile Long baseOffset = null;
    private volatile long logAppendTime = RecordBatch.NO_TIMESTAMP;
    private volatile RuntimeException error;
}
```

单个消息的响应由FutureRecordMetadata类表示， 它在ProduceRequestResult之上生成

```java
public final class FutureRecordMetadata implements Future<RecordMetadata> {
    private final ProduceRequestResult result;   // batch响应
    private final long relativeOffset;           // 此条消息在batch中的位置
    private final long createTimestamp;          // 创建时间
    private final Long checksum;                 // 校检值
    private final int serializedKeySize;         // key序列化之后的数据长度
    private final int serializedValueSize;       // value序列化之后的数据长度
}
```

ProducerBatch在添加消息的时候，就生成了消息的响应。

```java
public final class ProducerBatch {
    
    // 保存了回调函数，当ProducerBatch发送完成后，会调用里面每条消息的回调函数
    private final List<Thunk> thunks = new ArrayList<>();
    // MemoryRecords构造器
    private final MemoryRecordsBuilder recordsBuilder;
    // 包含消息的数目
    int recordCount;
    // 表示batch响应的future
    final ProduceRequestResult produceFuture;


    public FutureRecordMetadata tryAppend(long timestamp, byte[] key, byte[] value, Header[] headers, Callback callback, long now) {
        // 检查是否有足够的缓存，存储此条消息。
        // 如果没有，则返回null
        if (!recordsBuilder.hasRoomFor(timestamp, key, value, headers)) {
            return null;
        } else {
            // 将消息添加到MemoryRecords构造器里
            Long checksum = this.recordsBuilder.append(timestamp, key, value, headers);
            // 生成此条消息响应数据的future
            // 注意到使用了this.recordCount属性，来表示此条消息在batch中的位置
            FutureRecordMetadata future = new FutureRecordMetadata(this.produceFuture, this.recordCount, timestamp, checksum, key == null ? -1 : key.length, value == null ? -1 : value.length);
            // 将回调函数和future添加到thunks队列
            thunks.add(new Thunk(callback, future));
            // 更新消息数目
            this.recordCount++;
            return future;
        }
    }
    
    public MemoryRecords records() {
        // tryAppend方法会将消息添加到recordsBuilder，这里调用build方法即可生成MemoryRecords
        return recordsBuilder.build();
    }
}
```



在接收完响应后，ProducerBatch还负责执行回调函数。ProducerBatch将回调函数和响应结果都保存在了thunks列表里，它会在在batch响应完成后，调用completeFutureAndFireCallbacks方法，依次执行每条消息的回调。

```java
public final class ProducerBatch {
    // baseOffset为该batch的起始消息，在Kafka 对应的 topic partition 中的 offset
    private void completeFutureAndFireCallbacks(long baseOffset, long logAppendTime, RuntimeException exception) {
        // 通过set方法设置batch的响应数据
        produceFuture.set(baseOffset, logAppendTime, exception);

        // 执行每个消息的回调函数
        for (Thunk thunk : thunks) {
            try {
                if (exception == null) {
                    // 获取消息的响应数据，并执行回调函数
                    RecordMetadata metadata = thunk.future.value();
                    if (thunk.callback != null)
                        // 注意这里第二个参数表示异常。如果为null，则表示请求成功
                        thunk.callback.onCompletion(metadata, null);
                } else {
                    // 注意到这儿，如果此次请求异常，只有设置消息的回调函数，才能知道
                    // 第二次参数表示异常实例
                    if (thunk.callback != null)
                        thunk.callback.onCompletion(null, exception);
                }
            } catch (Exception e) {
                log.error("Error executing user-provided callback on message for topic-partition '{}'", topicPartition, e);
            }
        }
        // 表示future已完成
        produceFuture.done();
    }
}
```



RecordAccumulator作为消息缓冲队列，KafkaProducer会将消息先发送给RecordAccumulator。RecordAccumulator将消息集合，然后由Sender线程分批发送出去。

RecordAccumulator的append方法，负责添加消息。append方法主要是负责管理ProducerBatch队列，当ProducerBatch数据已满，就会新建ProducerBatch。真正的添加消息是在tryAppend方法里。

```java
public final class RecordAccumulator {
    
    // 消息集合，Key为分区，Value为该分区对应的batch队列
    private final ConcurrentMap<TopicPartition, Deque<ProducerBatch>> batches;
    // ProducerBatch可以保存数据的最小长度
    private final int batchSize;
    // 缓存池
    private final BufferPool free;
    
    public RecordAppendResult append(TopicPartition tp,
                                     long timestamp,
                                     byte[] key,
                                     byte[] value,
                                     Header[] headers,
                                     Callback callback,
                                     long maxTimeToBlock) throws InterruptedException {
        
        ByteBuffer buffer = null;
        if (headers == null) headers = Record.EMPTY_HEADERS;
        try {
            // 获取该分区的batch队列，如果没有就创建
            Deque<ProducerBatch> dq = getOrCreateDeque(tp);
            // 这里使用了锁，保证线程竞争
            synchronized (dq) {
                if (closed)
                    throw new KafkaException("Producer closed while send in progress");
                // 调用tryAppend方法添加，如果返回null，则表示当前ProducerBatch空间已满
                RecordAppendResult appendResult = tryAppend(timestamp, key, value, headers, callback, dq);
                // 表示添加成功
                if (appendResult != null)
                    return appendResult;
            }
            byte maxUsableMagic = apiVersions.maxUsableProduceMagic();
            // 确定batch的大小，比较当前消息的长度和指定最小的长度
            int size = Math.max(this.batchSize, AbstractRecords.estimateSizeInBytesUpperBound(maxUsableMagic, compression, key, value, headers));
            // 从缓冲池中申请内存
            buffer = free.allocate(size, maxTimeToBlock);
            synchronized (dq) {
                if (closed)
                    throw new KafkaException("Producer closed while send in progress");
                // 因为在申请内存的时候，Sender线程有可能刚好提取了消息，所以这里再次尝试调用tryAppend
                // 或者别的线程已经在这个时候，创建完了新的ProducerBatch
                RecordAppendResult appendResult = tryAppend(timestamp, key, value, headers, callback, dq);
                if (appendResult != null) {
                    return appendResult;
                }
                // 生成MemoryRecords构造器，它使用的缓存就是刚刚申请
                MemoryRecordsBuilder recordsBuilder = recordsBuilder(buffer, maxUsableMagic);
                // 新建ProducerBatch
                ProducerBatch batch = new ProducerBatch(tp, recordsBuilder, time.milliseconds());
                // 调用新建的ProducerBatch的tryAppend方法添加数据
                FutureRecordMetadata future = Utils.notNull(batch.tryAppend(timestamp, key, value, headers, callback, time.milliseconds()));
                // 将新建的ProducerBatch，添加到队列里
                dq.addLast(batch);

                // 因为这个buffer已经被新的ProducerBatch所使用，
                // 所以这里将其设置null，防止被释放
                buffer = null;

                return new RecordAppendResult(future, dq.size() > 1 || batch.isFull(), true);
            }
        } finally {
            // 如果申请的buffer没有用到，这里需要将其释放掉
            if (buffer != null)
                free.deallocate(buffer);
        }
    }
    
    // 负责将消息添加到，batch队列中的最后一个
    private RecordAppendResult tryAppend(long timestamp, byte[] key, byte[] value, Header[] headers, Callback callback, Deque<ProducerBatch> deque) {
        // 取出最后的ProducerBatch
        ProducerBatch last = deque.peekLast();
        if (last != null) {
            // 调用batch的tryAppend添加
            FutureRecordMetadata future = last.tryAppend(timestamp, key, value, headers, callback, time.milliseconds());
            // 如果返回null，表示当前batch的空间已满
            if (future == null)
                // 关闭当前batch的添加
                last.closeForRecordAppends();
            else
                // 返回RecordAppendResult
                return new RecordAppendResult(future, deque.size() > 1 || last.isFull(), false);
        }
        return null;
    }
}
```



RecordAccumulator的drain方法负责，从队列中提取消息。目前这里先不考虑事务，代码简化如下

```java
// nodes参数，表示限制请求是这些节点
public Map<Integer, List<ProducerBatch>> drain(Cluster cluster, Set<Node> nodes, int maxSize, long now) {
    Map<Integer, List<ProducerBatch>> batches = new HashMap<>();
    for (Node node : nodes) {
        int size = 0;
        // 获取该节点有哪些分区
        List<PartitionInfo> parts = cluster.partitionsForNode(node.id());
        List<ProducerBatch> ready = new ArrayList<>();
        int start = drainIndex = drainIndex % parts.size();
        do {
            PartitionInfo part = parts.get(drainIndex);
            TopicPartition tp = new TopicPartition(part.topic(), part.partition());
            // Only proceed if the partition has no in-flight batches.
            if (!isMuted(tp, now)) {
                Deque<ProducerBatch> deque = getDeque(tp);
                if (deque != null) {
                    synchronized (deque) {
                        ProducerBatch first = deque.peekFirst();
                        if (first != null) {
                            boolean backoff = first.attempts() > 0 && first.waitedTimeMs(now) < retryBackoffMs;
                            if (!backoff) {
                                if (size + first.estimatedSizeInBytes() > maxSize && !ready.isEmpty()) {
                                    break;
                                } else {
                                    ProducerIdAndEpoch producerIdAndEpoch = null;
                                    boolean isTransactional = false;
                                    ProducerBatch batch = deque.pollFirst();
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
```

