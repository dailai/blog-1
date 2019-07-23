



Fetcher发送消息，并且缓存消息结果。

FetchSessionHandler构建请求，并且检验响应。





寻找partition 中指定时间对应的消息



ListOffsetRequest 请求 

请求的节点 id

需要请求的哪些 partition，和 partition对应的时间。如果时间为-2，表示这个partition的起始位置。如果为-1，表示这个partition的结束位置。



ListOffsetResponse 响应

针对请求的每个 partition ， 都会返回它的 ofsset 和 该 offfset对应消息的timestamp





构建请求



```java
public class FetchMetadata {
    private final int sessionId;

    private final int epoch;
}
```









```java
public class SubscriptionState {
    private SubscriptionType subscriptionType; // 订阅方式
    
    private final OffsetResetStrategy defaultResetStrategy;  // 当该consumer group对于这个partition的消费offset不存在时，采用哪种方式缺人初始offset
    
    private final PartitionStates<TopicPartitionState> assignment;  // 包含了每个partition的状态
    
    private static class TopicPartitionState {
        private Long position; // last consumed position
        private Long highWatermark; // the high watermark from last fetch
        private Long logStartOffset; // the log start offset
        private Long lastStableOffset;
        private boolean paused;  // whether this partition has been paused by the user
        private OffsetResetStrategy resetStrategy;  // the strategy to use if the offset needs resetting
        private Long nextAllowedRetryTimeMs;    
    
}
```





FetchSessionHandler 构建请求数据，数据由FetchRequestData表示

```java
public class FetchSessionHandler {
    public static class FetchRequestData {
        /**
         * The partitions to send in the fetch request.
         */
        private final Map<TopicPartition, PartitionData> toSend;

        /**
         * The partitions to send in the request's "forget" list.
         */
        private final List<TopicPartition> toForget;

        /**
         * All of the partitions which exist in the fetch request session.
         */
        private final Map<TopicPartition, PartitionData> sessionPartitions;

        /**
         * The metadata to use in this fetch request.
         */
        private final FetchMetadata metadata;

    }
}
```



FetchSessionHandler 每个构建请求，FetchSessionHandler的内部类Buidler负责构建

```java
public class FetchMetadata {
    
    private final int sessionId;
    private final int epoch;
    
    public static final int INVALID_SESSION_ID = 0;
    public static final int INITIAL_EPOCH = 0;
    public static final int FINAL_EPOCH = -1;    
    
    public static final FetchMetadata INITIAL = new FetchMetadata(INVALID_SESSION_ID, INITIAL_EPOCH);
    public static final FetchMetadata LEGACY = new FetchMetadata(INVALID_SESSION_ID, FINAL_EPOCH);

    public boolean isFull() {
        return (this.epoch == INITIAL_EPOCH) || (this.epoch == FINAL_EPOCH);
    }    
}
```

每次请求都会包含这两个字段。注意到与每个节点的初始数据，都是 INITIAL 实例。

当收到第一个请求后，会更新sessionId，并且自增epoch的值。

当后来的请求成功时，自会增加epoch的值。



 

FetchSessionHandler在构建请求

```java
public class FetchSessionHandler {
    
    private final int node;   // 对应服务节点
    private FetchMetadata nextMetadata = FetchMetadata.INITIAL;   // 下次发送时的元数据，包含epoch（表示请求的序列号）和 session（）
    private LinkedHashMap<TopicPartition, PartitionData> sessionPartitions =
        new LinkedHashMap<>(0);
    
    public class Builder {
        private LinkedHashMap<TopicPartition, PartitionData> next = new LinkedHashMap<>();
        private LinkedHashMap<TopicPartition, PartitionData> next = new LinkedHashMap<>();
        
        public void add(TopicPartition topicPartition, PartitionData data) {
            next.put(topicPartition, data);
        }
        
        public FetchRequestData build() {
            if (nextMetadata.isFull()) {
                log.debug("Built full fetch {} for node {} with {}.",
                    nextMetadata, node, partitionsToLogString(next.keySet()));
                sessionPartitions = next;
                next = null;
                Map<TopicPartition, PartitionData> toSend =
                    Collections.unmodifiableMap(new LinkedHashMap<>(sessionPartitions));
                return new FetchRequestData(toSend, Collections.<TopicPartition>emptyList(), toSend, nextMetadata);
            }

            List<TopicPartition> added = new ArrayList<>();
            List<TopicPartition> removed = new ArrayList<>();
            List<TopicPartition> altered = new ArrayList<>();
            for (Iterator<Entry<TopicPartition, PartitionData>> iter =
                     sessionPartitions.entrySet().iterator(); iter.hasNext(); ) {
                Entry<TopicPartition, PartitionData> entry = iter.next();
                TopicPartition topicPartition = entry.getKey();
                PartitionData prevData = entry.getValue();
                PartitionData nextData = next.get(topicPartition);
                if (nextData != null) {
                    if (prevData.equals(nextData)) {
                        // Omit this partition from the FetchRequest, because it hasn't changed
                        // since the previous request.
                        next.remove(topicPartition);
                    } else {
                        // Move the altered partition to the end of 'next'
                        next.remove(topicPartition);
                        next.put(topicPartition, nextData);
                        entry.setValue(nextData);
                        altered.add(topicPartition);
                    }
                } else {
                    // Remove this partition from the session.
                    iter.remove();
                    // Indicate that we no longer want to listen to this partition.
                    removed.add(topicPartition);
                }
            }
            // Add any new partitions to the session.
            for (Iterator<Entry<TopicPartition, PartitionData>> iter =
                     next.entrySet().iterator(); iter.hasNext(); ) {
                Entry<TopicPartition, PartitionData> entry = iter.next();
                TopicPartition topicPartition = entry.getKey();
                PartitionData nextData = entry.getValue();
                if (sessionPartitions.containsKey(topicPartition)) {
                    // In the previous loop, all the partitions which existed in both sessionPartitions
                    // and next were moved to the end of next, or removed from next.  Therefore,
                    // once we hit one of them, we know there are no more unseen entries to look
                    // at in next.
                    break;
                }
                sessionPartitions.put(topicPartition, nextData);
                added.add(topicPartition);
            }
            log.debug("Built incremental fetch {} for node {}. Added {}, altered {}, removed {} " +
                    "out of {}", nextMetadata, node, partitionsToLogString(added),
                    partitionsToLogString(altered), partitionsToLogString(removed),
                    partitionsToLogString(sessionPartitions.keySet()));
            Map<TopicPartition, PartitionData> toSend =
                Collections.unmodifiableMap(new LinkedHashMap<>(next));
            Map<TopicPartition, PartitionData> curSessionPartitions =
                Collections.unmodifiableMap(new LinkedHashMap<>(sessionPartitions));
            next = null;
            return new FetchRequestData(toSend, Collections.unmodifiableList(removed),
                curSessionPartitions, nextMetadata);
        }        
        
    }
}
```





Fetcher类将获取的消息，保存到列表里面。



KafkaConsumer在每次 poll 时候，会调用updateAssignmentMetadataIfNeeded方法来加入组和获取分区的信息。

在加入组后，这里面会调用updateFetchPositions方法，获取分区信息。

分区信息分为两部分，一个是该consumer所属组的对于这个分区的消费位置。如果不存在，那么就会根据配置，决定是以该分区的开始位置还是该分区的结束位置，然后获取开始位置或者结束位置。



```java
public final class ConsumerCoordinator extends AbstractCoordinator {

    public boolean refreshCommittedOffsetsIfNeeded(final long timeoutMs) {
        final Set<TopicPartition> missingFetchPositions = subscriptions.missingFetchPositions();

        final Map<TopicPartition, OffsetAndMetadata> offsets = fetchCommittedOffsets(missingFetchPositions, timeoutMs);
        if (offsets == null) return false;

        for (final Map.Entry<TopicPartition, OffsetAndMetadata> entry : offsets.entrySet()) {
            final TopicPartition tp = entry.getKey();
            final long offset = entry.getValue().offset();
            log.debug("Setting offset for partition {} to the committed offset {}", tp, offset);
            this.subscriptions.seek(tp, offset);
        }
        return true;
    }
}
```





将那些没有消费记录的分区，重置开始位置。并且调用Fetcher获取对应分区的位置。

