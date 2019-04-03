# Kafka Consumer 管理 Offset 原理



## Consumer 元数据 ##

Consumer的元数据分为两部分，订阅信息和分区信息。



### 订阅信息 ###

Consumer消费有两种模式，订阅模式和分配模式。

订阅模式是consumer只需要指定哪些topic需要订阅，由GroupCoordinator分配分区。

分配模式是consumer自己指定哪些分区。

```java
public class SubscriptionState {
    // 订阅类型
    // AUTO_TOPICS 属于订阅模式，它指定哪些topic
    // AUTO_PATTERN 属于订阅模式，它使用正则匹配topic
    // USER_ASSIGNED 属于分配模式
    private enum SubscriptionType {
        NONE, AUTO_TOPICS, AUTO_PATTERN, USER_ASSIGNED
    }
    
    // 采用哪种订阅模式
    private SubscriptionType subscriptionType;

    // 订阅的正则表达式，只有订阅类型为AUTO_PATTERN，才会被使用
    private Pattern subscribedPattern;

    // 订阅的topic名称列表，只有订阅类型为AUTO_TOPICS，才会被使用
    private Set<String> subscription;
}
```



当consumer使用订阅模式，收到服务端返回的分区分配结果后，会保存起来。我们回想一下ConsumerCoordinator的原理，当它收到分配结果后，会调用onJoinComplete回调函数。

```java
public final class ConsumerCoordinator extends AbstractCoordinator {
  
    private final SubscriptionState subscriptions;    
    
    @Override
    protected void onJoinComplete(int generation,
                                  String memberId,   // 该成员 id
                                  String assignmentStrategy,   // 分配算法
                                  ByteBuffer assignmentBuffer) {   // 序列化之后的分配结果
        // 解析分配结果
        Assignment assignment = ConsumerProtocol.deserializeAssignment(assignmentBuffer);
        // 保存到SubscriptionState类
        subscriptions.assignFromSubscribed(assignment.partitions());                                       ......
    }
}                                 
```

SubscriptionState提供了assignFromSubscribed方法，来保存订阅模式的分配结果

```java
public class SubscriptionState {
    // 分配的分区信息
    private final PartitionStates<TopicPartitionState> assignment;

    public void assignFromSubscribed(Collection<TopicPartition> assignments) {
        // 为这些分区，初始化分区信息 TopicPartitionState
        Map<TopicPartition, TopicPartitionState> assignedPartitionStates = partitionToStateMap(assignments);
        // 触发回调函数
        fireOnAssignment(assignedPartitionStates.keySet());
        // 保存这些分区的信息
        this.assignment.set(assignedPartitionStates);
    }
}
```



当consumer使用分配模式时，必须手动指定分区。KafkaConsumer类提供了assign方法来指定分区

```java
public class KafkaConsumer<K, V> implements Consumer<K, V> {

    @Override
    public void assign(Collection<TopicPartition> partitions) {
        acquireAndEnsureOpen();
        try {
            // 检查指定的分区不为 null
            if (partitions == null) {
                throw new IllegalArgumentException("Topic partition collection to assign to cannot be null");
            } else if (partitions.isEmpty()) {
                //如果 partitions 为空，那么表明是不在消费消息
                this.unsubscribe();
            } else {
                // 指定的分区涉及到的topic列表
                Set<String> topics = new HashSet<>();
                for (TopicPartition tp : partitions) {
                    String topic = (tp != null) ? tp.topic() : null;
                    if (topic == null || topic.trim().isEmpty())
                        throw new IllegalArgumentException("Topic partitions to assign to cannot have null or empty topic");
                    topics.add(topic);
                }

                // make sure the offsets of topic partitions the consumer is unsubscribing from
                // are committed since there will be no following rebalance
                this.coordinator.maybeAutoCommitOffsetsAsync(time.milliseconds());

                // 将分区保存到SubscriptionState里
                this.subscriptions.assignFromUser(new HashSet<>(partitions));
                // 更新Metadata获取哪些topic的元数据
                metadata.setTopics(topics);
            }
        } finally {
            release();
        }
    }
}
```



SubscriptionState提供了assignFromSubscribed方法，来保存订阅模式的分配结果

```java
public class SubscriptionState {
  
    private final PartitionStates<TopicPartitionState> assignment;
    
    public void assignFromUser(Set<TopicPartition> partitions) {
        // 设置订阅类型为 USER_ASSIGNED
        setSubscriptionType(SubscriptionType.USER_ASSIGNED);

        if (!this.assignment.partitionSet().equals(partitions)) {
            // 执行回调函数
            fireOnAssignment(partitions);
            // 为每个分区，初始化分区信息
            Map<TopicPartition, TopicPartitionState> partitionToState = new HashMap<>();
            for (TopicPartition partition : partitions) {
                TopicPartitionState state = assignment.stateValue(partition);
                if (state == null)
                    state = new TopicPartitionState();
                partitionToState.put(partition, state);
            }
            // 保存到assignment
            this.assignment.set(partitionToState);
        }
    }
}
```



### 分区信息 ###

consumer为分配的分区，保存了对应的信息，以TopicPartitionState类表示。

```java
private static class TopicPartitionState {
    private Long position; // 消费位置
    private Long highWatermark; // 高水位
    private Long logStartOffset; // the log start offset
    private Long lastStableOffset;
    private boolean paused;  // whether this partition has been paused by the user
    private OffsetResetStrategy resetStrategy;  // 如何初始化consumer的消费位置
    private Long nextAllowedRetryTimeMs;
}
```

这些分区的消息都会保存在SubscriptionState里

 

## 获取 Offset 初始值 ##

KafkaConsumer每次发送请求时，都需要指定消费位置。如果该consumer所属的组，有消费记录，那么就会上次消费记录开始。如果没有，则需要根据auto.offset.reset配置项，来判断从分区开始位置，还是分区末尾位置读取。

KafkaConsumer提供 poll 方法拉取数据。poll 方法会去检查分区的消费位置

```java
public class KafkaConsumer<K, V> implements Consumer<K, V> {
    
    private ConsumerRecords<K, V> poll(final long timeoutMs, final boolean includeMetadataInTimeout) {
        acquireAndEnsureOpen();
        try {
            // 在拉取数据之前，必须先要配置好订阅信息
            if (this.subscriptions.hasNoSubscriptionOrUserAssignment()) {
                throw new IllegalStateException("Consumer is not subscribed to any topics or assigned any partitions");
            }

            long elapsedTime = 0L;
            do {
                // 通知NetworkClient，防止它阻塞
                client.maybeTriggerWakeup();
                final long metadataEnd;
                if (includeMetadataInTimeout) {
                    final long metadataStart = time.milliseconds();
                    // 调用updateAssignmentMetadataIfNeeded方法，检查消费位置
                    if (!updateAssignmentMetadataIfNeeded(remainingTimeAtLeastZero(timeoutMs, elapsedTime))) {
                        return ConsumerRecords.empty();
                    }
                    metadataEnd = time.milliseconds();
                    elapsedTime += metadataEnd - metadataStart;
                } else {
                    while (!updateAssignmentMetadataIfNeeded(Long.MAX_VALUE)) {
                        log.warn("Still waiting for metadata");
                    }
                    metadataEnd = time.milliseconds();
                }
                // 拉取数据
                final Map<TopicPartition, List<ConsumerRecord<K, V>>> records = pollForFetches(remainingTimeAtLeastZero(timeoutMs, elapsedTime));

                if (!records.isEmpty()) {
                    if (fetcher.sendFetches() > 0 || client.hasPendingRequests()) {
                        client.pollNoWakeup();
                    }

                    return this.interceptors.onConsume(new ConsumerRecords<>(records));
                }
                final long fetchEnd = time.milliseconds();
                elapsedTime += fetchEnd - metadataEnd;

            } while (elapsedTime < timeoutMs);

            return ConsumerRecords.empty();
        } finally {
            release();
        }
    }
}    
```





会调用updateAssignmentMetadataIfNeeded方法来加入组和获取分区的信息。

在加入组后，这里面会调用updateFetchPositions方法，获取分区信息。

分区信息分为两部分，一个是该consumer所属组的对于这个分区的消费位置。如果不存在，那么就会根据配置，决定是以该分区的开始位置还是该分区的结束位置，然后获取开始位置或者结束位置。









## 提交 Offset ##





