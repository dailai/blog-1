# Kafka Consumer 管理 Offset 原理



## Consumer 元数据 ##

Consumer会将一些订阅信息和分区信息，保存到SubscriptionState类里。



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



如果是订阅模式，consumer在加入组之后，获取到分配结果，会将



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









```java
public static class PartitionState<S> {
    private final TopicPartition topicPartition;
    private final S value;
}
```





```java
public class PartitionStates<S> {
    private final LinkedHashMap<TopicPartition, S> map = new LinkedHashMap<>();
}
```





```java
public class SubscriptionState {
    private final PartitionStates<TopicPartitionState> assignment;
}
```





consumer 在获取GroupCoordinator的分配结果后，会为分配到的每个分区，初始化TopicPartitionState数据。







 

## 获取 Offset 初始值 ##

KafkaConsumer在每次 poll 时候，会调用updateAssignmentMetadataIfNeeded方法来加入组和获取分区的信息。

在加入组后，这里面会调用updateFetchPositions方法，获取分区信息。

分区信息分为两部分，一个是该consumer所属组的对于这个分区的消费位置。如果不存在，那么就会根据配置，决定是以该分区的开始位置还是该分区的结束位置，然后获取开始位置或者结束位置。









## 提交 Offset ##





