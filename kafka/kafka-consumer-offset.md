# Kafka Consumer 管理 Offset 原理





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





```java
public class SubscriptionState {
    private final PartitionStates<TopicPartitionState> assignment;
}
```





consumer 在获取GroupCoordinator的分配结果后，会为分配到的每个分区，初始化TopicPartitionState数据。

