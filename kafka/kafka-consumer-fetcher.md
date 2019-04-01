



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









