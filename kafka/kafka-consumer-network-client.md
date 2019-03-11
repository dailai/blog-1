# Kafka ConsumerNetworkClient 原理 #

ConsumerNetworkClient在NetworkClient之外封装了一层，目的提供了简单的回调机制。

ConsumerNetworkClient每次发送请求，会返回RequestFuture。RequestFuture支持添加事件回调函数。这样在编程时，会更加方便。

RequestFuture使用了CountDownLatch来实现等待的功能

```java
public class RequestFuture<T> implements ConsumerNetworkClient.PollCondition {
    private static final Object INCOMPLETE_SENTINEL = new Object();
    private final AtomicReference<Object> result = new AtomicReference<>(INCOMPLETE_SENTINEL);    
    
    public void addListener(RequestFutureListener<T> listener) {
        this.listeners.add(listener);
        if (failed())
            fireFailure();
        else if (succeeded())
            fireSuccess();
    }
```

