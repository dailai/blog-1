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



RequestFuture有两个方法比较特殊，

chain方法，负责连接一个RequestFuture。当父RequestFuture完成时，子RequestFuture也同时完成

```java
public void chain(final RequestFuture<T> future) {
    addListener(new RequestFutureListener<T>() {
        @Override
        public void onSuccess(T value) {
            future.complete(value);
        }

        @Override
        public void onFailure(RuntimeException e) {
            future.raise(e);
        }
    });
}
```



compose方法，负责转换RequestFuture的泛型。它接收RequestFutureAdapter实例

```java
public <S> RequestFuture<S> compose(final RequestFutureAdapter<T, S> adapter) {
    final RequestFuture<S> adapted = new RequestFuture<>();
    // 添加监听器
    addListener(new RequestFutureListener<T>() {
        @Override
        public void onSuccess(T value) {
            adapter.onSuccess(value, adapted);
        }

        @Override
        public void onFailure(RuntimeException e) {
            adapter.onFailure(e, adapted);
        }
    });
    return adapted;
}
```



这里注意下RequestFutureAdapter，

它的onSuccess方法，必须根据value参数，生成新的值，并且赋予给adapted这个RequestFuture。

同样它的onFailure方法，必须根据exception参数，生成新的异常，并且赋予给adapted这个RequestFuture。