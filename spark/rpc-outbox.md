# 消息发送 #

## Outbox ##

Outbox 代表着发送者，一个Outbox对应着一个服务。请求该服务的消息，都要先发送到，然后由Outbox发送出去。Outbox管理着与服务的通信和发送的消息队列。

### 创建连接 ###

使用后台线程池连接，这个线程池的大小为配置项spark.rpc.connect.threads，默认60。超时时间为60s。

注意后台线程创建完连接后，它会主动尝试发送队列里的消息。

```scala
  private def launchConnectTask(): Unit = {
    // 提交连接任务给，nettyEnv的的线程池
    connectFuture = nettyEnv.clientConnectionExecutor.submit(new Callable[Unit] {

      override def call(): Unit = {
        try {
          val _client = nettyEnv.createClient(address)
          outbox.synchronized {
            
            client = _client
            if (stopped) {
              closeClient()
            }
          }
        } catch {
          case ie: InterruptedException =>
            // exit
            return
          case NonFatal(e) =>
            outbox.synchronized { connectFuture = null }
            handleNetworkFailure(e)
            return
        }
        outbox.synchronized { connectFuture = null }
        // 连接完成后，就尝试发送消息
        drainOutbox()
      }
    })
  }
```

### 发送消息 ###

首先将消息添加到队列里面，然后判断是否已经连接服务器，如果没有连接，则请求后台线程创建连接。如果有，则直接发送。注意到，这里发送消息，有线程冲突。因为后台线程创建完连接后，它会主动尝试发送队列里的消息。

```scala
  private def drainOutbox(): Unit = {
    var message: OutboxMessage = null
     // 锁，防止发送消息的主线程，和创建连接的线程有冲突
    synchronized {
      if (stopped) {
        return
      }
      if (connectFuture != null) {
        // connectFuture不为null，表示正在创建连接中，但未完成
        return
      }
      if (client == null) {
        // 如果connectFuture为null，client也为null，表示没有连接
        // 所以这儿提交创建新连接的任务
        launchConnectTask()
        return
      }
      if (draining) {
        // draining为true，表示已有线程正在发送消息
        return
      }
      message = messages.poll()
      // messages.poll返回null，表示消息队列都已经发送完成
      if (message == null) {
        return
      }
      // 更新draiing为true
      draining = true
    }
    while (true) {
      try {
        val _client = synchronized { client }
        if (_client != null) {
          // 发送消息
          message.sendWith(_client)
        } else {
          assert(stopped == true)
        }
      } catch {
        case NonFatal(e) =>
          handleNetworkFailure(e)
          return
      }
      synchronized {
        if (stopped) {
          return
        }
        // 循环从队列获取消息
        message = messages.poll()
        if (message == null) {
          // 知道所有的消息发送完成
          draining = false
          return
        }
      }
    }
  }
```



## NettyRpcEnv ##

NettyRpcEnv包含Outbox列表，当发送消息时，如果发现没有对应的Outbox，则创建。

```scala
class NettyRpcEnv() {
  private def postToOutbox(receiver: NettyRpcEndpointRef, message: OutboxMessage): Unit = {
    if (receiver.client != null) {
      message.sendWith(receiver.client)
    } else {
      require(receiver.address != null,
        "Cannot send message to client endpoint with no listen address.")
      val targetOutbox = {
        val outbox = outboxes.get(receiver.address)
        if (outbox == null) {
          val newOutbox = new Outbox(this, receiver.address)
          val oldOutbox = outboxes.putIfAbsent(receiver.address, newOutbox)
          if (oldOutbox == null) {
            newOutbox
          } else {
            oldOutbox
          }
        } else {
          outbox
        }
      }
      if (stopped.get) {
        // It's possible that we put `targetOutbox` after stopping. So we need to clean it.
        outboxes.remove(receiver.address)
        targetOutbox.stop()
      } else {
        targetOutbox.send(message)
      }
    }
  }
}
    
```









