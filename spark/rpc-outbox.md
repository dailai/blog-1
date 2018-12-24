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
      // receiver.client非空，表示这是server在响应client
      message.sendWith(receiver.client)
    } else {
      require(receiver.address != null,
        "Cannot send message to client endpoint with no listen address.")
      val targetOutbox = {
        // 根据发送地址，从outboxes取出对应的Outbox
        val outbox = outboxes.get(receiver.address)
        if (outbox == null) {
          // 如果没有则实例化
          val newOutbox = new Outbox(this, receiver.address)
          // 因为有多个线程同时发送消息，所以这里调用putIfAbsent方法，防止竞争
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
        // 调用Outbox发送消息
        targetOutbox.send(message)
      }
    }
  }
}
    
```



## 消息类型 ##

Outbox提供了send方法，给调用方发送消息。从send方法的申明可以看到，它只接受OutboxMessage类型的消息。OutboxMessage表示发送消息，它有两个子类，OneWayOutboxMessage和RpcOutboxMessage。OneWayOutboxMessage表示，请求不需要返回结果。RpcOutboxMessage表示，需要返回结果。



### OneWayOutboxMessage ###

```scala
private[netty] case class OneWayOutboxMessage(content: ByteBuffer) extends OutboxMessage
  with Logging {

  override def sendWith(client: TransportClient): Unit = {
    client.send(content)
  }

  override def onFailure(e: Throwable): Unit = {
    e match {
      case e1: RpcEnvStoppedException => logWarning(e1.getMessage)
      case e1: Throwable => logWarning(s"Failed to send one-way RPC.", e1)
    }
  }

}
```

发送OneWayOutboxMessage，就直接调用TransportClient的send方法发送出去。



### RpcOutboxMessage ###

```scala
private[netty] case class RpcOutboxMessage(
    content: ByteBuffer,
    _onFailure: (Throwable) => Unit,
    _onSuccess: (TransportClient, ByteBuffer) => Unit)
  extends OutboxMessage with RpcResponseCallback with Logging {
        private var client: TransportClient = _
  private var requestId: Long = _

  override def sendWith(client: TransportClient): Unit = {
    this.client = client
    this.requestId = client.sendRpc(content, this)
  }
      
  def onTimeout(): Unit = {
    if (client != null) {
      client.removeRpcRequest(requestId)
    } else {
      logError("Ask timeout before connecting successfully")
    }
  }
      
  override def onFailure(e: Throwable): Unit = {
    _onFailure(e)
  }

  override def onSuccess(response: ByteBuffer): Unit = {
    _onSuccess(client, response)
  }
```

发送RpcOutboxMessage，首先保存了TransportClient， 然后将自身作为回调对象，通过TransportClient的sendRpc发送出去。

这里有三个回调函数，onTimeout代表着超时，这里超时功能的实现，是由NettyRpcEnv的timeoutScheduler实现的。

onSuccess代表着成功返回，是在TransportResponseHandler中，会被调用。

onFailure代表着失败，当连接失败时，Outbox会调用这个方法。





