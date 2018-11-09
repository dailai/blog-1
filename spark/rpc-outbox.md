# 消息发送 #

## Outbox ##

Outbox 代表着发送者，一个Outbox对应着一个服务。请求该服务的消息，都要先发送到，然后由Outbox发送出去。Outbox管理着与服务的通信和发送的消息队列。



连接

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
        // It's possible that no thread is draining now. If we don't drain here, we cannot send the
        // messages until the next message arrives.
        drainOutbox()
      }
    })
  }
```





发送

首先将消息添加到队列里面，然后判断是否已经连接服务器，如果没有连接，则请求后台线程创建连接。如果有，则直接发送。注意到，这里发送消息，有线程冲突。因为后台线程创建完连接后，它会主动尝试发送队列里的消息。



## NettyRpcEnv ##

NettyRpcEnv包含Outbox列表。







