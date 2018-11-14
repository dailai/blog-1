# 客户端介绍 #

## 实例化客户端 ###

spark rpc的客户端使用EndpointRef类表示，它的唯一实现类是NettyRpcEndpointRef。EndpointRef的实例化，是有RpcEnv负责，RpcEnv的唯一实现类是NettyRpcEnv。

```scala
private[netty] class NettyRpcEnv(
  
  def asyncSetupEndpointRefByURI(uri: String): Future[RpcEndpointRef] = {
    val addr = RpcEndpointAddress(uri)
    val endpointRef = new NettyRpcEndpointRef(conf, addr, this)
    val verifier = new NettyRpcEndpointRef(
      conf, RpcEndpointAddress(addr.rpcAddress, RpcEndpointVerifier.NAME), this)
    verifier.ask[Boolean](RpcEndpointVerifier.CheckExistence(endpointRef.name)).flatMap { find =>
      if (find) {
        Future.successful(endpointRef)
      } else {
        Future.failed(new RpcEndpointNotFoundException(uri))
      }
    }(ThreadUtils.sameThread)
  }

```



## 发送消息 ##

```scala
private[netty] class NettyRpcEndpointRef(
    override def ask[T: ClassTag](message: Any, timeout: RpcTimeout): Future[T] = {
    nettyEnv.ask(new RequestMessage(nettyEnv.address, this, message), timeout)
  } 
```



NettyEnv有两种模式，一个是server模式，一个是client模式。当使用client模式时，它的server属性为null，address属性也为null。

```scala
private[netty] def ask[T: ClassTag](message: RequestMessage, timeout: RpcTimeout): Future[T] = {
    val promise = Promise[Any]()
    val remoteAddr = message.receiver.address

    def onFailure(e: Throwable): Unit = {
      if (!promise.tryFailure(e)) {
        logWarning(s"Ignored failure: $e")
      }
    }

    def onSuccess(reply: Any): Unit = reply match {
      case RpcFailure(e) => onFailure(e)
      case rpcReply =>
        if (!promise.trySuccess(rpcReply)) {
          logWarning(s"Ignored message: $reply")
        }
    }

    try {
      if (remoteAddr == address) {
        // 这里是客户端才会调用ask方法请求，所以nettyEnv的address是为null
        // 当remoteAddr为null时，则表明Endpoint和EndpointRef是同一个进程
        val p = Promise[Any]()
        p.future.onComplete {
          case Success(response) => onSuccess(response)
          case Failure(e) => onFailure(e)
        }(ThreadUtils.sameThread)
        dispatcher.postLocalMessage(message, p)
      } else {
        // 这里是发送消息给远程Endpoint
        val rpcMessage = RpcOutboxMessage(message.serialize(this),
          onFailure,
          (client, response) => onSuccess(deserialize[Any](client, response)))
        // 发送消息到Outbox
        postToOutbox(message.receiver, rpcMessage)
        promise.future.onFailure {
          case _: TimeoutException => rpcMessage.onTimeout()
          case _ =>
        }(ThreadUtils.sameThread)
      }

      val timeoutCancelable = timeoutScheduler.schedule(new Runnable {
        override def run(): Unit = {
          onFailure(new TimeoutException(s"Cannot receive any reply from ${remoteAddr} " +
            s"in ${timeout.duration}"))
        }
      }, timeout.duration.toNanos, TimeUnit.NANOSECONDS)
      promise.future.onComplete { v =>
        timeoutCancelable.cancel(true)
      }(ThreadUtils.sameThread)
    } catch {
      case NonFatal(e) =>
        onFailure(e)
    }
    promise.future.mapTo[T].recover(timeout.addMessageIfTimeout)(ThreadUtils.sameThread)
  }
```



## 消息序列化 ##

远程调用意味着通信，通信需要制定好数据格式。spark序列化消息，也有统一的标准，RequestMessage就是负责这个。它的序列化是基于java本身的。

### 消息格式 ###

```shell
-----------------------------------------------------------------
sender address |	receiver address	| name       |   content
-----------------------------------------------------------------
address 格式    |    address 格式         | string     | object
-----------------------------------------------------------------
```

上面的address消息分为两种格式，一种值是空，一种非空。

当为空时

```shell
-------------
False   
-------------
Boolean
-------------

```

当不为空时

```shell
-----------------------------
True      | host    |  port 
-----------------------------
Boolean   | string  |  int
------------------------------
```



### RequestMessage ###

RequestMessage类比较简单，它的serialize方法负责序列化消息。也是使用DataOutputStream的方法，将java的对象转换为字节。

```scala
private[netty] class RequestMessage(
    val senderAddress: RpcAddress,
    val receiver: NettyRpcEndpointRef,
    val content: Any) {

  /** Manually serialize [[RequestMessage]] to minimize the size. */
  def serialize(nettyEnv: NettyRpcEnv): ByteBuffer = {
    val bos = new ByteBufferOutputStream()
    val out = new DataOutputStream(bos)
    try {
      writeRpcAddress(out, senderAddress)
      writeRpcAddress(out, receiver.address)
      out.writeUTF(receiver.name)
      val s = nettyEnv.serializeStream(out)
      try {
        s.writeObject(content)
      } finally {
        s.close()
      }
    } finally {
      out.close()
    }
    bos.toByteBuffer
  }

  private def writeRpcAddress(out: DataOutputStream, rpcAddress: RpcAddress): Unit = {
    if (rpcAddress == null) {
      out.writeBoolean(false)
    } else {
      out.writeBoolean(true)
      out.writeUTF(rpcAddress.host)
      out.writeInt(rpcAddress.port)
    }
  }
```





## TransportContext初始化 ##

TransportClient的实例化由TransportClientFactory负责创建，而TransportClientFactory的实例化由TransportContext负责。