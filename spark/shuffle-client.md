# ShuffleClient #

ShuffleClient表示shuffle files的客户端，支持远程读取文件。

BlockTransferService继承ShuffleClient，增加了写文件。

NettyBlockTransferService继承BlockTransferService， 实现了所有的接口，也同时提供了



## 启动服务 ##

NettyBlockTransferService是基于Netty框架的，之前在rpc框架中也有讲到TransportServer。

```scala
override def init(blockDataManager: BlockDataManager): Unit = {
  // 初始化NettyBlockRpcServer
  val rpcHandler = new NettyBlockRpcServer(conf.getAppId, serializer, blockDataManager)
 // 生成TransportServerBootstrap
  var serverBootstrap: Option[TransportServerBootstrap] = None
  if (authEnabled) {
    serverBootstrap = Some(new AuthServerBootstrap(transportConf, securityManager))
	)
  }
  // 实例化transportContext， 注册 rpcHandler
  transportContext = new TransportContext(transportConf, rpcHandler)
  server = createServer(serverBootstrap.toList)
  appId = conf.getAppId
  logInfo(s"Server created on ${hostName}:${server.getPort}")
}

 private def createServer(bootstraps: List[TransportServerBootstrap]): TransportServer = {
    def startService(port: Int): (TransportServer, Int) = {
      // 通过transportContext实例化server
      val server = transportContext实例化server.createServer(bindAddress, port, bootstraps.asJava)
      (server, server.getPort)
    }

    Utils.startServiceOnPort(_port, startService, conf, getClass.getName)._1
  }
```

注意到NettyBlockRpcServer，它继承了RpcHandler，实现了处理请求的逻辑。NettyBlockRpcServer只接收OpenBlocks和UploadBlock请求。

```scala
class NettyBlockRpcServer(
    appId: String,
    serializer: Serializer,
    blockManager: BlockDataManager)
  extends RpcHandler with Logging {
    override def receive(
      client: TransportClient,
      rpcMessage: ByteBuffer,
      responseContext: RpcResponseCallback): Unit = {
    	val message = BlockTransferMessage.Decoder.fromByteBuffer(rpcMessage)
    	logTrace(s"Received request: $message")

    	message match {
            case openBlocks: OpenBlocks =>
            	// 生成streamId
            	val streamId = streamManager.registerStream(appId, blocks.iterator.asJava)
           		.......
            	// 响应StreamHandle结果
            	responseContext.onSuccess(new StreamHandle(streamId, blocksNum).toByteBuffer)
            
            case uploadBlock: UploadBlock =>
            	val data = new NioManagedBuffer(ByteBuffer.wrap(uploadBlock.blockData))
            	val blockId = BlockId(uploadBlock.blockId)
        		//  调用blockManager写入数据
            	blockManager.putBlockData(blockId, data, level, classTag)
        		responseContext.onSuccess(ByteBuffer.allocate(0))
        }
    }
  }
```



## 客户请求 ##

NettyBlockTransferService也实现了客户端的接口。fetchBlocks负责读取远端Block。这里请求支持失败重试，这里涉及到了RetryingBlockFetcher。RetryingBlockFetcher的原理是当请求失败后，会将请求丢到后台的线程继续尝试。