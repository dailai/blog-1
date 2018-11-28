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

### 读取请求 ###

NettyBlockTransferService也实现了客户端的接口。fetchBlocks负责读取远端 Block。这里请求支持失败重试，这里涉及到了RetryingBlockFetcher。RetryingBlockFetcher的原理是当请求失败后，会将请求丢到后台的线程继续尝试。

```java
public class RetryingBlockFetcher {
    // 实际获取Block的客户端
    private final BlockFetchStarter fetchStarter;
    // 后台重试的线程
    private static final ExecutorService executorService = Executors.newCachedThreadPool(
    NettyUtils.createThreadFactory("Block Fetch Retry"));
    
    public void start() {
    	fetchAllOutstanding();
  	}
    
    private void fetchAllOutstanding() {
        
        try {
            // 请求Block数据
      		fetchStarter.createAndStart(blockIdsToFetch, myListener);
    	} catch (Exception e) {
            // 判断是否能够重试
            if (shouldRetry(e)) {
                // 失败重试
        		initiateRetry();
      		} else {
                // 调用失败函数
        		for (String bid : blockIdsToFetch) {
          			listener.onBlockFetchFailure(bid, e);
        		}
      		}
        }
    }
    
    private synchronized void initiateRetry() {
    	retryCount += 1;
    	currentListener = new RetryingBlockFetchListener();
        // 向后台线程，提交fetchAllOutstanding的任务
        executorService.submit(() -> {
        	Uninterruptibles.sleepUninterruptibly(retryWaitTime, TimeUnit.MILLISECONDS);
        	fetchAllOutstanding();
    	});
    }
        
```



再回到NettyBlockTransferService的fetchBlocks方法, 里面定了BlockFetchStarter类，它表示实际请求的程序。

blockFetchStarter实例化了TransportClient, 然后调用通过OneForOneBlockFetcher来请求数据。

```
val blockFetchStarter = new RetryingBlockFetcher.BlockFetchStarter {
  override def createAndStart(blockIds: Array[String], listener: BlockFetchingListener) {
    val client = clientFactory.createClient(host, port)
    new OneForOneBlockFetcher(client, appId, execId, blockIds.toArray, listener,
      transportConf, shuffleFiles).start()
  }
```

OneForOneBlockFetcher，首先根据BlockIds生成请求OpenBlocks，调用client的sendRpc发送请求。收到的响应消息是StreamHandle，它包含了streamId，和ChunkId列表。然后客户端会再去请求Chunk的数据。如果shuffleFiles不为空，表示这些数据都要存到文件里，这里client调用stream方法。否则，这些数据会存到内存里，这里client调用fetchChunk方法。

```java
public class OneForOneBlockFetcher {
    
    private final OpenBlocks openMessage;
    
    public void start() {
        client.sendRpc(openMessage.toByteBuffer(), new RpcResponseCallback() {
            @Override
      		public void onSuccess(ByteBuffer response) {
                streamHandle = (StreamHandle) BlockTransferMessage.Decoder.fromByteBuffer(response);
                for (int i = 0; i < streamHandle.numChunks; i++) {
                    if (shuffleFiles != null) {
                        client.stream(OneForOneStreamManager.genStreamChunkId(streamHandle.streamId, i),
                                       new DownloadCallback(shuffleFiles[i], i));
                    }	else {
                        	client.fetchChunk(streamHandle.streamId, i, chunkCallback);
                    }
                   ...............
                }
            }
        }
                       
                    
                                      
```

### 写请求 ###

uploadBlock方法实现了请求写Block，它简单的调用了client.sendRpc，发送UploadBlock请求消息。

```scala
override def uploadBlock(....) {
    val result = Promise[Unit]()
    client.sendRpc(new UploadBlock(appId, execId, blockId.toString, metadata, array).toByteBuffer,
      new RpcResponseCallback {
        override def onSuccess(response: ByteBuffer): Unit = {
          logTrace(s"Successfully uploaded block $blockId")
          result.success((): Unit)
        }
        override def onFailure(e: Throwable): Unit = {
          logError(s"Error while uploading block $blockId", e)
          result.failure(e)
        }
      })
    result.future
}
```

