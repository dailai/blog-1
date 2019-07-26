## Shuffle 位置获取

当读取Shuffle 的数据之前，需要先MapOutputTracker获取数据所在的位置，然后才会读取数据。











## ShuffleBlockFetcherIterator

首先根据 shuffle 数据所在的位置，分为本地数据和远程数据。本地数据直接从文件中即可读取，而远程数据需要通过网络传输。



### 生成请求



maxBytesInFlight 表示最大量



至于远程的shuffle数据，首先根据所在位置进行分组。然后将组里的数据，根据大小进一步分组。一个请求包含了多份 shuffle 数据，这个请求的数据总和，基本控制在不大于 1 / maxBytesInFlight，但也有例外的情况，

```scala
val targetRequestSize = math.max(maxBytesInFlight / 5, 1L)
val remoteRequests = new ArrayBuffer[FetchRequest]
// blocksByAddress 根据地址分组，存储着shuffle数据列表
for ((address, blockInfos) <- blocksByAddress) {
    val iterator = blockInfos.iterator
    var curRequestSize = 0L
    var curBlocks = new ArrayBuffer[(BlockId, Long)]
    while (iterator.hasNext) {
        // 遍历shuffle数据大小，直到累积和大于targetRequestSize
        curBlocks += ((blockId, size))
        curRequestSize += size
        if (curRequestSize >= targetRequestSize) {
            // 累积的数据足够大了，那么就生成一个请求
            // 但是可能发生一种情况，curRequestSize 略小于 targetRequestSize，并且新加的这份数据相当大，那么就会造成此次请求包含的数据会特别大
            remoteRequests += new FetchRequest(address, curBlocks)
            curBlocks = new ArrayBuffer[(BlockId, Long)]
            curRequestSize = 0
        }
    
    if (curBlocks.nonEmpty) {
        remoteRequests += new FetchRequest(address, curBlocks)
    }
```



### 并发请求

上面已经生成了请求，现在如何将其高效率的发送出去。通常我们都是采用异步的方式，spark 也是基于 Netty 来实现异步传输的。但是同时 spark 在此基础上，还实现了并发的限制，防止占用过大的资源。

正在发送的请求数，不能超过指定数量，由 spark.reducer.maxReqsInFlight 配置表示，默认 Int.MaxValue，可以认为无限制。

正在请求的数据总和，不能超过指定数量，由spark.reducer.maxSizeInFlight 配置表示，默认为 48MB。

对于单次请求的数据过大时，spark 会使用 stream 模式请求，也就是将数据分块下载，存储到文件里，每个文件对应着一份shuffle数据。对于数据较小的情况，spark 会将数据全部存储到内存里。这个阈值由spark.reducer.maxReqSizeShuffleToMem 配置指定，比较奇怪的是默认值为Long.MaxValue，可以认为是无限大。如果发生了shuffle 倾斜，这就很容易造成内存溢出了。





## ShuffleClient

ShuffleClient表示shuffle 数据的客户端，支持远程读取数据。

BlockTransferService继承ShuffleClient，增加了上传数据。

NettyBlockTransferService继承BlockTransferService， 实现了所有的接口。

ExternalShuffleClient 实现。。。。





## OneForOneBlockFetcher







## Netty TransportClient

netty in pipeline























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

