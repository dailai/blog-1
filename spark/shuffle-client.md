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





## ShuffleClient 类图

%plantuml%

@startuml
abstract class ShuffleClient
class BlockTransferService
class NettyBlockTransferService
class ExternalShuffleClient
class MesosExternalShuffleClient

ShuffleClient <|-- BlockTransferService
BlockTransferService <|-- NettyBlockTransferService
ShuffleClient <|-- ExternalShuffleClient
ExternalShuffleClient <|-- MesosExternalShuffleClient

@enduml

%plantuml%

ShuffleClient表示shuffle 数据的客户端，支持远程读取数据。

BlockTransferService继承ShuffleClient，增加了上传数据。

NettyBlockTransferService继承BlockTransferService， 实现了所有的接口。

ExternalShuffleClient 实现。。。。





## OneForOneBlockFetcher

NettyBlockTransferService 使用 OneForOneBlockFetcher 远程获取shuffle数据。

首先发送 OpenBlocks rpc 请求，

```java
public class OpenBlocks extends BlockTransferMessage {
  public final String appId; // 
  public final String execId;  // executor id
  public final String[] blockIds; // 获取的block id列表
}
```

响应 StreamHandle

```java
public class StreamHandle extends BlockTransferMessage {
  public final long streamId; // 分配的stream id
  public final int numChunks; // 多少块 
}
```



如果指定了文件名，那么表示接收的数据需要存储到文件。所以会以 stream 的方式请求数据。

否则就采用fetch的方式请求数据，数据会先存到内存里。如果数据过大，就会造成内存溢出。

```java
private class DownloadCallback implements StreamCallback {
    private WritableByteChannel channel = null;
    
    // 只要接收到数据，就会立马写入到文件中
    public void onData(String streamId, ByteBuffer buf) throws IOException {
        channel.write(buf);
    }
    
    // 获取数据完成后，会通知listener
    public void onComplete(String streamId) throws IOException {
        channel.close();
        // 基于文件封装的buffer
        ManagedBuffer buffer = new FileSegmentManagedBuffer(transportConf, targetFile, 0, targetFile.length());
        // 执行listener回调函数
        listener.onBlockFetchSuccess(blockIds[chunkIndex], buffer);
    }
}
```



```java
private class ChunkCallback implements ChunkReceivedCallback {
    // buffer是netty分配的堆外内存
    public void onSuccess(int chunkIndex, ManagedBuffer buffer) {
        listener.onBlockFetchSuccess(blockIds[chunkIndex], buffer);
    }
}
```





## NettyBlockTransferService 启动服务 ##

NettyBlockTransferService 类不仅作实现了客户端的接口，同样它还负责创建服务端的实例。服务端是基于 netty 框架实现的，它的核心处理 由 NettyBlockRpcServer 类负责。



注意到NettyBlockRpcServer，它继承了RpcHandler，实现了处理请求的逻辑。NettyBlockRpcServer只接收OpenBlocks和UploadBlock请求。

```scala
class NettyBlockRpcServer(appId: String, serializer: Serializer, blockManager: BlockDataManager) extends RpcHandler with Logging {
    override def receive(
      client: TransportClient, rpcMessage: ByteBuffer, responseContext: RpcResponseCallback): Unit = {
    	val message = BlockTransferMessage.Decoder.fromByteBuffer(rpcMessage)
    	logTrace(s"Received request: $message")

    	message match {
            case openBlocks: OpenBlocks =>
                val blocksNum = openBlocks.blockIds.length
                // 针对每个blockId，生成FileSegmentManagedBuffer
                val blocks = for (i <- (0 until blocksNum).view)
                  yield blockManager.getBlockData(BlockId.apply(openBlocks.blockIds(i)))
            	// 生成streamId，并且在StreamManager注册
            	val streamId = streamManager.registerStream(appId, blocks.iterator.asJava)
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





## StreamManager

StreamManager 有两个子类，NettyStreamManager 和 OneForOneStreamManager。

NettyStreamManager 用于传输配置文件或 jar 包。

目前 NettyBlockRpcServer 使用的是 OneForOneStreamManager，负责shuffle 数据的服务端。

```java
public class OneForOneStreamManager extends StreamManager {
  // 用来生成递增唯一的streamId
  private final AtomicLong nextStreamId;
  // 根据streamId 找到对应的数据，数据由StreamState表示
  private final ConcurrentHashMap<Long, StreamState> streams;
  
  // 注册stream数据
  public long registerStream(String appId, Iterator<ManagedBuffer> buffers) {
    long myStreamId = nextStreamId.getAndIncrement();
    streams.put(myStreamId, new StreamState(appId, buffers));
    return myStreamId;
  }
}
```





回到TransportRequestHandler，它处理ChunkFetchRequest请求时，调用了StreamManager 的 getChunk 方法。在处理 StreamRequest请求时，调用了StreamManager 的 openStream 方法。













## 客户请求 ##

### 读取请求 ###

NettyBlockTransferService也实现了客户端的接口。fetchBlocks负责读取远端 Block。这里请求支持失败重试，这里涉及到了RetryingBlockFetcher。RetryingBlockFetcher的原理是当请求失败后，会将请求丢到后台的线程继续尝试。



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



### 写请求 ###

uploadBlock方法实现了请求写Block，它简单的调用了client.sendRpc，发送UploadBlock请求消息。

