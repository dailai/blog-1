# Spark Streaming 数据源读取 #

  

Spark Streaming 支持多种数据源，数据源的读取都是由Receiver表示。

相关的uml图:

{% plantuml %}

@startuml

 

@enduml

{% endplantuml %}



Receiver会启动后台线程，持续的拉取数据，发送给 ReceiverSupervisor。ReceiverSupervisor会将数转发给BlockGenerator。



BlockGenerator 接收数据后，会先缓存起来，每隔一段时间，就会将接收的数据封装成一个Block，然后发送出去。



## 接收消息 ##

Receiver提供了store方法添加单条数据

```scala
abstract class Receiver[T](val storageLevel: StorageLevel) extends Serializable {

  def store(dataItem: T) {
    supervisor.pushSingle(dataItem)
  }

}
```



ReceiverSupervisor是一个抽象类，ReceiverSupervisorImpl是它的唯一实现类。

```scala
private[streaming] class ReceiverSupervisorImpl {
    
    def pushSingle(data: Any) {
       defaultBlockGenerator.addData(data)
    }
}
```

BlockGenerator

```scala
private[streaming] class BlockGenerator {
    
  @volatile private var currentBuffer = new ArrayBuffer[Any]

  def addData(data: Any): Unit = {
    if (state == Active) {
      waitToPush()
      synchronized {
        if (state == Active) {
          currentBuffer += data
        } else {
          throw new SparkException(
            "Cannot add data as BlockGenerator has not been started or has been stopped")
        }
      }
    } else {
      throw new SparkException(
        "Cannot add data as BlockGenerator has not been started or has been stopped")
    }
  }
}
```



## 封装Block ##

数据发送给BlockGenerator后，BlockGenerator会有一个定时的后台线程，将接收的数据封装成Block。

RecurringTimer类是定时线程，下面的blockIntervalMs参数表示间隔时间，updateCurrentBuffer参数表示执行函数。

updateCurrentBuffer会将数据封装成Block，然后发送给队列。

```scala
private[streaming] class BlockGenerator(
  
  // 获取时间间隔
  private val blockIntervalMs = conf.getTimeAsMs("spark.streaming.blockInterval", "200ms")
  // 实例化一个定时器
  private val blockIntervalTimer =
    new RecurringTimer(clock, blockIntervalMs, updateCurrentBuffer, "BlockGenerator")
  // 数据缓存数组
  private var currentBuffer = new ArrayBuffer[Any]
  
  // Block的队列大小  
  private val blockQueueSize = conf.getInt("spark.streaming.blockQueueSize", 10)
  //存储Block的队列
  private val blocksForPushing = new ArrayBlockingQueue[Block](blockQueueSize)
  
  private def updateCurrentBuffer(time: Long): Unit = {
    try {
      var newBlock: Block = null
      // 使用synchronized防止线程竞争
      synchronized {
        if (currentBuffer.nonEmpty) {
          // 将数据保存到 newBlockBuffer
          val newBlockBuffer = currentBuffer
          // 重置 currentBuffer 为空数组
          currentBuffer = new ArrayBuffer[Any]
          // 生成StreamBlock的Id
          val blockId = StreamBlockId(receiverId, time - blockIntervalMs)
          listener.onGenerateBlock(blockId)
          //生成Block
          newBlock = new Block(blockId, newBlockBuffer)
        }
      }

      if (newBlock != null) {
        // 添加到队列里
        blocksForPushing.put(newBlock)  // put is blocking when queue is full
      }
    } catch {
      case ie: InterruptedException =>
        logInfo("Block updating timer thread was interrupted")
      case e: Exception =>
        reportError("Error in block updating thread", e)
    }
  }
}
```



## 发送Block

BlockGenerator会有一个后台线程，专门负责将Block发送出去。

```scala
private[streaming] class BlockGenerator {
    
  private val blockPushingThread = new Thread() { override def run() { keepPushingBlocks() } }
    
  private def keepPushingBlocks() {

    def areBlocksBeingGenerated: Boolean = synchronized {
      state != StoppedGeneratingBlocks
    }

    try {
      
      while (areBlocksBeingGenerated) {
        // 从队列里获取block
        Option(blocksForPushing.poll(10, TimeUnit.MILLISECONDS)) match {
          // 调用pushBlock方法发送block
          case Some(block) => pushBlock(block)
          case None =>
        }
      }

      // 如果BlockGenerator停止了，则发送队列中剩余的block
      logInfo("Pushing out the last " + blocksForPushing.size() + " blocks")
      while (!blocksForPushing.isEmpty) {
        val block = blocksForPushing.take()
        logDebug(s"Pushing block $block")
        pushBlock(block)
        logInfo("Blocks left to push " + blocksForPushing.size())
      }
      logInfo("Stopped block pushing thread")
    } catch {
      case ie: InterruptedException =>
        logInfo("Block pushing thread was interrupted")
      case e: Exception =>
        reportError("Error in block pushing thread", e)
    }
  }
  
  // pushBlock调用了listener的回调函数
  private def pushBlock(block: Block) {
    listener.onPushBlock(block.id, block.buffer)
    logInfo("Pushed block " + block.id)
  }
}
```



上面的pushBlock调用了listener的方法，listener是在BlockGenerator初始化的时候，会指定参数。BlockGenerator的初始化是在ReceiverSupervisorImpl类。

```scala
private[streaming] class ReceiverSupervisorImpl
  // BlockGeneratorListener的实现
  private val defaultBlockGeneratorListener = new BlockGeneratorListener {
    def onAddData(data: Any, metadata: Any): Unit = { }

    def onGenerateBlock(blockId: StreamBlockId): Unit = { }

    def onError(message: String, throwable: Throwable) {
      reportError(message, throwable)
    }

    def onPushBlock(blockId: StreamBlockId, arrayBuffer: ArrayBuffer[_]) {
      // 调用pushArrayBuffer发送block
      pushArrayBuffer(arrayBuffer, None, Some(blockId))
    }
  }

  def pushArrayBuffer(
      arrayBuffer: ArrayBuffer[_],
      metadataOption: Option[Any],
      blockIdOption: Option[StreamBlockId]
    ) {
    pushAndReportBlock(ArrayBufferBlock(arrayBuffer), metadataOption, blockIdOption)
  }

  def pushAndReportBlock(
      receivedBlock: ReceivedBlock,
      metadataOption: Option[Any],
      blockIdOption: Option[StreamBlockId]
    ) {
    val blockId = blockIdOption.getOrElse(nextBlockId)
    // 使用receivedBlockHandler保存block数据
    val blockStoreResult = receivedBlockHandler.storeBlock(blockId, receivedBlock)
    
    val numRecords = blockStoreResult.numRecords
    val blockInfo = ReceivedBlockInfo(streamId, numRecords, metadataOption, blockStoreResult)
    // 将保存结果的元信息发送给Driver端
    trackerEndpoint.askSync[Boolean](AddBlock(blockInfo))
  }
}
```