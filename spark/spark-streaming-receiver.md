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



## driver端 ##

上面 executor 端会将任务发送给 driver 端的 ReceiverTrackerEndpoint。ReceiverTrackerEndpoint是Rpc服务，负责Receiver的管理，包括启动Receiver和接收Receiver发送过来的Block。



## Receiver启动 ##

ReceiverTracker首先从DStreamGraph中获取所有的ReceiverInputDStream，然后取得它的Receiver。这样就得到了Receiver列表，然后为每一个Receiver分配一个Executor，运行ReceiverSupervisorImpl服务。这里运行的Executor是一直占用的，直到整个spark streaming的任务停止。

首先来看下是怎么分配receiver的运行位置。分配算法由ReceiverSchedulingPolicy类负责，原理如下

首先介绍三个变量：

* hostToExecutors， 每个host对应的executor列表
* scheduledLocations， 每个recevier对应的TaskLocation列表
* numReceiversOnExecutor， 每个executor可能执行receiver的数目

再介绍分配算法：

1. 首先根据 receiver 指定的 host 位置，从该 host 的executor 列表中，找到对应 receiver 数目最少的那个，分配给这个 receiver
2. 将剩下没有指定位置的 receiver，从所有 executor 中，找到对应 receiver 数目最少的那个，分配给这个 receiver
3. 如果还有空闲的executor，那么从 receiver 列表中，找到分配 executor 数目最少的那个receiver，然后将这个空闲executor分配给receiver

代码如下：

```scala
def scheduleReceivers(
    receivers: Seq[Receiver[_]],
    executors: Seq[ExecutorCacheTaskLocation]): Map[Int, Seq[TaskLocation]] = {
  // 每个host对应的executor列表
  val hostToExecutors = executors.groupBy(_.host)
  // 每个recevier对应的TaskLocation列表
  val scheduledLocations = Array.fill(receivers.length)(new mutable.ArrayBuffer[TaskLocation])
  // 每个executor可能执行receiver的数目, 初始值为0
  val numReceiversOnExecutor = mutable.HashMap[ExecutorCacheTaskLocation, Int]()
  executors.foreach(e => numReceiversOnExecutor(e) = 0)
  
  // 遍历receivers列表
  for (i <- 0 until receivers.length) {
    // 如果该receiver指定了位置，那么提取所在位置的host
    receivers(i).preferredLocation.foreach { host =>
      hostToExecutors.get(host) match {
        case Some(executorsOnHost) =>
          // 从该host中寻找分配receiver数目最少的那个executor
          val leastScheduledExecutor =
            executorsOnHost.minBy(executor => numReceiversOnExecutor(executor))
          // 更新scheduledLocations集合
          scheduledLocations(i) += leastScheduledExecutor
          // 更新numReceiversOnExecutor集合
          numReceiversOnExecutor(leastScheduledExecutor) =
            numReceiversOnExecutor(leastScheduledExecutor) + 1
        case None =>
          // preferredLocation is an unknown host.
          // Note: There are two cases:
          // 1. This executor is not up. But it may be up later.
          // 2. This executor is dead, or it's not a host in the cluster.
          // Currently, simply add host to the scheduled executors.

          // Note: host could be `HDFSCacheTaskLocation`, so use `TaskLocation.apply` to handle
          // this case
          scheduledLocations(i) += TaskLocation(host)
      }
    }
  }

  // 遍历那些没有指定位置的receiver
  for (scheduledLocationsForOneReceiver <- scheduledLocations.filter(_.isEmpty)) {
    // 从executor列表中挑选出，分配receiver数目最小的executor
    val (leastScheduledExecutor, numReceivers) = numReceiversOnExecutor.minBy(_._2)
    // 更新scheduledLocations集合
    scheduledLocationsForOneReceiver += leastScheduledExecutor
    // 更新numReceiversOnExecutor集合
    numReceiversOnExecutor(leastScheduledExecutor) = numReceivers + 1
  }

  // 如果还有空闲的executor
  val idleExecutors = numReceiversOnExecutor.filter(_._2 == 0).map(_._1)
  for (executor <- idleExecutors) {
    // 选择出分配executor数目最少的receiver
    val leastScheduledExecutors = scheduledLocations.minBy(_.size)
    // 将这个空闲executor分配给这个receiver
    leastScheduledExecutors += executor
  }
  // 返回 InputDStream 对应 TaskLocaltion的列表
  receivers.map(_.streamId).zip(scheduledLocations).toMap
}
```

获取到receiver的分配位置后，ReceiverTracker的startReceiver方法，负责启动单个Receiver，代码简化如下

```scala
private def startReceiver(
    receiver: Receiver[_],
    scheduledLocations: Seq[TaskLocation]): Unit = {
  
  // 定义Executor端的运行函数
  val startReceiverFunc: Iterator[Receiver[_]] => Unit =
    (iterator: Iterator[Receiver[_]]) => {
        // 取出receiver，这里的rdd只包含一个receiver
        val receiver = iterator.next()
        assert(iterator.hasNext == false)
        // 运行ReceiverSupervisorImpl服务
        val supervisor = new ReceiverSupervisorImpl(
          receiver, SparkEnv.get, serializableHadoopConf.value, checkpointDirOption)
        supervisor.start()
        // 等待服务停止
        supervisor.awaitTermination()
    }

  // 将 receiver 转换为 RDD
  val receiverRDD: RDD[Receiver[_]] =
    if (scheduledLocations.isEmpty) {
      // 如果没有指定Executor位置，则随机分配
      ssc.sc.makeRDD(Seq(receiver), 1)
    } else {
      // 如果指定Executor位置，则传递给makeRDD函数
      val preferredLocations = scheduledLocations.map(_.toString).distinct
      ssc.sc.makeRDD(Seq(receiver -> preferredLocations))
    }
  // 设置RDD的名称
  receiverRDD.setName(s"Receiver $receiverId")
  // 调用sparkContext的方法，提交任务。这里传递了receiverRDD和执行函数startReceiverFunc
  val future = ssc.sparkContext.submitJob[Receiver[_], Unit, Unit](
    receiverRDD, startReceiverFunc, Seq(0), (_, _) => Unit, ())
  // 执行回调函数
  future.onComplete {
    ......
  }(ThreadUtils.sameThread)
}
```



## 接收 Block ##

ReceiverTrackerEndpoint负责处理AddBlock请求





ReceivedBlockTracker负责管理来自输入流的数据。

它有两个重要变量：

* streamIdToUnallocatedBlockQueues， 保存所有InputDStream对应的还未分配的数据
* timeToAllocatedBlocks， 保存了已分配的数据

当ReceiverTrackerEndpoint收到AddBlock请求， 会调用ReceivedBlockTracker的addBlock方法添加数据。

当spark streaming需要从这儿分配到数据， 才能提交Job。allocateBlocksToBatch方法负责分配数据。

```scala
private[streaming] class ReceivedBlockTracker(
    conf: SparkConf,
    hadoopConf: Configuration,
    streamIds: Seq[Int],         // 所有InputDStream的id
    clock: Clock,
    recoverFromWriteAheadLog: Boolean,
    checkpointDirOption: Option[String])
  extends Logging {
  
  private type ReceivedBlockQueue = mutable.Queue[ReceivedBlockInfo]
  
  // key为InputDStream的id， Value为对应的Block队列
  private val streamIdToUnallocatedBlockQueues = new mutable.HashMap[Int, ReceivedBlockQueue]
  // Key为数据批次的时间，Value为分配的Blocks集合
  private val timeToAllocatedBlocks = new mutable.HashMap[Time, AllocatedBlocks]
  
  // 获取对应InputDStream的未分配的Block队列
  private def getReceivedBlockQueue(streamId: Int): ReceivedBlockQueue = {
    streamIdToUnallocatedBlockQueues.getOrElseUpdate(streamId, new ReceivedBlockQueue)
  }
      
  //添加Block
  def addBlock(receivedBlockInfo: ReceivedBlockInfo): Boolean = {
    // 如果配置了wal，则写入wal日志
    val writeResult = writeToLog(BlockAdditionEvent(receivedBlockInfo))
    if (writeResult) {
      synchronized {
        // 添加Block信息到streamIdToUnallocatedBlockQueues集合
        getReceivedBlockQueue(receivedBlockInfo.streamId) += receivedBlockInfo
      }
    }
    writeResult
  }

  // 分配Block
  def allocateBlocksToBatch(batchTime: Time): Unit = synchronized {
    // 检测batchTime的时间必须大于上一次分配的时间
    if (lastAllocatedBatchTime == null || batchTime > lastAllocatedBatchTime) {
      // 获取所有InputDStream的待分配数据
      val streamIdToBlocks = streamIds.map { streamId =>
          (streamId, getReceivedBlockQueue(streamId).dequeueAll(x => true))
      }.toMap
      //实例化AllocatedBlocks
      val allocatedBlocks = AllocatedBlocks(streamIdToBlocks)
      if (writeToLog(BatchAllocationEvent(batchTime, allocatedBlocks))) {
        // 将分配的数据，保存到timeToAllocatedBlocks集合
        timeToAllocatedBlocks.put(batchTime, allocatedBlocks)
        // 更新最后一次分配时间
        lastAllocatedBatchTime = batchTime
      } else {
        logInfo(s"Possibly processed batch $batchTime needs to be processed again in WAL recovery")
      }
    } else {
      logInfo(s"Possibly processed batch $batchTime needs to be processed again in WAL recovery")
    }
  }  
  
```





