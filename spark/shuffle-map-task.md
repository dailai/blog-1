# Spark ShuffleMapTask 原理 #



ShuffleMapTask 返回结果 MapStatus， 结果会被发送给Driver。最后保存在MapOutputTrackerMaster。

```mermaid
sequenceDiagram
    CoarseGrainedExecutorBackend ->>+ DriverEndpoint : send(StatusUpdate)
    DriverEndpoint ->>+ TaskSchedulerImpl : statusUpdate
    TaskSchedulerImpl ->>+ TaskResultGetter : enqueueSuccessfulTask
    TaskResultGetter ->>+ TaskSchedulerImpl : handleSuccessfulTask
    TaskSchedulerImpl ->>+ TaskSetManager : handleSuccessfulTask
    TaskSetManager ->>+ DagScheduler : taskEnded
    DagScheduler ->>+ DAGSchedulerEventProcessLoop : post(CompletionEvent)
    DAGSchedulerEventProcessLoop ->>+ DagScheduler : handleTaskCompletion
    DagScheduler ->>+ MapOutputTrackerMaster : registerMapOutputs
    
    MapOutputTrackerMaster -->>- DagScheduler : 
    DagScheduler -->>- DAGSchedulerEventProcessLoop : 
    DAGSchedulerEventProcessLoop -->>- DagScheduler  : 
    DagScheduler -->>- TaskSetManager  : 
    TaskSetManager -->>- TaskSchedulerImpl : 
    TaskSchedulerImpl -->>- TaskResultGetter : 
    TaskResultGetter -->>- TaskSchedulerImpl : 
    TaskSchedulerImpl -->>- DriverEndpoint : 
    DriverEndpoint -->>- CoarseGrainedExecutorBackend : #
```



MapOutputTracker 对于shuffle启动重要的沟通作用。shuffle的过程分为writer和reader两块。

shuffle writer会将数据保存到Block里面，然后将数据的位置发送给MapOutputTracker。

shuffle reader通过向 MapOutputTracker获取shuffle writer的数据位置之后，才能读取到数据。





{% plantuml %}

@startuml

abstract class MapOutputTracker

MapOutputTracker <|-- MapOutputTrackerMaster
MapOutputTracker <|-- MapOutputTrackerWorker
MapOutputTrackerMaster -- MapOutputTrackerMasterEndpoint


@enduml

{% plantuml %}



MapOutputTrackerMaster是运行在driver节点上的，它管理着shuffle的输出信息。

MapOutputTrackerWorker是运行在executor节点上的，它会向driver请求shuffle输出的信息。

MapOutputTrackerMasterEndpoint是运行在driver节点上的Rpc服务，提供shuffle信息的获取。





## MapOutputTrackerMaster ##

MapOutputTrackerMaster管理所有shuffle的数据输出位置，

mapStatuses， 类型 ConcurrentHashMap[Int, Array[MapStatus]]， Key为shuffleId， Value为该shuffle的MapStatus列表

shuffleIdLocks， 类型为ConcurrentHashMap[Int, AnyRef]， Key为shuffleId， Value为普通的Object实例，仅仅作为锁存在。

提供接口新增shuffle

```scala
def registerShuffle(shuffleId: Int, numMaps: Int) {
  if (mapStatuses.put(shuffleId, new Array[MapStatus](numMaps)).isDefined) {
    throw new IllegalArgumentException("Shuffle ID " + shuffleId + " registered twice")
  }
  // 
  shuffleIdLocks.putIfAbsent(shuffleId, new Object())
}
```

新增MapStatus

```scala
def registerMapOutputs(shuffleId: Int, statuses: Array[MapStatus], changeEpoch: Boolean = false) {
  mapStatuses.put(shuffleId, statuses.clone())
  if (changeEpoch) {
    incrementEpoch()
  }
}
```



MapOutputTrackerMaster有一个队列，存储着获取MapStatus的请求。MapOutputTrackerMasterEndpoint在收到请求后，会将请求添加到这个队列里。MapOutputTrackerMaster还有着一个线程池，来处理队列的消息。

```scala
class MapOutputTrackerMaster {

  // 请求队列
  private val mapOutputRequests = new LinkedBlockingQueue[GetMapOutputMessage]

  // 处理请求的线程池
  private val threadpool: ThreadPoolExecutor = {
    val numThreads = conf.getInt("spark.shuffle.mapOutput.dispatcher.numThreads", 8)
    val pool = ThreadUtils.newDaemonFixedThreadPool(numThreads, "map-output-dispatcher")
    for (i <- 0 until numThreads) {
      pool.execute(new MessageLoop)
    }
    pool
  }
  
  // MessageLoop线程，处理请求
  private class MessageLoop extends Runnable {
    override def run(): Unit = {
      try {
        while (true) {
          try {
            // 取出请求
            val data = mapOutputRequests.take()
             if (data == PoisonPill) {
              // Put PoisonPill back so that other MessageLoops can see it.
              mapOutputRequests.offer(PoisonPill)
              return
            }
            val context = data.context
            val shuffleId = data.shuffleId

            // 获取该shuffle对应的MapStatus
            val mapOutputStatuses = getSerializedMapOutputStatuses(shuffleId)
            // 返回结果给rpc客户端
            context.reply(mapOutputStatuses)
          } catch {
            case NonFatal(e) => logError(e.getMessage, e)
          }
        }
      } catch {
        case ie: InterruptedException => // exit
      }
    }
  }
}
```



上面调用了getSerializedMapOutputStatuses函数，获取请求结果。它涉及到了缓存和序列化。这里缓存了每次的请求结果。对于缓存，必然涉及到缓存失效的问题。MapOutputTrackerMaster使用了epoch代表着数据的版本，cacheEpoch代表着缓存的版本。如果epoch 大于 cacheEpoch， 则表示缓存失效，需要重新获取。

```scala
private[spark] class MapOutputTrackerMaste {
  
  private var cacheEpoch = epoch
  
  // 缓存请求结果，注意到结果是Byte类型，是序列化之后的数据
  private val cachedSerializedStatuses = new ConcurrentHashMap[Int, Array[Byte]]().asScala
  
  
  def getSerializedMapOutputStatuses(shuffleId: Int): Array[Byte] = {
    // 该shuffleId 对应的 MapStatus列表
    var statuses: Array[MapStatus] = null
    // 序列化的结果
    var retBytes: Array[Byte] = null
    var epochGotten: Long = -1

    // 检查是否缓存有效，如果有效则返回true，并且设置retBytes的值
    // 如果失效，则返回false
    def checkCachedStatuses(): Boolean = {
      epochLock.synchronized {
        // 如果缓存失效，则清除缓存，并且保持cacheEpoch与epoch一致
        if (epoch > cacheEpoch) {
          cachedSerializedStatuses.clear()
          clearCachedBroadcast()
          cacheEpoch = epoch
        }
        
        cachedSerializedStatuses.get(shuffleId) match {
          // 如果有对应的shuffle缓存，则返回true，设置retBytes的值
          case Some(bytes) =>
            retBytes = bytes
            true
            
          // 如果没有对应的shuffle缓存，则从mapStatuses取出MapStatues
          case None =>
            logDebug("cached status not found for : " + shuffleId)
            statuses = mapStatuses.getOrElse(shuffleId, Array.empty[MapStatus])
            epochGotten = epoch
            false
        }
      }
    }
      
    // 调用checkCachedStatuses， 查看是否有缓存
    if (checkCachedStatuses()) return retBytes  
    
    // 序列化MapStatues列表
    var shuffleIdLock = shuffleIdLocks.get(shuffleId)
      shuffleIdLock.synchronized {
         if (checkCachedStatuses()) return retBytes
         // 序列化
         val (bytes, bcast) = MapOutputTracker.serializeMapStatuses(statuses, broadcastManager,
             isLocal, minSizeForBroadcast)
         // 更新缓存
         epochLock.synchronized {
           if (epoch == epochGotten) {
              cachedSerializedStatuses(shuffleId) = bytes
              if (null != bcast) cachedSerializedBroadcast(shuffleId) = bcast
           } else {
              logInfo("Epoch changed, not caching!")
              removeBroadcast(bcast)
          }
      }
     // 返回序列化的结果
     bytes
   }
}
```



查看Executor节点如何获取shuffle的输出数据信息

MapOutputTrackerWorker继承MapOutputTracker， 它提供了getStatuses方法，获取shuffle的MapStatus。

mapStatuses属性在MapOutputTrackerWorker， 表示executor节点的缓存。



getStatuses优先去从本地缓存mapStatuses获取，如果没有，则发送GetMapOutputStatuses请求给driver。

```scala
// 正在请求的shuffleId集合
private val fetching = new HashSet[Int]

private def getStatuses(shuffleId: Int): Array[MapStatus] = {
    
  val statuses = mapStatuses.get(shuffleId).orNull
  // 如果mapStatuses没有shuffleId的数据，则会向dirver请求
  if (statuses == null) {
    var fetchedStatuses: Array[MapStatus] = null
    fetching.synchronized {
      // 如果已经在请求该shuffle的MapStatus
      while (fetching.contains(shuffleId)) {
        try {
          // 等待通知
          fetching.wait()
        } catch {
          case e: InterruptedException =>
        }
      }

      // 判断shuffleId的结果是否已经获取到了
      fetchedStatuses = mapStatuses.get(shuffleId).orNull
      if (fetchedStatuses == null) {
        // 没有获取到，则添加到fetching集合
        fetching += shuffleId
      }
    }

    if (fetchedStatuses == null) {
     
      try {
        // askTracker方法里，会去调用Rpc的客户端，发送GetMapOutputStatuses请求
        val fetchedBytes = askTracker[Array[Byte]](GetMapOutputStatuses(shuffleId))
        // 将字节反序列化，得到MapStatus列表
        fetchedStatuses = MapOutputTracker.deserializeMapStatuses(fetchedBytes)
        // 将结果添加到mapStatuses
        mapStatuses.put(shuffleId, fetchedStatuses)
      } finally {
        fetching.synchronized {
          // 通知此shuffleId请求完成
          fetching -= shuffleId
          fetching.notifyAll()
        }
      }
    }

    if (fetchedStatuses != null) {
      return fetchedStatuses
    } else {
      // 如果请求结果，没有该shuffle的输出数据，则抛出异常
      logError("Missing all output locations for shuffle " + shuffleId)
      throw new MetadataFetchFailedException(
        shuffleId, -1, "Missing all output locations for shuffle " + shuffleId)
    }
  } else {
    return statuses
  }
}
```







