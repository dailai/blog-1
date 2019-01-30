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

```scala
private[streaming] class BlockGenerator(

  private val blockIntervalMs = conf.getTimeAsMs("spark.streaming.blockInterval", "200ms")

  private val blockIntervalTimer =
    new RecurringTimer(clock, blockIntervalMs, updateCurrentBuffer, "BlockGenerator")
  
  private def updateCurrentBuffer(time: Long): Unit = {
    try {
      var newBlock: Block = null
      synchronized {
        if (currentBuffer.nonEmpty) {
          val newBlockBuffer = currentBuffer
          currentBuffer = new ArrayBuffer[Any]
          val blockId = StreamBlockId(receiverId, time - blockIntervalMs)
          listener.onGenerateBlock(blockId)
          newBlock = new Block(blockId, newBlockBuffer)
        }
      }

      if (newBlock != null) {
        blocksForPushing.put(newBlock)  // put is blocking when queue is full
      }
    } catch {
      case ie: InterruptedException =>
        logInfo("Block updating timer thread was interrupted")
      case e: Exception =>
        reportError("Error in block updating thread", e)
    }
  }
    
```

## 发送Block





