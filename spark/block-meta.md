# Block 元信息 #



## BlockManagerId ##

BlockManagerId包含下列信息：

- executorId
- host
- port
- topology，　网络拓扑信息

## BlockId ##

BlockId对于不同类型的Block，对应着不同的子类。

### RDDBlockId ###

字段信息：

* rddId
* splitIndex

### ShuffleBlockId ###

字段信息：

* shuffleId
* mapId
* reduceId

### ShuffleDataBlockId ###

同 ShuffleBlockId

### ShuffleIndexBlockId ###

字段信息：

* shuffleId
* mapId
* reduceId

### BroadcastBlockId ###

字段信息：

* broadcastId
* field

### TaskResultBlockId ###

字段信息：

* taskId

### StreamBlockId ###

字段信息：

* streamId
* uniqueId



## BlockInfo ##

 字段信息：

* StorageLevel level， 表明底层存储的类型
* size， 数据大小
* readerCount， reader的数目
* writerTask， 是否有Task正在写入



## BlockInfoManager ##

 字段信息：

* HashMap[BlockId, BlockInfo] infos
* MultiMap[TaskAttemptId, BlockId]  writeLocksByTask
* HashMap[TaskAttemptId, ConcurrentHashMultiset[BlockId]]  readLocksByTask

BlockInfoManager提供了BlockInfo的读写锁。调用lockForWriting获取写锁，调用lockForReading获取读锁，调用unlock释放锁。

## BlockManagerMaster ##

BlockManagerMaster负责和driver通信，

## BlockManagerSlaveEndpoint ##

负责接收master的命令，比如删除block， rdd， shuffle，获取block的信息



## BlockManager ##

字段信息：

* BlockManagerMaster master
* BlockInfoManager blockInfoManager
* MemoryStore memoryStore
* DiskStore diskStore
* BlockManagerSlaveEndpoint slaveEndpoint   shuffleClient

BlockManager

### 向block写入数据 ###

```scala
private[spark] class BlockManager {
  override def putBlockData(
      blockId: BlockId,
      data: ManagedBuffer,
      level: StorageLevel,
      classTag: ClassTag[_]): Boolean = {
    putBytes(blockId, new ChunkedByteBuffer(data.nioByteBuffer()), level)(classTag)
  }
    
  def putBytes[T: ClassTag](
      blockId: BlockId,
      bytes: ChunkedByteBuffer,
      level: StorageLevel,
      tellMaster: Boolean = true): Boolean = {
    require(bytes != null, "Bytes is null")
    doPutBytes(blockId, bytes, level, implicitly[ClassTag[T]], tellMaster)
  }
}
```

