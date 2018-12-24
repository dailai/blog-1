# BlockManager #

## 字段信息 ##

- BlockManagerMaster master
- BlockInfoManager blockInfoManager
- MemoryStore memoryStore
- DiskStore diskStore
- BlockManagerSlaveEndpoint slaveEndpoint   shuffleClient

## block写数据 ##

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

doPut方法主要是封装BlockInfoManager的锁，为写Block提供了方便。

```scala
 def doPut[T](
      blockId: BlockId,
      level: StorageLevel,
      classTag: ClassTag[_],
      tellMaster: Boolean,
      keepReadLock: Boolean)(putBody: BlockInfo => Option[T]): Option[T] = {
   // 构造BlockInfo， 获取写锁
   val putBlockInfo = {
      val newInfo = new BlockInfo(level, classTag, tellMaster)
      if (blockInfoManager.lockNewBlockForWriting(blockId, newInfo)) {
        newInfo
      } else {
        logWarning(s"Block $blockId already exists on this machine; not re-adding it")
        if (!keepReadLock) {
          // lockNewBlockForWriting returned a read lock on the existing block, so we must free it:
          releaseLock(blockId)
        }
        return None
      }
    }
    // 执行写入Block数据
     val result: Option[T] = try {
      val res = putBody(putBlockInfo)
      exceptionWasThrown = false
      if (res.isEmpty) {
        // the block was successfully stored
        if (keepReadLock) {
          blockInfoManager.downgradeLock(blockId)
        } else {
          blockInfoManager.unlock(blockId)
        }
      } else {
        removeBlockInternal(blockId, tellMaster = false)
        logWarning(s"Putting block $blockId failed")
      }
 }
```

#### 写入字节数据

如果写入数据同时支持内存和磁盘，spark会优先写入内存。如果内存不够，则写入磁盘。

```scala
private def doPutBytes[T](
    blockId: BlockId,
    bytes: ChunkedByteBuffer,
    level: StorageLevel,
    classTag: ClassTag[T],
    tellMaster: Boolean = true,
    keepReadLock: Boolean = false): Boolean = {
  doPut(blockId, level, classTag, tellMaster = tellMaster, keepReadLock = keepReadLock) { info =>
      if (level.useMemory) {
        // 优先写入内存
        val putSucceeded = if (level.deserialized) {
          // 如果需要解析数据，存储对象，则调用dataDeserializeStream返回流
          val values =
            serializerManager.dataDeserializeStream(blockId, bytes.toInputStream())(classTag)
          // 将对象存储到memoryStore
          memoryStore.putIteratorAsValues(blockId, values, classTag) match {
            // 返回Right，表示成功
            case Right(_) => true
            // 返回Left，表示失败 
            case Left(iter) =>
              // 调用PartiallyUnrolledIterator的close方法，释放内存
              iter.close()
              false
          }
        } else {
          // 不需要解析数据，直接存储字节
          val memoryMode = level.memoryMode
          // 将数据存储到memoryStore
          memoryStore.putBytes(blockId, size, memoryMode, () => {
            // 如果是存储位置是非堆内存，这里会将堆内的数据，拷贝到非堆
            if (memoryMode == MemoryMode.OFF_HEAP &&
                bytes.chunks.exists(buffer => !buffer.isDirect)) {
              bytes.copy(Platform.allocateDirectBuffer)
            } else {
              bytes
            }
          })
        }
      if (!putSucceeded && level.useDisk) {
          // 如果内存写入失败，并且支持写入磁盘，则会尝试写入磁盘
          logWarning(s"Persisting block $blockId to disk instead.")
          diskStore.putBytes(blockId, bytes)
        }
          ....
                  
          
```

如果写入数据只支持磁盘，则直接写入diskstore

```
if (level.useDisk) {
  diskStore.putBytes(blockId, bytes)
}
```

#### 写入对象数据 ####

BlockManager提供了doPutIterator，支持写入对象。基本逻辑和doPutBytes差不多，只不过调用了memorystore的putIteratorAsValues和putIteratorAsBytes 方法。



## block读取数据 ##

如果写入数据同时支持内存和磁盘，spark会优先去内存中读取。如果内存中不存在，则从磁盘中读取。

#### 读取对象数据 ####

从内存中读取数据，首先获取读锁，注意到CompletionIterator，它封装了Iterator，提供了当遍历完的，执行回调函数。这里当数据读取完后，会释放读锁。

```scala
  def getLocalValues(blockId: BlockId): Option[BlockResult] = {
    // 获取读锁
    blockInfoManager.lockForReading(blockId) match {
      case None =>
        logDebug(s"Block $blockId was not found")
        None
      case Some(info) =>
        val level = info.level
        // 如果内存中存储了Block数据
        if (level.useMemory && memoryStore.contains(blockId)) {
            val iter: Iterator[Any] = if (level.deserialized) {
            // 从memoryStore读取对象
            memoryStore.getValues(blockId).get
          } else {
            // 从memoryStore读取字节
            serializerManager.dataDeserializeStream(
              blockId, memoryStore.getBytes(blockId).get.toInputStream())(info.classTag)
          }
          // 这里使用了CompletionIterator， 当遍历完时候，会调用releaseLock释放锁
          val ci = CompletionIterator[Any, Iterator[Any]](iter, {
            releaseLock(blockId, taskAttemptId)
          })
          Some(new BlockResult(ci, DataReadMethod.Memory, info.size))
```

如果内存中没有，则从磁盘读取，并且数据会尽量缓存到内存中。

```scala
if (level.useDisk && diskStore.contains(blockId)) {
    // 从diskStore读取数据
    val diskData = diskStore.getBytes(blockId)
    val iterToReturn: Iterator[Any] = {
        if (level.deserialized) {
         	// 解析数据
            val diskValues = serializerManager.dataDeserializeStream(
                blockId,
                diskData.toInputStream())(info.classTag)
            // 缓存数据到内存中
            maybeCacheDiskValuesInMemory(info, blockId, level, diskValues)
        } else {
            // 缓存字节数据到内存中
            val stream = maybeCacheDiskBytesInMemory(info, blockId, level, diskData)
            .map { _.toInputStream(dispose = false) }
            .getOrElse { diskData.toInputStream() }
            // 解析数据
            serializerManager.dataDeserializeStream(blockId, stream)(info.classTag)
        }
    }
    // 遍历完，调用releaseLockAndDispose释放读锁，并且释放数据内存
    val ci = CompletionIterator[Any, Iterator[Any]](iterToReturn, {
        releaseLockAndDispose(blockId, diskData, taskAttemptId)
    })
    Some(new BlockResult(ci, DataReadMethod.Disk, info.size))
}
```



这里涉及到缓存数据到内存，也表明了spark对内存的使用频率非常高。一种是缓存对象，另一种是缓存字节。

缓存对象，是调用了MemoryStore的putIteratorAsValues方法。如果所有的数据写入成功，那么MemoryStore会数据存储到MemoryEntry里。否则，只能返回PartiallyUnrolledIterator，一部分存到UnrolledMemory，一部分仍然存到内存里。

缓存字节，是调用了MemoryStore的putBytes方法。如果所有的数据写入成功，那么MemoryStore会数据存储到MemoryEntry里。否则全部的数据仍然存储在磁盘里。



