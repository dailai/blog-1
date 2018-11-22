# MemoryStore #

spark 会将Block的数据，优先存储到memory，如果memoty不足，则写入到磁盘里。



## StorageMemoryPool ##

StorageMemoryPool代表着内存池，专门负责Block在内存的存储。它有下列属性：

* poolSize，总的内存大小
* memoryFree ，空闲内存大小
* memoryUsed，已使用内存大小
* memoryStore， MemoryStore对象，当内存不足时，负责清理内存
* memoryMode， 内存存储的位置，是堆还是非堆



## MemoryEntry ##

MemoryEntry 类代表着实际的存储数据和对应的元信息。它有下列属性：

* size，数据大小
* memoryMode， 内存存储的位置，是堆还是非堆
* classTag， 数据的类型

上面的classTag代表着存储数据的类型，spark会根据这个，来序列化和反序列化。

MemoryEntry有两个子类，非别代表着数据的存储格式是否序列化。SerializedMemoryEntry用ChunkedByteBuffer来存储数据，代表着序列化格式。DeserializedMemoryEntry用Array[T]来存储数据，T就是classTag，代表着反序列化格式。

MemoryStore包含了entries字段，保存了BlockId到MemoryEntry的映射。



## 清理内存 ##

当写入Block数据，内存不足时，MemoryStore会调用evictBlocksToFreeSpace方法，清理和释放内存。

```scala
def evictBlocksToFreeSpace(
      blockId: Option[BlockId],
      space: Long,
      memoryMode: MemoryMode): Long = {
    
}
```

上述的blockId表示为哪个Block释放内存，space是要申请内存的大小，memoryMode表示存储位置。

释放内存的原理是，首先得到这个BlockId所属的Rdd，然后找到只要不是这个Rdd属于的BlockId，就尝试释放掉。注意当Block被读的时候，会跳过。下面是判断Block是否可以被删除

```scala
def blockIsEvictable(blockId: BlockId, entry: MemoryEntry[_]): Boolean = {
        entry.memoryMode == memoryMode && (rddToAdd.isEmpty || rddToAdd != getRddId(blockId))
      }
```

遍历entries的代码：

```scala
 val iterator = entries.entrySet().iterator()
        while (freedMemory < space && iterator.hasNext) {
          val pair = iterator.next()
          val blockId = pair.getKey
          val entry = pair.getValue
          if (blockIsEvictable(blockId, entry)) {
            // blocking=flase，代表尝试获取写锁。如果没有，则直接跳过
            if (blockInfoManager.lockForWriting(blockId, blocking = false).isDefined) {
              selectedBlocks += blockId
              freedMemory += pair.getValue.size
            }
          }
        }
```

删除Block的原理是，如果Block使用了磁盘存储，那么会先把Block的数据先写入到磁盘里面，然后将Block从entries删除，最后调用MemoryManager释放内存。



## 数据写入 ##

MemoryStore支持两种输入格式，一个是ChunkedByteBuffer，另一个是Iterator[T]。

### 字节存储 ###

MemoryStore会直接将ChunkedByteBuffer存到SerializedMemoryEntry类，表示这是序列化的数据。

```scala
def putBytes[T: ClassTag](
      blockId: BlockId,
      size: Long,
      memoryMode: MemoryMode,
      _bytes: () => ChunkedByteBuffer): Boolean = {
    // 首先确保BlockId不存在
    require(!contains(blockId), s"Block $blockId is already present in the MemoryStore")
    // 向MemoryManager申请内存
    if (memoryManager.acquireStorageMemory(blockId, size, memoryMode)) {
      val bytes = _bytes()
      assert(bytes.size == size)
      // 将数据保存到SerializedMemoryEntry
      val entry = new SerializedMemoryEntry[T](bytes, memoryMode, implicitly[ClassTag[T]])
      entries.synchronized {
        // 存放到entries
        entries.put(blockId, entry)
      }
    }
        
```



### 对象存储 ###

对于Iterator[T]的输入，MemoryStore提供两个方法，分别用于将数据存储到SerializedMemoryEntry和DeserializedMemoryEntry。

#### 非序列化存储 ####

它接收一个迭代器Iterator[T]，包含了类型T的对象。首先它会遍历Iterator， 将数据存储到列表里面。因为列表会占用内存，所以当遍历的时候，它每隔固定的数量，会检测是否申请内存。每次申请内存大约为当前的0.5倍。

```scala
  private[storage] def putIteratorAsValues[T](
      blockId: BlockId,
      values: Iterator[T],
      classTag: ClassTag[T]): Either[PartiallyUnrolledIterator[T], Long] = {

      require(!contains(blockId), s"Block $blockId is already present in the MemoryStore")
      
      val memoryCheckPeriod = 16
      val memoryGrowthFactor = 1.5
      var vector = new SizeTrackingVector[T]()(classTag)
      // 已经申请的内存大小
      var memoryThreshold = initialMemoryThreshold
      
      while (values.hasNext && keepUnrolling) {
      // 将数据添加到vector容器
      vector += values.next()
      // 遍历每隔 memoryCheckPeriod 次，触发内存检查
      if (elementsUnrolled % memoryCheckPeriod == 0) {
        val currentSize = vector.estimateSize()
        // 当数据大于当前申请的内存
        if (currentSize >= memoryThreshold) {
          // 申请的内存为当前使用内存的1.5倍 减去 已经申请的内存
          val amountToRequest = (currentSize * memoryGrowthFactor - memoryThreshold).toLong
          // 向MemoryManager申请内存
          keepUnrolling = reserveUnrollMemoryForThisTask(blockId, amountToRequest, MemoryMode.ON_HEAP)
          if (keepUnrolling) {
            unrollMemoryUsedByThisBlock += amountToRequest
          }
          // New threshold is currentSize * memoryGrowthFactor
          memoryThreshold += amountToRequest
        }
      }
      elementsUnrolled += 1
    }
    ......
  }
```

注意上面while的退出条件。一种是内存不够，另一个是数据都已经遍历完。

对于内存不足的情况，返回已经解析的数据和剩余的Iterator。

对于第二种情况，还有涉及到内存的申请。注意下，刚刚vector容器存放着数据，这里所占用的内存是用作UnrollMemory。而DeserializedMemoryEntry所占用的内存，是用作Storage的。所以这里会有一个内存转移的操作。

```scala
def transferUnrollToStorage(amount: Long): Unit = {
        // 使用 Synchronize来确保执行操作的原子性
        memoryManager.synchronized {
          // 首先释放UnrollMemory
          releaseUnrollMemoryForThisTask(MemoryMode.ON_HEAP, amount)
          // 然后申请同样大小的内存
          val success = memoryManager.acquireStorageMemory(blockId, amount, MemoryMode.ON_HEAP)
          assert(success, "transferring unroll memory to storage memory failed")
        }
      }
```



### 序列化存储 ###

它遍历Iterator，将数据存储到ChunkedByteBufferOutputStream。ChunkedByteBufferOutputStream写入数据时，会自动扩容。所以这里申请内存是在ChunkedByteBufferOutputStream写入数据之后。

```scala
// 实例化ChunkedByteBufferOutputStream
val bbos = new ChunkedByteBufferOutputStream(chunkSize, allocator)
// 实例RedirectableOutputStream，用于后面的序列化
val redirectableStream = new RedirectableOutputStream
redirectableStream.setOutputStream(bbos)
// 支持序列化的stream
val serializationStream: SerializationStream = {
    val autoPick = !blockId.isInstanceOf[StreamBlockId]
    val ser = serializerManager.getSerializer(classTag, autoPick).newInstance()
    ser.serializeStream(serializerManager.wrapForCompression(blockId, redirectableStream))
}

def reserveAdditionalMemoryIfNecessary(): Unit = {
    // 如果ChunkedByteBufferOutputStream的大小超过了已经申请内存的大小
    if (bbos.size > unrollMemoryUsedByThisBlock) {
        // 计算需要申请内存的大小，为当前数据的大小减去已经申请内存的大小
        val amountToRequest = bbos.size - unrollMemoryUsedByThisBlock
        // 向MmoryManager申请内存
        keepUnrolling = reserveUnrollMemoryForThisTask(blockId, amountToRequest, memoryMode)
        if (keepUnrolling) {
            unrollMemoryUsedByThisBlock += amountToRequest
        }
    }
}


while (values.hasNext && keepUnrolling) {
    serializationStream.writeObject(values.next())(classTag)
    reserveAdditionalMemoryIfNecessary()
}


```

后面的逻辑同上







