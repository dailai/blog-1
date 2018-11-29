# shuffle 排序 #

在进行shuffle之前，map端会先将数据进行排序。排序的规则，根据不同的场景，会分为两种。首先会根据Key将元素分成不同的partition。第一种只需要保证元素的partitionId排序，但不会保证同一个partitionId的内部排序。第二种是既保证元素的partitionId排序，也会保证同一个partitionId的内部排序。

因为有些shuffle操作涉及到聚合，对于这种需要聚合的操作，使用PartitionedAppendOnlyMap来排序。

对于不需要聚合的，则使用PartitionedPairBuffer排序。

## 排序规则 ##

排序的规则，根据不同的场景，分为是否rdd指定了order两种。

### 指定order ###

如果rdd指定了order，元素的partitionId排序，并且也会保证同一个partitionId的内部排序。

这里比较的数据格式为（partitionId, key）。

```scala
def partitionKeyComparator[K](keyComparator: Comparator[K]): Comparator[(Int, K)] = {
  new Comparator[(Int, K)] {
    override def compare(a: (Int, K), b: (Int, K)): Int = {
      // 优先比较partition
      val partitionDiff = a._1 - b._1
      if (partitionDiff != 0) {
        partitionDiff
      } else {
        // 如果partition相等，则继续比较key
        keyComparator.compare(a._2, b._2)
      }
    }
  }
}
```

继续看keyComparator的定义, 在ExternalSorter里定义。这里只是比较了两个的hashCode。

```scala
  private val keyComparator: Comparator[K] = ordering.getOrElse(new Comparator[K] {
    override def compare(a: K, b: K): Int = {
      val h1 = if (a == null) 0 else a.hashCode()
      val h2 = if (b == null) 0 else b.hashCode()
      if (h1 < h2) -1 else if (h1 == h2) 0 else 1
    }
  })
```

### 不指定order ###

如果rdd没有指定order，则只需要比较partitionId排序，而不需要对同一个partitionId内部排序。

这里比较的数据格式为（partitionId, key）

```scala
def partitionComparator[K]: Comparator[(Int, K)] = new Comparator[(Int, K)] {
    override def compare(a: (Int, K), b: (Int, K)): Int = {
      a._1 - b._1
    }
  }
```

## 排序种类 ##

### 非聚合排序 ###

当只使用rdd的groupby的方法，那么这里只需要分组，而不需要聚合。这种情况会使用PartitionedPairBuffer排序。首先调用PartitionedPairBuffer的insert方法，向PartitionedPairBuffer添加元素，最后调用partitionedDestructiveSortedIterator获取排序后的结果。

PartitionedPairBuffer使用array存储存储元素。本来一条数据有三个属性，partition，key，value。但array 将数据切分成两部分（partition， key） 和 （value），分别存储到array的元素里。当数据的空间不足时会调用growArray申请新的数组，大小是原来的两倍。注意到array的数据类型为AnyRef，即使申请两倍的大小，占用的内存相比于数据也是比较小的。

```scala
private[spark] class PartitionedPairBuffer[K, V](initialCapacity: Int = 64) {
    // 数据的容量
	private var capacity = initialCapacity
  	private var curSize = 0
    // 因为一条数据，占用array的两个位置。所以数组的大小为 数据的容量的两倍
  	private var data = new Array[AnyRef](2 * initialCapacity)
    
    def insert(partition: Int, key: K, value: V): Unit = {
    if (curSize == capacity) {
      // 容量不足会申请内存
      growArray()
    }
    // 存储（partition， key）元素
    data(2 * curSize) = (partition, key.asInstanceOf[AnyRef])
    // 存储（value）元素
    data(2 * curSize + 1) = value.asInstanceOf[AnyRef]
    curSize += 1
  }
    
  private def growArray(): Unit = {
    if (capacity >= MAXIMUM_CAPACITY) {
      throw new IllegalStateException(s"Can't insert more than ${MAXIMUM_CAPACITY} elements")
    }
    val newCapacity =
      if (capacity * 2 < 0 || capacity * 2 > MAXIMUM_CAPACITY) { // Overflow
        MAXIMUM_CAPACITY
      } else {
        capacity * 2
      }
    val newArray = new Array[AnyRef](2 * newCapacity)
    System.arraycopy(data, 0, newArray, 0, 2 * capacity)
    data = newArray
    capacity = newCapacity
    resetSamples()
  }

}
	
```

Sorter这里是调用了的TimSort算法。TimSort的 定义在org.apache.spark.util.collection包里，具体原理可以搜索。当Sorter排序后，结果仍在存在data数组里。这里封装了data的访问方式，提供了Iterator的遍历。

```scala
override def partitionedDestructiveSortedIterator(keyComparator: Option[Comparator[K]])
    : Iterator[((Int, K), V)] = {
    val comparator = keyComparator.map(partitionKeyComparator).getOrElse(partitionComparator)
    new Sorter(new KVArraySortDataFormat[(Int, K), AnyRef]).sort(data, 0, curSize, comparator)
    iterator
  }

private def iterator(): Iterator[((Int, K), V)] = new Iterator[((Int, K), V)] {
    var pos = 0

    override def hasNext: Boolean = pos < curSize

    override def next(): ((Int, K), V) = {
      if (!hasNext) {
        throw new NoSuchElementException
      }
      val pair = (data(2 * pos).asInstanceOf[(Int, K)], data(2 * pos + 1).asInstanceOf[V])
      pos += 1
      pair
    }
  }
```

### 聚合排序 ###





## spill原理 ##

当每添加一条数据时，都会检查是否需要将结果存储到磁盘中。

```
@volatile private var map = new PartitionedAppendOnlyMap[K, C]
@volatile private var buffer = new PartitionedPairBuffer[K, C]

def insertAll(records: Iterator[Product2[K, V]]): Unit = {
  val shouldCombine = aggregator.isDefined

  if (shouldCombine) {
    // 使用map存储数据
    val mergeValue = aggregator.get.mergeValue
    val createCombiner = aggregator.get.createCombiner
    var kv: Product2[K, V] = null
    val update = (hadValue: Boolean, oldValue: C) => {
      if (hadValue) mergeValue(oldValue, kv._2) else createCombiner(kv._2)
    }
    while (records.hasNext) {
      addElementsRead()
      kv = records.next()
      // 调用getPartition根据key，获得partitionId
      map.changeValue((getPartition(kv._1), kv._1), update)
      // 检测是否触发spill
      maybeSpillCollection(usingMap = true)
    }
  } else {
    // 使用buffer存储数据
    while (records.hasNext) {
      addElementsRead()
      val kv = records.next()
      // 调用getPartition根据key，获得partitionId
      buffer.insert(getPartition(kv._1), kv._1, kv._2.asInstanceOf[C])
      // 检测是否触发spill
      maybeSpillCollection(usingMap = false)
    }
  }
}
```

上述maybeSpillCollection用来触发spill过程。

```scala
private def maybeSpillCollection(usingMap: Boolean): Unit = {
    var estimatedSize = 0L
    if (usingMap) {
      // 获取map的大小
      estimatedSize = map.estimateSize()
      // 触发spill过程
      if (maybeSpill(map, estimatedSize)) {
        map = new PartitionedAppendOnlyMap[K, C]
      }
    } else {
      // 获取buffer的大小
      estimatedSize = buffer.estimateSize()
      // 触发spill过程
      if (maybeSpill(buffer, estimatedSize)) {
        buffer = new PartitionedPairBuffer[K, C]
      }
    }
  }

protected def maybeSpill(collection: C, currentMemory: Long): Boolean = {
    var shouldSpill = false
    // 每隔32条数据，检查是否当前使用内存超过限制
    if (elementsRead % 32 == 0 && currentMemory >= myMemoryThreshold) {
      // 申请2倍内存
      val amountToRequest = 2 * currentMemory - myMemoryThreshold
      val granted = acquireMemory(amountToRequest)
      myMemoryThreshold += granted
      // 如果申请之后的内存，仍然不够存储，那么触发spill
      shouldSpill = currentMemory >= myMemoryThreshold
    }
    // 当内存不足，或者数据大小超过了指定值，则执行spill
    shouldSpill = shouldSpill || _elementsRead > numElementsForceSpillThreshold
    if (shouldSpill) {
      _spillCount += 1
      logSpillage(currentMemory)
      // 调用spill
      spill(collection)
      _elementsRead = 0
      _memoryBytesSpilled += currentMemory
      // 释放数据占用的内存
      releaseMemory()
    }
    shouldSpill
  }

override protected[this] def spill(collection: WritablePartitionedPairCollection[K, C]): Unit = {
    // 调用destructiveSortedWritablePartitionedIterator获取排序后的结果
    val inMemoryIterator = collection.destructiveSortedWritablePartitionedIterator(comparator)
    // 将排序后的结果写入文件
    val spillFile = spillMemoryIteratorToDisk(inMemoryIterator)
    // 记录文件
    spills += spillFile
  }
```

spillMemoryIteratorToDisk负责将排序后的结果，写到磁盘。它生成TempShuffleBlockId，然后调用diskBlockmanager执行写动作。这里是通过batch的方式写入文件

```scala
  private[this] def spillMemoryIteratorToDisk(inMemoryIterator: WritablePartitionedIterator) : SpilledFile = {
      // 创建TempShuffleBlock
  	  val (blockId, file) = diskBlockManager.createTempShuffleBlock()
      // 生成Writer
      val writer: DiskBlockObjectWriter =
      blockManager.getDiskWriter(blockId, file, serInstance, fileBufferSize, spillMetrics)
      // 记录此次partition拥有的数据条数
      val elementsPerPartition = new Array[Long](numPartitions)
      // 每一次刷新，都会生成一个batch
      val batchSizes = new ArrayBuffer[Long]
      
      // 
      def flush(): Unit = {
      	val segment = writer.commitAndGet()
      	batchSizes += segment.length
      	_diskBytesSpilled += segment.length
      	objectsWritten = 0
      }
      
      while (inMemoryIterator.hasNext) {
          val partitionId = inMemoryIterator.nextPartition()
          // 将数据写入writer
          inMemoryIterator.writeNext(writer)
          // 记录每个partition的数目
          elementsPerPartition(partitionId) += 1
          objectsWritten += 1
          // 每写入固定条数的数据，则刷新写入磁盘
          if (objectsWritten == serializerBatchSize) {
             flush()
          }
      }
      // 将剩余的数据，刷新到磁盘
      if (objectsWritten > 0) {
        flush()
      }
      // 返回SpilledFile对象，包含了此次spill文件的信息
      SpilledFile(file, blockId, batchSizes.toArray, elementsPerPartition)
      
  }
```



## 读取spill文件 ##

spill的文件读取由SpillReader负责，





从上面的spill介绍，可以看到对于一个输入，可能会被切分，生成多个spill文件。目前只能每个spill文件是有序的。所以这里需要将spill文件进行合并。

