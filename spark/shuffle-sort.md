# shuffle 排序 #

在进行shuffle之前，map端会先将数据进行排序。排序的规则，根据不同的场景，会分为两种。首先会根据Key将元素分成不同的partition。第一种只需要保证元素的partitionId排序，但不会保证同一个partitionId的内部排序。第二种是既保证元素的partitionId排序，也会保证同一个partitionId的内部排序。

因为有些shuffle操作涉及到聚合，对于这种需要聚合的操作，使用PartitionedAppendOnlyMap来排序。

对于不需要聚合的，则使用PartitionedPairBuffer排序。



## PartitionedPairBuffer 原理 ##

PartitionedPairBuffer的原理很简单，它使用数组保存数据，最后调用Tim排序算法，根据分区索引排序。

数据格式如下：

```shell
--------------------------------------------------------------------------
     AnyRef            |   AnyRef   |       AnyRef           |   AnyRef  |
--------------------------------------------------------------------------
   partitionId, key    |   value    |     partitionId, key   |    value  | 
--------------------------------------------------------------------------

```

PartitionedPairBuffer使用Array[AnyRef] 数组，AnyRef指向两种格式的数据，一种是(parition, key)的元组， 一种是value。

```scala
class PartitionedPairBuffer {
    
    // 使用AnyRef数组，保存数据
    private var data = new Array[AnyRef](2 * initialCapacity)
    // 数组目前的索引
    private var curSize = 0
    // 数组的容量
    private var capacity = initialCapacity
    
    // 添加数据
    def insert(partition: Int, key: K, value: V): Unit = {
       if (curSize == capacity) {
           // 增大数组的容量
           growArray()
       }
       // // 存储（partition， key）元素
       data(2 * curSize) = (partition, key.asInstanceOf[AnyRef])
       // 写入 value
       data(2 * curSize + 1) = value.asInstanceOf[AnyRef]
       curSize += 1
    }
    
    override def partitionedDestructiveSortedIterator(keyComparator: Option[Comparator[K]])
         : Iterator[((Int, K), V)] = {
        val comparator = keyComparator.map(partitionKeyComparator).getOrElse(partitionComparator)
        // 调用TimSort算法
        new Sorter(new KVArraySortDataFormat[(Int, K), AnyRef]).sort(data, 0, curSize, comparator)
        iterator
    }
}
```



##  PartitionedAppendOnlyMap 原理 ##

PartitionedAppendOnlyMap继承PartitionedAppendOnlyMap，主要的添加由PartitionedAppendOnlyMap负责。PartitionedAppendOnlyMap自己实现了哈希表，采用了二次探测算法避免哈希冲突。

```scala
class AppendOnlyMap[K, V](initialCapacity: Int = 64) {
  
  // 因为一条数据，占用array的两个位置。所以数组的大小为 数据的容量的两倍
  private var data = new Array[AnyRef](2 * capacity)  
  private var capacity = nextPowerOf2(initialCapacity)
  
  private def rehash(h: Int): Int = Hashing.murmur3_32().hashInt(h).asInt()
  
  // 添加数据
  def update(key: K, value: V): Unit = {
    assert(!destroyed, destructionMessage)
    val k = key.asInstanceOf[AnyRef]
    // 处理key为null
    if (k.eq(null)) {
      if (!haveNullValue) {
        incrementSize()
      }
      nullValue = value
      haveNullValue = true
      return
    }
    // 根据key的hashCode，哈希计算其在数组的位置
    // 这里使用二次探测算法避免哈希冲突
    var pos = rehash(key.hashCode) & mask
    var i = 1
    while (true) {
      val curKey = data(2 * pos)
      // 如果当前位置的值为空，则写入对应的值
      if (curKey.eq(null)) {
        data(2 * pos) = k
        data(2 * pos + 1) = value.asInstanceOf[AnyRef]
        incrementSize()  // Since we added a new key
        return
      } else if (k.eq(curKey) || k.equals(curKey)) {
        // 如果等于当前key，则更新value
        data(2 * pos + 1) = value.asInstanceOf[AnyRef]
        return
      } else {
        // 如果发生哈希冲突，则更新pos位置
        val delta = i
        pos = (pos + delta) & mask
        i += 1
      }
    }
  }
    
  // 提供修改数据，这里涉及到聚合操作
  // updateFunc函数接收两个参数，第一个参数表示这个位置是否已经有数据
  // 第二个参数表示原有的数据
  def changeValue(key: K, updateFunc: (Boolean, V) => V): V = {
    val k = key.asInstanceOf[AnyRef]
    if (k.eq(null)) {
      if (!haveNullValue) {
        incrementSize()
      }
      nullValue = updateFunc(haveNullValue, nullValue)
      haveNullValue = true
      return nullValue
    }
    // 使用二次探测算法，找到数据的位置
    var pos = rehash(k.hashCode) & mask
    var i = 1
    while (true) {
      val curKey = data(2 * pos)
      if (curKey.eq(null)) {
        // 如果当前位置没数据，则调用updateFunc函数，返回新的值
        val newValue = updateFunc(false, null.asInstanceOf[V])
        data(2 * pos) = k
        data(2 * pos + 1) = newValue.asInstanceOf[AnyRef]
        incrementSize()
        return newValue
      } else if (k.eq(curKey) || k.equals(curKey)) {
        // 如果当前位置有数据，则调用updateFunc函数，返回新的值
        val newValue = updateFunc(true, data(2 * pos + 1).asInstanceOf[V])
        data(2 * pos + 1) = newValue.asInstanceOf[AnyRef]
        return newValue
      } else {
        // 更新pos位置直到找到key
        val delta = i
        pos = (pos + delta) & mask
        i += 1
      }
    }
    null.asInstanceOf[V] // Never reached but needed to keep compiler happy
  }
}
```



##  ExternalSorter原理 ##

SortShuffleWriter使用ExternalSorter进行排序，ExternalSorter会根据是否需要聚合来选择不同的算法。



```scala
private[spark] class ExternalSorter[K, V, C] {

  def insertAll(records: Iterator[Product2[K, V]]): Unit = {
    // TODO: stop combining if we find that the reduction factor isn't high
    val shouldCombine = aggregator.isDefined

    if (shouldCombine) {
      // 涉及到聚合，使用PartitionedAppendOnlyMap算法
      val mergeValue = aggregator.get.mergeValue
      val createCombiner = aggregator.get.createCombiner
      var kv: Product2[K, V] = null
      // 构造聚合函数，如果有旧有值，则调用聚合的mergeValue合并值。否则调用createCombiner实例combiner
      val update = (hadValue: Boolean, oldValue: C) => {
        if (hadValue) mergeValue(oldValue, kv._2) else createCombiner(kv._2)
      }
      while (records.hasNext) {
        addElementsRead()
        kv = records.next()
        // 调用getPartition根据key，获得partitionId， 添加到map里
        map.changeValue((getPartition(kv._1), kv._1), update)
        // 检测是否触发spill
        maybeSpillCollection(usingMap = true)
      }
    } else { 
      // 没有涉及到聚合，使用PartitionedPairBuffer算法
      while (records.hasNext) {
        addElementsRead()
        val kv = records.next()
        buffer.insert(getPartition(kv._1), kv._1, kv._2.asInstanceOf[C])
        // 检测是否触发spill
        maybeSpillCollection(usingMap = false)
      }
    }
  }
}
```



上面的maybeSpillCollection方法，当数据过多时，会触发溢写。溢写由spill方法负责，它会将已添加的数据进行排序。











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

spill文件首先根据partition分片，每个partitioin分片又被切分成多个batch分片。每个batch分片，才是存储了record。

```shell
------------------------------------------------------------------------------------
                     parition 0                                  |     ......
------------------------------------------------------------------------------------
                batch 0              |  batch 1     |   .....    |
-------------------------------------------------------------------------------------
record 0   |  record 2  | ....       |
```



spill的文件读取由SpillReader负责，SpillReader有几个属性比较重要：

* partitionId， 当前读取record的所在partition
* indexInPartition， 当前读取的record在partition的索引
* batchId， 当前读取的record的所在batch

* lastPartitionId，下一个record所在的partition
* nextPartitionToRead， 要读取的下一个partition

访问数据，也是按照partition的顺序遍历的。通过readNextPartition方法，返回partition的数据迭代器。

```scala
def readNextPartition(): Iterator[Product2[K, C]] = new Iterator[Product2[K, C]] {
  // 记录当前Iterator的所在partition
  val myPartition = nextPartitionToRead
  nextPartitionToRead += 1

  override def hasNext: Boolean = {
    if (nextItem == null) {
      // 调用SpillReader的readNextItem方法，返回record
      nextItem = readNextItem()
      if (nextItem == null) {
        return false
      }
    }
    assert(lastPartitionId >= myPartition)
    // 如果下一个要读取的record所在的partition，不在等于当前Iterator的所在partition，
    // 也就是当前partition的数据都已经读完
    lastPartitionId == myPartition
  }

  override def next(): Product2[K, C] = {
    if (!hasNext) {
      throw new NoSuchElementException
    }
    val item = nextItem
    nextItem = null
    item
  }
}
```



## 合并排序结果 ##

从上面的spill介绍，可以看到对于一个输入，可能会被切分，生成多个spill文件，和最后一部分存在内存的结果。目前只能切片是有序的，所以这里需要将结果合并。

```sacla
private def merge(spills: Seq[SpilledFile], inMemory: Iterator[((Int, K), C)])
    : Iterator[(Int, Iterator[Product2[K, C]])] = {
  // 为每个spill file 生成 SpillReader
  val readers = spills.map(new SpillReader(_))
  val inMemBuffered = inMemory.buffered
  // 按照partition顺序，遍历各个切片
  (0 until numPartitions).iterator.map { p =>
    val inMemIterator = new IteratorForPartition(p, inMemBuffered)
    // 合并切片为一个iterators
    val iterators = readers.map(_.readNextPartition()) ++ Seq(inMemIterator)
    if (aggregator.isDefined) {
      // 指定了聚合，调用mergeWithAggregation排序
      (p, mergeWithAggregation(
        iterators, aggregator.get.mergeCombiners, keyComparator, ordering.isDefined))
    } else if (ordering.isDefined) {
      // 指定了order，调用mergeSort合并排序
      (p, mergeSort(iterators, ordering.get))
    } else {
      // 因为这里已经保证了iterators是按照partition排序
      (p, iterators.iterator.flatten)
    }
  }
}
```



### 指定聚合 ###





### 指定order ###

这里使用了合并排序的算法。实现使用了PriorityQueue。PriorityQueue存储Iterator，比较大小是将Iterator的第一个数据。每次从最小的Iterator取出数据后，然后将iterator重新插入到PriorityQueue，这样PriorityQueue就会将Iterator重新排序。

```
private def mergeSort(iterators: Seq[Iterator[Product2[K, C]]], comparator: Comparator[K])
    : Iterator[Product2[K, C]] =
{
  val bufferedIters = iterators.filter(_.hasNext).map(_.buffered)
  type Iter = BufferedIterator[Product2[K, C]]
  val heap = new mutable.PriorityQueue[Iter]()(new Ordering[Iter] {
    // Use the reverse of comparator.compare because PriorityQueue dequeues the max
    override 
    def compare(x: Iter, y: Iter): Int = -comparator.compare(x.head._1, y.head._1)
  })
  // 添加所有的iterator
  heap.enqueue(bufferedIters: _*)  // Will contain only the iterators with hasNext = true
  new Iterator[Product2[K, C]] {
    override def hasNext: Boolean = !heap.isEmpty

    override def next(): Product2[K, C] = {
      if (!hasNext) {
        throw new NoSuchElementException
      }
      // 取出最小的iterator
      val firstBuf = heap.dequeue()
      // 从iterator中取第一个数据
      val firstPair = firstBuf.next()
      if (firstBuf.hasNext) {
        // 重新插入到PriorityQueue， 让PriorityQueue重新排序
        heap.enqueue(firstBuf)
      }
      firstPair
    }
  }
}
```

