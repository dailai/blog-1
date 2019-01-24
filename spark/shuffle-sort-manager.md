## shuffle sort manager ##



## shuffle 算法选择 ##

根据不同的情形，提供三个shuffle sort writer选择。

* BypassMergeSortShuffleWriter ： 当前shuffle没有聚合， 并且分区数小于spark.shuffle.sort.bypassMergeThreshold（默认200）
* UnsafeShuffleWriter ： 当前rdd的数据支持序列化（即UnsafeRowSerializer），并且没有聚合， 并且分区数小于  2^24。

* SortShuffleWriter ： 其余



下图是整个shuffle相关的uml图



@startuml

abstract class ShuffleHandle

class  BaseShuffleHandle

class BypassMergeSortShuffleHandle

class SerializedShuffleHandle

ShuffleHandle <|-- BaseShuffleHandle
BaseShuffleHandle <|-- BypassMergeSortShuffleHandle
BaseShuffleHandle <|-- SerializedShuffleHandle

interface ShuffleManager

class SortShuffleManager

ShuffleManager <|-- SortShuffleManager

abstract class ShuffleWriter

class BypassMergeSortShuffleWriter

class SortShuffleWriter

class UnsafeShuffleWriter

ShuffleWriter <|-- BypassMergeSortShuffleWriter

ShuffleWriter <|-- SortShuffleWriter

ShuffleWriter <|-- UnsafeShuffleWriter

interface ShuffleReader

class BlockStoreShuffleReader

ShuffleReader <|-- BlockStoreShuffleReader

ShuffleManager --> ShuffleHandle : registerShuffle

ShuffleManager --> ShuffleWriter : getWriter

ShuffleManager --> ShuffleReader : getReader

@enduml



ShuffleHandle 会保存shuffle writer算法需要的信息。根据ShuffleHandle的类型，来选择ShuffleWriter的类型。

ShuffleWriter负责在map端生成中间数据，ShuffleReader负责在reduce端读取和整合中间数据。

ShuffleManager 提供了registerShuffle方法，根据shuffle的dependency情况，选择出ShuffleHandler。registerShuffle方法比较简单，这里简单说下原理。它对于不同的ShuffleHandler，有着不同的条件

* BypassMergeSortShuffleHandle :  该shuffle不需要聚合，并且reduce端的分区数目小于配置项spark.shuffle.sort.bypassMergeThreshold，默认为200
* SerializedShuffleHandle  :  该shuffle支持数据不需要聚合，并且必须支持序列化时seek位置，还需要reduce端的分区数目小于16777216（1 << 24 + 1）
* BaseShuffleHandle  :  其余情况

getWriter方法会根据egisterShuffle方法返回的ShuffleHandler，选择出哪种 shuffle writer，原理比较简单：

如果是BypassMergeSortShuffleHandle， 则选择BypassMergeSortShuffleWriter

如果是SerializedShuffleHandle， 则选择UnsafeShuffleWriter

如果是BaseShuffleHandle， 则选择SortShuffleWriter



ShuffleWriter只有两个方法，write和stop方法。使用者首先调用write方法，添加数据，完成排序最后调用stop方法，返回MapStatus结果。下面依次介绍ShuffleWriter的三个子类。



## DiskBlockObjectWriter 原理 ##

在介绍shuffle writer 之前，需要先介绍下DiskBlockObjectWriter原理，因为后面的shuffle writer 都会使用它将数据写入文件。

它提供了文件写入功能，在此之上还加入了统计，压缩和序列化。  它使用了装饰流，涉及到FileOutputStream ， TimeTrackingOutputStream， ManualCloseBufferedOutputStream， 压缩流， 序列化流。

TimeTrackingOutputStream增加对写花费时间的统计。

ManualCloseBufferedOutputStream 继承 OutputStream， 更改了close方法。使用者必须调用manualClose方法手动关闭。这样做是防止外层的装饰流调用close，导致里面的流也会调用close。代码如下:

```scala
trait ManualCloseOutputStream extends OutputStream {
  abstract override def close(): Unit = {
    flush()
  }

  def manualClose(): Unit = {
    super.close()
  }
}
```

压缩流和序列化流都是Spark SerializerManager实例化的。

DiskBlockObjectWriter的流初始化，代码如下：

```scala
private def initialize(): Unit = {
  // 文件输出流
  fos = new FileOutputStream(file, true)
  // 获取该文件的Channel，通过Channel获取写位置
  channel = fos.getChannel()
  // 装饰流 TimeTrackingOutputStream， writeMetrics是作为统计使用的
  ts = new TimeTrackingOutputStream(writeMetrics, fos)
  // 继承缓冲流，但是作为ManualCloseOutputStream的接口
  class ManualCloseBufferedOutputStream
    extends BufferedOutputStream(ts, bufferSize) with ManualCloseOutputStream
  mcs = new ManualCloseBufferedOutputStream
}

def open(): DiskBlockObjectWriter = {
  if (hasBeenClosed) {
    throw new IllegalStateException("Writer already closed. Cannot be reopened.")
  }
  if (!initialized) {
    initialize()
    initialized = true
  }
  // 通过SerializerManager装饰压缩流
  bs = serializerManager.wrapStream(blockId, mcs)
  // 通过SerializerInstance装饰序列流
  objOut = serializerInstance.serializeStream(bs)
  streamOpen = true
  this
}
```

注意到 initialize方法只会调用一次，open方法会多次调用。因为DiskBlockObjectWriter涉及到了序列化，而序列化流是有缓存的，当每次flush序列化流后，都会关闭它，并且调用open获取新的序列化流。

DiskBlockObjectWriter提供了write方法写数据，还提供了commitAndGet方法flush序列化流。commitAndGet返回FileSegment，包含了自从上一次提交开始，到此次commit的写入数据的位置信息 (起始位置，数据长度)。

```scala
def write(key: Any, value: Any) {
  if (!streamOpen) {
    open()
  }
  // 只是简单调用了objOut流，写入key和value
  objOut.writeKey(key)
  objOut.writeValue(value)
  // 记录写入的数据条数和字节数
  recordWritten()
}

def commitAndGet(): FileSegment = {
    if (streamOpen) {
        // NOTE: Because Kryo doesn't flush the underlying stream we explicitly flush both the
        //       serializer stream and the lower level stream.
        // 调用objOut流的flush
        objOut.flush()
        // 调用bs的flush
        bs.flush()
        // 关闭objOut流
        objOut.close()
        streamOpen = false

        if (syncWrites) {
            // 调用文件sync方法，强制flush内核缓存
            val start = System.nanoTime()
            fos.getFD.sync()
            writeMetrics.incWriteTime(System.nanoTime() - start)
        }
        // 获取文件的写位置
        val pos = channel.position()
        // committedPosition表示上一次commit的时候的位置
        // 如果是第一次commit，那么是打开文件时的长度
        // pos - committedPosition计算出自从上一次commit开始，写入数据的长度
        val fileSegment = new FileSegment(file, committedPosition, pos - committedPosition)
        // 更新 committedPosition
        committedPosition = pos
        // 更新写入字节数
        writeMetrics.incBytesWritten(committedPosition - reportedPosition)
        fileSegment
    } else {
        new FileSegment(file, committedPosition, 0)
    }
}

```





## 索引文件 ##

IndexShuffleBlockResolver类负责创建索引文件，它提供了writeIndexFileAndCommit方法创建索引。





## BypassMergeSortShuffleHandle 原理 ##

BypassMergeSortShuffleHandle会为 reduce端的每个分区，创建一个DiskBlockObjectWriter。根据Key判断分区索引，然后添加到对应的DiskBlockObjectWriter，写入到文件。 最后按照分区索引顺序，将所有的文件汇合到同一个文件。如下图所示：



接下来看看源码的实现

```java
final class BypassMergeSortShuffleWriter<K, V> extends ShuffleWriter<K, V> {
  
  public void write(Iterator<Product2<K, V>> records) throws IOException {
    final SerializerInstance serInstance = serializer.newInstance();
    final long openStartTime = System.nanoTime();
    // DiskBlockObjectWriter数组，索引是reduce端的分区索引
    partitionWriters = new DiskBlockObjectWriter[numPartitions];
    // FileSegment数组，索引是reduce端的分区索引
    partitionWriterSegments = new FileSegment[numPartitions];
    // 为每个reduce端的分区，创建临时Block和文件
    for (int i = 0; i < numPartitions; i++) {
      final Tuple2<TempShuffleBlockId, File> tempShuffleBlockIdPlusFile =
        blockManager.diskBlockManager().createTempShuffleBlock();
      final File file = tempShuffleBlockIdPlusFile._2();
      final BlockId blockId = tempShuffleBlockIdPlusFile._1();
      partitionWriters[i] =
        blockManager.getDiskWriter(blockId, file, serInstance, fileBufferSize, writeMetrics);
    }
   
    // 遍历数据，根据key找到分区索引，存到对应的文件中
    while (records.hasNext()) {
      final Product2<K, V> record = records.next();
      // 获取数据的key
      final K key = record._1();
      // 根据reduce端的分区器，判断该条数据应该存在reduce端的哪个分区
      // 并且通过DiskBlockObjectWriter，存到对应的文件中
      partitionWriters[partitioner.getPartition(key)].write(key, record._2());
    }

    // 调用DiskBlockObjectWriter的commitAndGet方法，获取FileSegment，包含写入的数据信息
    for (int i = 0; i < numPartitions; i++) {
      final DiskBlockObjectWriter writer = partitionWriters[i];
      partitionWriterSegments[i] = writer.commitAndGet();
      writer.close();
    }
    // 获取最终结果的文件名
    File output = shuffleBlockResolver.getDataFile(shuffleId, mapId);
    // 根据output文件名，生成临时文件。临时文件的名称只是在output文件名后面添加了一个uuid
    File tmp = Utils.tempFileWith(output);
    try {
      // 将所有的文件都合并到tmp文件中
      partitionLengths = writePartitionedFile(tmp);
      // 这里writeIndexFileAndCommit会将tmp文件重命名，并且会创建索引文件。
      shuffleBlockResolver.writeIndexFileAndCommit(shuffleId, mapId, partitionLengths, tmp);
    } finally {
      if (tmp.exists() && !tmp.delete()) {
        logger.error("Error while deleting temp file {}", tmp.getAbsolutePath());
      }
    }
    mapStatus = MapStatus$.MODULE$.apply(blockManager.shuffleServerId(), partitionLengths);
  }
}
```

并且创建索引文件，记录分区数据对应的文件所在位置。



## UnsafeShuffleWriter 原理 ##

UnsafeShuffleWriter会首先将数据序列化，保存在MemoryBlock中。

LongArray可以看作是一个Long类型的数组，不过它支持堆内和堆外内存。

这里Long类型包含了三部分的数组，分区索引，所在的内存块，所在内存块中的偏移位置。一个Long类型占据64bit，格式如下：

```shell
---------------------------------------------------------------
     24 bit           |    13 bit        |      27 bit
---------------------------------------------------------------
   partitionId        |   memoryBlock    |      offset
--------------------------------------------------------------
```





ShuffleInMemorySorter

ShuffleInMemorySorter包含了LongArray， 并且提供了替换LongArray接口。并且支持数据按照分区索引排序



ShuffleExternalSorter 包含ShuffleInMemorySorter， 支持申请内存和磁盘溢写。

ShuffleExternalSorter指定了申请内存块的最大容量，不能超过 1<< 27，也就是不能超过27位。



UnsafeShuffleWriter会将ShuffleExternalSorter的溢写的文件，合并到一起。







SortShuffleWriter

SortShuffleWriter使用 ExternalSorter 排序合并。



ExternalSorter会比较复杂，因为它支持聚合，排序。

