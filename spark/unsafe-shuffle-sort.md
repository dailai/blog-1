# unsafa shuffle sort #

unsafa shuffle sort 只能根据partition排序，不能保证key的顺序。因为这里只需要保证partition的顺序，所以比较的时候不需要key和value。所以将key和value的值进行序列化保存起来，记录对应的存储位置。当排好序后，根据存储位置再将结果取出来。



## 序列化 ##

```scala
public class UnsafeShuffleWriter<K, V> extends ShuffleWriter<K, V> {
    void insertRecordIntoSorter(Product2<K, V> record) throws IOException {
        assert(sorter != null);
        final K key = record._1();
        // 根据key获取partitionId
        final int partitionId = partitioner.getPartition(key);
        // 开始序列化，清空buffer
        serBuffer.reset();
        // serOutputStream是serBuffer的装饰器，提供序列化功能
        // 写入key值
        serOutputStream.writeKey(key, OBJECT_CLASS_TAG);
        // 写入value值
        serOutputStream.writeValue(record._2(), OBJECT_CLASS_TAG);
        serOutputStream.flush();

        final int serializedRecordSize = serBuffer.size();
        assert (serializedRecordSize > 0);
		// 调用ShuffleExternalSorter的insertRecord方法，添加record
        sorter.insertRecord(
          serBuffer.getBuf(), Platform.BYTE_ARRAY_OFFSET, serializedRecordSize, partitionId);
      }
}
```





```java
final class ShuffleExternalSorter extends MemoryConsumer {
  public void insertRecord(Object recordBase, long recordOffset, int length, int partitionId)
    throws IOException {

    // 每隔固定的数目，就会触发spill
    if (inMemSorter.numRecords() >= numElementsForSpillThreshold) {
      spill();
    }
	// 如果内存不够，则申请扩容
    growPointerArrayIfNecessary();
    // Need 4 bytes to store the record length.
    final int required = length + 4;
    acquireNewPageIfNecessary(required);

    assert(currentPage != null);
    final Object base = currentPage.getBaseObject();
    // 生成recordAddress， 通过它可以找到数据
    final long recordAddress = taskMemoryManager.encodePageNumberAndOffset(currentPage, pageCursor);
    // 将数据存储到MemoryBlock
    // 首先写入数据大小length
    Platform.putInt(base, pageCursor, length);
    pageCursor += 4;
    // 拷贝数据到MemoryBlock
    Platform.copyMemory(recordBase, recordOffset, base, pageCursor, length);
    pageCursor += length;
    // 将数据位置和partition添加到inMemSorter
    inMemSorter.insertRecord(recordAddress, partitionId);
  }
}

```



这里PackedRecordPointer.packPointer方法，将recordPointer和partitionId编码，装载到一个Long数据里。数据格式如下：

```shell
----------------------------------------------------
			PackedRecordPointer                |
----------------------------------------------------
    recordPointer          |     partitionId    |    
----------------------------------------------------
    5 bytes                |      3 bytes       |
----------------------------------------------------
```

封装PackedRecordPointer， 然后存储到array里。

```java
final class ShuffleInMemorySorter {
    
  private LongArray array;
  private int pos = 0;

  public void insertRecord(long recordPointer, int partitionId) {
    array.set(pos, PackedRecordPointer.packPointer(recordPointer, partitionId));
    pos++;
  }
```



## 排序 ##

当配置指定了spark.shuffle.sort.useRadixSort， 那么这里会使用桶排序。否则则使用TimSort算法。

```java
private final boolean useRadixSort = conf.getBoolean("spark.shuffle.sort.useRadixSort", true);

public ShuffleSorterIterator getSortedIterator() {
  int offset = 0;
  if (useRadixSort) {
  	
    offset = RadixSort.sort(
      array, pos,
      PackedRecordPointer.PARTITION_ID_START_BYTE_INDEX,
      PackedRecordPointer.PARTITION_ID_END_BYTE_INDEX, false, false);
  } else {
    MemoryBlock unused = new MemoryBlock(
      array.getBaseObject(),
      array.getBaseOffset() + pos * 8L,
      (array.size() - pos) * 8L);
    LongArray buffer = new LongArray(unused);
    Sorter<PackedRecordPointer, LongArray> sorter =
      new Sorter<>(new ShuffleSortDataFormat(buffer));

    sorter.sort(array, 0, pos, SORT_COMPARATOR);
  }
  return new ShuffleSorterIterator(pos, array, offset);
}
```

