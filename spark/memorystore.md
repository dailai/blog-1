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

释放内存的原理是，首先得到这个BlockId所属的Rdd，然后找到只要不是这个Rdd属于的BlockId，就尝试释放掉。注意当Block被读的时候，会跳过。