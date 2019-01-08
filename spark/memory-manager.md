# Spark 内存管理 #

spark为了更加高效的使用内存，自己来管理内存。



StorageMemoryPool有两种模式 ， 分为堆内内存和堆外内存。







内存池 



内存地址

```
MemoryLocation
```

内存块

```
MemoryBlock
```





任务内存管理

```
TaskMemoryManager
```



```
allocatedPages 记录分配的内存块
```







内存分配接口

```
MemoryAllocator
```



堆内分配

```java
public class HeapMemoryAllocator implements MemoryAllocator {
  // 缓存池， Key的内存块的大小， Value为内存块的引用
  @GuardedBy("this")
  private final Map<Long, LinkedList<WeakReference<MemoryBlock>>> bufferPoolsBySize =
    new HashMap<>();
  
  @Override
  public MemoryBlock allocate(long size) throws OutOfMemoryError {
    // 首先去寻找是否缓冲池有对应大小的内存块
    if (shouldPool(size)) {
      synchronized (this) {
        final LinkedList<WeakReference<MemoryBlock>> pool = bufferPoolsBySize.get(size);
        if (pool != null) {
          while (!pool.isEmpty()) {
            final WeakReference<MemoryBlock> blockReference = pool.pop();
            final MemoryBlock memory = blockReference.get();
            // 如果找到内存块，则直接返回
            if (memory != null) {
              assert (memory.size() == size);
              return memory;
            }
          }
          bufferPoolsBySize.remove(size);
        }
      }
    }
    // 新建数组，则会在堆内分配空间
    // 注意size是指定分配空间的字节数，一个long占有8个字节
    long[] array = new long[(int) ((size + 7) / 8)];
    // 注意这里array是数组的开始地址，但是数组会包含头部，长度为Platform.LONG_ARRAY_OFFSET
    // 真正的数据开始位置是在头部之后
    MemoryBlock memory = new MemoryBlock(array, Platform.LONG_ARRAY_OFFSET, size);
    if (MemoryAllocator.MEMORY_DEBUG_FILL_ENABLED) {
      memory.fill(MemoryAllocator.MEMORY_DEBUG_FILL_CLEAN_VALUE);
    }
    return memory;
  }
        
  private static final int POOLING_THRESHOLD_BYTES = 1024 * 1024;
  
  // 这里倾向于缓存大的内存块，因为大的内存块回收，会很影响jvm的性能
  private boolean shouldPool(long size) {
    return size >= POOLING_THRESHOLD_BYTES;
  }
}  
```



堆外内存调用Unsafe的allocateMemory方法，分配堆外内存

```java
public class UnsafeMemoryAllocator implements MemoryAllocator {

  @Override
  public MemoryBlock allocate(long size) throws OutOfMemoryError {
    // Platform 使用 unsafe分配堆外内存， 返回内存地址
    long address = Platform.allocateMemory(size);
    MemoryBlock memory = new MemoryBlock(null, address, size);
    if (MemoryAllocator.MEMORY_DEBUG_FILL_ENABLED) {
      memory.fill(MemoryAllocator.MEMORY_DEBUG_FILL_CLEAN_VALUE);
    }
    return memory;
  }
}
```





内存池

```
MemoryPool
```

提供有锁的访问内存信息，比如内存总大小，剩余大小，已使用的大小



内存管理

内存从位置来说，分为 堆内，堆外

从使用方面来说，分为 存储， 执行



```
StaticMemoryManager
```

存储只能是堆内

执行 包含 堆内 和 堆外







```
UnifiedMemoryManager
```









```
MemoryConsumer
```

提供了申请和释放内存的接口，并且支持数据溢写到磁盘。



```
ShuffleExternalSorter
```

都继承MemoryConsumer



MemoryConsumer的申请内存，都是调用TaskMemoryManager的方法。



```
TaskMemoryManager
```

的原理很重要



TaskMemoryManager包含了所有的MemoryConsumer, 只负责execution用途的内存管理



```scala
/**
 * Allocate a block of memory that will be tracked in the MemoryManager's page table; this is
 * intended for allocating large blocks of Tungsten memory that will be shared between operators.
 *
 * Returns `null` if there was not enough memory to allocate the page. May return a page that
 * contains fewer bytes than requested, so callers should verify the size of returned pages.
 */
public MemoryBlock allocatePage(long size, MemoryConsumer consumer) {
  assert(consumer != null);
  assert(consumer.getMode() == tungstenMemoryMode);
  if (size > MAXIMUM_PAGE_SIZE_BYTES) {
    throw new IllegalArgumentException(
      "Cannot allocate a page with more than " + MAXIMUM_PAGE_SIZE_BYTES + " bytes");
  }

  // acquireExecutionMemory会去检测是否有足够的内存，
  // 还会尝试将MemoryConsumer溢写到磁盘来释放内存
  long acquired = acquireExecutionMemory(size, consumer);
  if (acquired <= 0) {
    return null;
  }

  final int pageNumber;
  synchronized (this) {
    pageNumber = allocatedPages.nextClearBit(0);
    if (pageNumber >= PAGE_TABLE_SIZE) {
      releaseExecutionMemory(acquired, consumer);
      throw new IllegalStateException(
        "Have already allocated a maximum of " + PAGE_TABLE_SIZE + " pages");
    }
    allocatedPages.set(pageNumber);
  }
  MemoryBlock page = null;
  try {
    page = memoryManager.tungstenMemoryAllocator().allocate(acquired);
  } catch (OutOfMemoryError e) {
    logger.warn("Failed to allocate a page ({} bytes), try again.", acquired);
    // there is no enough memory actually, it means the actual free memory is smaller than
    // MemoryManager thought, we should keep the acquired memory.
    synchronized (this) {
      acquiredButNotUsed += acquired;
      allocatedPages.clear(pageNumber);
    }
    // this could trigger spilling to free some pages.
    return allocatePage(size, consumer);
  }
  page.pageNumber = pageNumber;
  pageTable[pageNumber] = page;
  if (logger.isTraceEnabled()) {
    logger.trace("Allocate page number {} ({} bytes)", pageNumber, acquired);
  }
  return page;
}
```





```scala
// 保存分配的MemoryBlock
private final MemoryBlock[] pageTable = new MemoryBlock[PAGE_TABLE_SIZE];

// 表示生成MemoryBlock的pagenum，这个对应着在列表pageTable的索引
private final BitSet allocatedPages = new BitSet(PAGE_TABLE_SIZE);
```





存储内存

```scala
private[spark] class MemoryStore(

  def putBytes[T: ClassTag](
      blockId: BlockId,
      size: Long,
      memoryMode: MemoryMode,
      _bytes: () => ChunkedByteBuffer): Boolean = {
    require(!contains(blockId), s"Block $blockId is already present in the MemoryStore")
    // 调用acquireStorageMemory申请内存
    if (memoryManager.acquireStorageMemory(blockId, size, memoryMode)) {
      // We acquired enough memory for the block, so go ahead and put it
      // 使用传递的函数，生成ChunkedByteBuffer
      val bytes = _bytes()
      assert(bytes.size == size)
      val entry = new SerializedMemoryEntry[T](bytes, memoryMode, implicitly[ClassTag[T]])
      entries.synchronized {
        entries.put(blockId, entry)
      }
      logInfo("Block %s stored as bytes in memory (estimated size %s, free %s)".format(
        blockId, Utils.bytesToString(size), Utils.bytesToString(maxMemory - blocksMemoryUsed)))
      true
    } else {
      false
    }
  }    
}
```



执行内存



