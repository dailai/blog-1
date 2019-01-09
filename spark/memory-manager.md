# Spark 内存管理 #

spark为了更加高效的使用内存，自己来管理内存。spark支持堆内内存分配和堆外内存分配，



## 内存分配 ##

spark分配内存是以内存块为单位。内存块包含内存地址和内存大小。

### 内存块 ###

内存地址是由MemoryLocation类表示的。它存储着两部分数据，一个是头部，一个是数据。

```java
public class MemoryLocation {

  @Nullable
  Object obj;       // 内存的起始地址

  long offset;     // 数据的偏移位置
}
```

内存块由MemoryBlock表示，它除了包含了内存地址，还有内存大小。

```java
public class MemoryBlock extends MemoryLocation {
  private final long length;     //  内存中的数据大小
}
```



### 内存分配接口 ###

MemoryAllocator定义了内存分配的接口。HeapMemoryAllocator类实现了MemoryAllocator接口，支持在堆内分配内存。UnsafeMemoryAllocator则支持在堆外分配内存。

### 堆内分配 ###

HeapMemoryAllocator对于大的数据块，会缓存下来，注意这里是弱引用。当大的内存用完后，但是没有被jvm回收之前，会提供给新的需求。这样可以尽量的减少 jvm 垃圾回收。

HeapMemoryAllocator的分配，只是实例化一个Long类型的数组。

```java
public class HeapMemoryAllocator implements MemoryAllocator {
  // 缓存池， Key的内存块的大小， Value为内存块的弱引用
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
            // 返回弱引用
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
    // 新建数组，数组是属于堆内内存
    // 注意size是指定分配内存的字节数，一个long占有8个字节
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

HeapMemoryAllocator的释放内存方法，并没有正在的释放，只是保存了它的弱引用。

```java
@Override
public void free(MemoryBlock memory) {
  final long size = memory.size();
  if (MemoryAllocator.MEMORY_DEBUG_FILL_ENABLED) {
    memory.fill(MemoryAllocator.MEMORY_DEBUG_FILL_FREED_VALUE);
  }
  if (shouldPool(size)) {
    synchronized (this) {
      LinkedList<WeakReference<MemoryBlock>> pool = bufferPoolsBySize.get(size);
      if (pool == null) {
        pool = new LinkedList<>();
        bufferPoolsBySize.put(size, pool);
      }
      pool.add(new WeakReference<>(memory));
    }
  } else {
    // Do nothing
  }
}
```



### 堆外分配  ###

UnsafeMemoryAllocator调用Platform的allocateMemory方法，分配堆外内存。

调用Platform的freeMemory方法，释放堆外内存。

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
    
  @Override
  public void free(MemoryBlock memory) {
    assert (memory.obj == null) :
      "baseObject not null; are you trying to use the off-heap allocator to free on-heap memory?";
    if (MemoryAllocator.MEMORY_DEBUG_FILL_ENABLED) {
      memory.fill(MemoryAllocator.MEMORY_DEBUG_FILL_FREED_VALUE);
    }
    Platform.freeMemory(memory.offset);
  }
}
```

接下来继续研究Platform的源码，Platform其实是调用了Unsafe来分配和释放堆外内存的。

```java
public final class Platform {
  // 使用了Unsafe类来分配堆外内存
  private static final Unsafe _UNSAFE;
  // 通过反射获取Unsafe实例
  static {
    sun.misc.Unsafe unsafe;
    try {
      Field unsafeField = Unsafe.class.getDeclaredField("theUnsafe");
      unsafeField.setAccessible(true);
      unsafe = (sun.misc.Unsafe) unsafeField.get(null);
    } catch (Throwable cause) {
      unsafe = null;
    }
    _UNSAFE = unsafe;
  }
    
  // 调用Unsafe分配内存
  public static long allocateMemory(long size) {
    return _UNSAFE.allocateMemory(size);
  }

  // 调用Unsafe释放内存
  public static void freeMemory(long address) {
    _UNSAFE.freeMemory(address);
  }
}
```

 

## 内存管理 ##

### 内存池  ###

内存池负责管理内存，它只负责管理下列的数值，比如内存总大小，剩余大小，已使用的大小。但它不负责实际的内存分配。 

内存池的用处分为两种，一个是用作存储的，一部分用作执行任务的。

MemoryPool是内存池的基类，它只是简单的提供了，有锁访问内存信息的方法。

```scala
abstract class MemoryPool(lock: Object) {

  @GuardedBy("lock")
  private[this] var _poolSize: Long = 0
    
  // 返回内存的容量
  final def poolSize: Long = lock.synchronized {
    _poolSize
  }

  // 返回剩余的容量
  final def memoryFree: Long = lock.synchronized {
    _poolSize - memoryUsed
  }

  // 增大内存容量
  final def incrementPoolSize(delta: Long): Unit = lock.synchronized {
    require(delta >= 0)
    _poolSize += delta
  }

  // 减少内存容量
  final def decrementPoolSize(delta: Long): Unit = lock.synchronized {
    require(delta >= 0)
    require(delta <= _poolSize)
    require(_poolSize - delta >= memoryUsed)
    _poolSize -= delta
  }

  // 返回已经使用的内存大小
  def memoryUsed: Long
}
```



StorageMemoryPool 继承 MemoryPool， 增加申请内存接口。它支持堆外和堆内内存，由memoryMode参数指定。

```scala
private[memory] class StorageMemoryPool(
    lock: Object,
    memoryMode: MemoryMode       // 指定存储在堆内还是堆外
  ) extends MemoryPool(lock) with Logging {
    
    def acquireMemory(blockId: BlockId, numBytes: Long): Boolean = lock.synchronized {
        // 计算需要额外释放多少内存，才能满足需求
        val numBytesToFree = math.max(0, numBytes - memoryFree)
        acquireMemory(blockId, numBytes, numBytesToFree)
    }
     
    def acquireMemory(
      blockId: BlockId,
      numBytesToAcquire: Long,
      numBytesToFree: Long): Boolean = lock.synchronized {
        // 如果需要额外释放内存，则调用memoryStore将其他block的数据存储到磁盘
        if (numBytesToFree > 0) {
            memoryStore.evictBlocksToFreeSpace(Some(blockId), numBytesToFree, memoryMode)
        }
        // 检查是否已经有足够的内存
        val enoughMemory = numBytesToAcquire <= memoryFree
        // 如果有，则增加内存的使用量
        if (enoughMemory) {
            _memoryUsed += numBytesToAcquire
        }
        enoughMemory
    }
    
    // 返回已使用的内存容量
    override def memoryUsed: Long = lock.synchronized {
      _memoryUsed
    }
 
}
```



ExecutionMemoryPool继承 MemoryPool， 增加了申请内存接口。它支持堆外和堆内内存，由memoryMode参数指定。

```scala
class ExecutionMemoryPool(
    lock: Object,
    memoryMode: MemoryMode
  ) extends MemoryPool(lock) with Logging {

  // 每个任务对应的已分配的内存大小
  private val memoryForTask = new mutable.HashMap[Long, Long]()

  // maybeGrowPool 函数， 在统一内存管理模式下，会从部分存储内存转到执行内存下
  // computeMaxPoolSize函数， 计算容量的最大值
  private[memory] def acquireMemory(
      numBytes: Long,
      taskAttemptId: Long,
      maybeGrowPool: Long => Unit = (additionalSpaceNeeded: Long) => Unit,
      computeMaxPoolSize: () => Long = () => poolSize): Long = lock.synchronized {
    
    while (true) {
      // 已经运行的task的数目
      val numActiveTasks = memoryForTask.keys.size
      // 当前任务已经分配的内存
      val curMem = memoryForTask(taskAttemptId)

      // 尝试增大内存容量
      maybeGrowPool(numBytes - memoryFree)

      // 实时计算容量的最大值
      val maxPoolSize = computeMaxPoolSize()
      // 计算每个任务的最大和最小的内存
      val maxMemoryPerTask = maxPoolSize / numActiveTasks
      val minMemoryPerTask = poolSize / (2 * numActiveTasks)

      // 计算此次任务最大的可以分配内存， 每个任务的内存不能超过maxMemoryPerTask
      val maxToGrant = math.min(numBytes, math.max(0, maxMemoryPerTask - curMem))
      // 计算可以分配的最大内存， 
      val toGrant = math.min(maxToGrant, memoryFree)

      // 如果当前内存不够，并且分配以后当前任务的内存仍然小于minMemoryPerTask
      // 那么等待资源释放
      if (toGrant < numBytes && curMem + toGrant < minMemoryPerTask) {
        logInfo(s"TID $taskAttemptId waiting for at least 1/2N of $poolName pool to be free")
        lock.wait()
      } else {
        // 如果内存足够，则返回已分配的内存数目
        memoryForTask(taskAttemptId) += toGrant
        return toGrant
      }
    }
    // 否则返回0
    0L  // Never reached
  }
  
  // 释放任务的内存
  def releaseMemory(numBytes: Long, taskAttemptId: Long): Unit = lock.synchronized {
    val curMem = memoryForTask.getOrElse(taskAttemptId, 0L)
    // 计算任务可以释放的内存
    var memoryToFree = if (curMem < numBytes) {
      logWarning(
        s"Internal error: release called on $numBytes bytes but task only has $curMem bytes " +
          s"of memory from the $poolName pool")
      curMem
    } else {
      numBytes
    }
    
    // 更新memoryForTask表
    if (memoryForTask.contains(taskAttemptId)) {
      memoryForTask(taskAttemptId) -= memoryToFree
      if (memoryForTask(taskAttemptId) <= 0) {
        memoryForTask.remove(taskAttemptId)
      }
    }
    // 通知等待内存释放的线程
    lock.notifyAll() // Notify waiters in acquireMemory() that memory has been freed
  }
}


  
```



### 管理策略 ###

MemoryManager 负责管理下列内存池

* onHeapStorageMemoryPool ： StorageMemoryPool，  堆内
* offHeapStorageMemoryPool   :   StorageMemoryPool， 堆外
* onHeapExecutionMemoryPool  :  ExecutionMemoryPool， 堆内
* offHeapExecutionMemoryPool  :  ExecutionMemoryPool， 堆外

对于内存的管理，spark支持两种策略，静态资源管理，动态资源管理。



#### 静态资源管理 ####

静态资源管理会初始化各个内存池的大小，之后内存池的大小不能改变。

静态资源管理不支持用于存储的堆外内存。

内存分为两个用途，一个是缓存数据使用的，另一部分是任务执行使用的。

getMaxStorageMemory方法

```scala
private[spark] object StaticMemoryManager {

  private val MIN_MEMORY_BYTES = 32 * 1024 * 1024

  // 返回存储数据的内存容量
  private def getMaxStorageMemory(conf: SparkConf): Long = {
    val systemMaxMemory = conf.getLong("spark.testing.memory", Runtime.getRuntime.maxMemory)
    val memoryFraction = conf.getDouble("spark.storage.memoryFraction", 0.6)
    val safetyFraction = conf.getDouble("spark.storage.safetyFraction", 0.9)
    (systemMaxMemory * memoryFraction * safetyFraction).toLong
  }

  // 返回执行任务使用的内存容量
  private def getMaxExecutionMemory(conf: SparkConf): Long = {
    val systemMaxMemory = conf.getLong("spark.testing.memory", Runtime.getRuntime.maxMemory)
    val memoryFraction = conf.getDouble("spark.shuffle.memoryFraction", 0.2)
    val safetyFraction = conf.getDouble("spark.shuffle.safetyFraction", 0.8)
    (systemMaxMemory * memoryFraction * safetyFraction).toLong
  }

}
```







```scala
private[spark] class StaticMemoryManager(
    conf: SparkConf,
    maxOnHeapExecutionMemory: Long,
    override val maxOnHeapStorageMemory: Long,
    numCores: Int)
  extends MemoryManager(
    conf,
    numCores,
    maxOnHeapStorageMemory,
    maxOnHeapExecutionMemory) {

  def this(conf: SparkConf, numCores: Int) {
    this(
      conf,
      StaticMemoryManager.getMaxExecutionMemory(conf),  // 从配置文件读取和计算execution的内存容量
      StaticMemoryManager.getMaxStorageMemory(conf),    // 从配置文件读取和计算storage的内存容量
      numCores)
  }

  // 不支持堆外的存储内存，所以会将offHeapStorageMemoryPool的内存，
  // 转到offHeapExecutionMemoryPool里面
  offHeapExecutionMemoryPool.incrementPoolSize(offHeapStorageMemoryPool.poolSize)
  offHeapStorageMemoryPool.decrementPoolSize(offHeapStorageMemoryPool.poolSize)
  
  override def acquireStorageMemory(
      blockId: BlockId,
      numBytes: Long,
      memoryMode: MemoryMode): Boolean = synchronized {
    // 不支持堆外的存储内存
    require(memoryMode != MemoryMode.OFF_HEAP,
      "StaticMemoryManager does not support off-heap storage memory")
    // 如果申请内存大于堆内内存，则直接返回false，表示申请内存失败
    if (numBytes > maxOnHeapStorageMemory) {
      // Fail fast if the block simply won't fit
      logInfo(s"Will not store $blockId as the required space ($numBytes bytes) exceeds our " +
        s"memory limit ($maxOnHeapStorageMemory bytes)")
      false
    } else {
      // 向onHeapStorageMemoryPool申请内存
      onHeapStorageMemoryPool.acquireMemory(blockId, numBytes)
    }
  }
  
  private[memory]
  override def acquireExecutionMemory(
      numBytes: Long,
      taskAttemptId: Long,
      memoryMode: MemoryMode): Long = synchronized {
    memoryMode match {
      case MemoryMode.ON_HEAP => onHeapExecutionMemoryPool.acquireMemory(numBytes, taskAttemptId)
      case MemoryMode.OFF_HEAP => offHeapExecutionMemoryPool.acquireMemory(numBytes, taskAttemptId)
    }
  }
  
}
```



#### 动态资源管理 ####

```
UnifiedMemoryManager
```





## 内存消费端 ##





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



任务内存管理

```
TaskMemoryManager
```



```
allocatedPages 记录分配的内存块
```





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

```
final class ShuffleExternalSorter extends MemoryConsumer {
  public void insertRecord(Object recordBase, long recordOffset, int length, int partitionId)
  throws IOException {

    // for tests
    assert(inMemSorter != null);
    if (inMemSorter.numRecords() >= numElementsForSpillThreshold) {
      logger.info("Spilling data because number of spilledRecords crossed the threshold " +
        numElementsForSpillThreshold);
      spill();
    }

    growPointerArrayIfNecessary();
    // Need 4 bytes to store the record length.
    final int required = length + 4;
    acquireNewPageIfNecessary(required);
    
  }
  
  
  private void growPointerArrayIfNecessary() throws IOException {
    assert(inMemSorter != null);
    if (!inMemSorter.hasSpaceForAnotherRecord()) {
      long used = inMemSorter.getMemoryUsage();
      LongArray array;
      try {
        // could trigger spilling
        array = allocateArray(used / 8 * 2);
      } catch (OutOfMemoryError e) {
        // should have trigger spilling
        if (!inMemSorter.hasSpaceForAnotherRecord()) {
          logger.error("Unable to grow the pointer array");
          throw e;
        }
        return;
      }
      // check if spilling is triggered or not
      if (inMemSorter.hasSpaceForAnotherRecord()) {
        freeArray(array);
      } else {
        inMemSorter.expandPointerArray(array);
      }
    }
  }

  /**
   * Allocates more memory in order to insert an additional record. This will request additional
   * memory from the memory manager and spill if the requested memory can not be obtained.
   *
   * @param required the required space in the data page, in bytes, including space for storing
   *                      the record size. This must be less than or equal to the page size (records
   *                      that exceed the page size are handled via a different code path which uses
   *                      special overflow pages).
   */
  private void acquireNewPageIfNecessary(int required) {
    if (currentPage == null ||
      pageCursor + required > currentPage.getBaseOffset() + currentPage.size() ) {
      // TODO: try to find space in previous pages
      currentPage = allocatePage(required);
      pageCursor = currentPage.getBaseOffset();
      allocatedPages.add(currentPage);
    }
  }
```







MemoryManager只负责管理内存的数量，而不负责实际的分配的

MemoryConsumer 提供了实际的分配内存接口。allocateArray 和 allocatePage。这些都是转发给TaskMemoryManager

TaskMemoryManager 获取是实际的内存分配，但它只负责申请execution用途的内存

