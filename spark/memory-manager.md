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

HeapMemoryAllocator

直接申请数组

堆外内存

```
UnsafeMemoryAllocator
```

调用Unsafe的allocateMemory方法，分配堆外内存



