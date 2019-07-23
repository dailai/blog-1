# Kafka 客户端的缓存管理

## 前言

Kafka 作为一个高吞吐量的消息队列，它的很多设计都体现了这一点。比如它的客户端，无论是 Producer 还是 Consumer    ，都会内置一个缓存存储消息。这样类似于我们写文件时，并不会一次只写一个字节，而是写到一个缓存里，然后等缓存满了，才会将缓存里的数据写入到磁盘。这种缓存机制可以有效的提高吞吐量。本篇文章介绍缓存在 Kafka 客户端的实现原理



## Producer 缓存

首先介绍 Producer 端的缓存实现，它内部实现了一个内存池。当向缓存添加消息时，会先向内存池申请内存。当消息发送完成后，就会向内存池释放内存。

### 内存池使用

我们知道 Producer 发送消息，会先将它存到 RecordAccumulator 的缓存里，存储的格式是将多个消息压缩成一个 batch 存储。

我们通过观察 RecordAccumulator 的 append 接口，可以看到每次缓存消息之前，都会向内存池申请内存。

```java
public final class RecordAccumulator {
    // 消息缓存队列
    private final ConcurrentMap<TopicPartition, Deque<ProducerBatch>> batches;    
    // 内存池
    private final BufferPool free;
    
    public RecordAppendResult append(....) {
        // 计算该消息占用的内存大小    
        int size = Math.max(this.batchSize, AbstractRecords.estimateSizeInBytesUpperBound(maxUsableMagic, compression, key, value, headers));
        // 向内存池申请内存
        buffer = free.allocate(size, maxTimeToBlock);
        .......
    }
}
```



Sender 线程会将缓存的消息batch发送出去。当消息batch发送后，会触发 RecordAccumulator 释放内存。

```java
public final class RecordAccumulator {
    // 内存池
    private final BufferPool free;
    
    public void deallocate(ProducerBatch batch) {
        incomplete.remove(batch);
        // 检测是否该消息batch数据太大了，如果太大了则需要切割。所以这种情况不需要释放内存
        if (!batch.isSplitBatch())
            // 向内存池释放内存
            free.deallocate(batch.buffer(), batch.initialCapacity());
    }    
}
```





### 内存池结构

使用内存池有两个优点，一个是能够限制内存的使用量，另一个是减少内存的申请和回收的频率。虽然 java 支持自动 gc ，但是 gc 是有成本的。为了减少 gc 开销，我们可以预先分配一段常驻内存（不会被回收），然后从这里借用内存，使用后又还给它，这样就没有任何的 gc。

但是内存池的实现有一个难点，那就是如何高效的重新利用。因为每次申请的内存大小都不相同，这样就没办法直接利用了。一种常见的做法是只缓存那些特定大小的内存，这样就很容易实现了复用。我们知道 Kafka 为了提高吞吐量，都是以 batch 格式保存消息。Producer 端实现的内存池，它结合了消息 batch 的特点，试图将每个消息 batch 的大小控制在一定范围内。基于这个原因，Kafka 的内存池分为两部分。一部分是特定大小内存块的缓存池，另一个是非缓存池。







当申请内存的长度等于特定的数值，则优先从缓存池中获取。如果缓存池没有空闲的，那么需要先向非缓存池部分申请内存。等到这块内存使用完后，才会被存储到缓存池。注意到缓存池的大小是可变的，一开始为零。随着用户申请和释放，才慢慢增长起来的。

如果申请的内存不等于特定的数值，则向非缓存池申请。如果内存空间不够用，那么就需要释放缓存池的内存。

缓存池的内存一般都很少回收，除非是内存空间不足。而非缓存池的内存，都是使用后丢弃，等待 gc 回收。



### 内存池实现

```java
public class BufferPool {
    private final long totalMemory;  // 整个内存池的总容量
    private long nonPooledAvailableMemory;  // 非缓存池的空闲大小    
    private final int poolableSize;  // 缓存池中，特定内存的大小
    private final ReentrantLock lock;  // 锁用来防治并发
    private final Deque<ByteBuffer> free;  // 缓存池中的空闲内存块
    private final Deque<Condition> waiters;  // 等待申请的用户

    // 参数size表示申请的内存大小，参数maxTimeToBlockMs表示等待的最长时间
    public ByteBuffer allocate(int size, long maxTimeToBlockMs) throws InterruptedException {
        if (size > this.totalMemory)
            throw new IllegalArgumentException("...")
        ByteBuffer buffer = null;
        this.lock.lock();
        try {
            // 如果申请大小等于指定的值，并且缓存池中有空闲的内存块，则直接返回
            if (size == poolableSize && !this.free.isEmpty())
                return this.free.pollFirst();
            // 计算总的空闲内存大小， 等于缓存池的大小 + 非缓存池的空闲大小
            int freeListSize = freeSize() * this.poolableSize;
            if (this.nonPooledAvailableMemory + freeListSize >= size) {
                // 如果空闲内存足够，那么需要保证非缓存池的空闲空间足够
                // 因为所有的内存分配都是从非缓存池开始
                freeUp(size);
                this.nonPooledAvailableMemory -= size;
            } else {
                // 如果空闲内存不够，那么需要等待别的用户释放内存
                int accumulated = 0;
                Condition moreMemory = this.lock.newCondition();
                try {
                    long remainingTimeToBlockNs = TimeUnit.MILLISECONDS.toNanos(maxTimeToBlockMs);
                    // 添加到等待集合中
                    this.waiters.addLast(moreMemory);
                    // 循环等待
                    while (accumulated < size) {
                        ......
                        // 等待通知
                        waitingTimeElapsed = !moreMemory.await(remainingTimeToBlockNs, TimeUnit.NANOSECONDS);
                    }
                        
```



当多个用户并发申请内存，是通过锁控制的。里面包含了一个等待的用户队列，队列采用了先进先出的方式。



## Consumer 缓存







## 避免大数据重新分配







