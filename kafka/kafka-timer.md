# Kafka 延迟任务 #



TimerTaskList表示 TimerTask的链表，提供了链表的添加和删除。还提供了链表中所有任务的回调。它有expiration属性，表示这个链表的过期时间

TimerTaskEntry表示链表项，它封装了TimerTask。

TimingWheel表示时间轮，它有wheelSize块，每块的时间长度为tickMs。每块对应着一个TimerTaskList，

它有一个变量currentTime，表示当前时间。这个当前时间的单位是tickMs。



比如我们有个时间轮，它的时间块长度为一小时。那么



时间轮可以分等级的，类似于钟表一样。钟表有三个指针，时针，分针，秒针。每个指针对应着一个时间轮，当子时间轮旋转一圈后，父时间轮就会前进一格。







Timer提供延迟超时功能，



DelayedOperation表示延迟操作

DelayedOperationPurgatory提供了延迟操作的线程，它支持提前完成任务



```scala
private[server] def maybeTryComplete(): Boolean = {
  var retry = false
  var done = false
  do {
    if (lock.tryLock()) {
      try {
        
        tryCompletePending.set(false)
          // A1
        done = tryComplete()
      } finally {
        lock.unlock()
      }
      
      retry = tryCompletePending.get()
    } else {
        // B1
      retry = !tryCompletePending.getAndSet(true)
      
    }
  } while (!isCompleted && retry)
  done
}
```



maybeTryComplete方法实现得很精巧，它能保证尽量及时的检测任务是否可以完成。如果线程A首先获取锁，但是这时没有满足条件。之后线程B获取锁失败，但是此时说不定满足条件，所以这里需要再次检查条件，至于是哪个线程执行都可以。

如果线程A成功获取锁，它会设置tryCompletePending为false，并且尝试检测任务是否可以提前完成。

如果线程B获取锁失败，说明线程B是在线程A之后运行maybeTryComplete方法的。如果此时线程B设置tryCompletePending为true，在线程A设置tryCompletePending之前，那么线程B会继续尝试获取锁，线程A也会继续获取锁。

如果此时线程B设置tryCompletePending为true，在线程A设置tryCompletePending之后，那么线程



一个线程获取锁，执行tryComplete方法尝试完成，并且设置tryCompletePending为false。

另一个线程获取锁失败，就会获取tryCompletePending的值，取反向，并且将tryCompletePending为true。