# Kafka 定时器 #

Kafka在处理请求时，使用了多种延迟任务。而延迟任务需要定时器支持，Kafka自己实现了时间轮，实现定时器功能。关于时间轮的概念，我们可以用常见的钟表来讲述。

时间轮都有着自己的定时范围，以时钟的时针为例，指针每前进一格，代表着时间过去一个小时。它最多有12格，能表示最大延迟时间不超过12个小时的任务。

时间轮可以分等级的，子时间轮的最大延迟时间，刚好为父时间轮的一格。类似于钟表一样。钟表有三个指针，时针，分针，秒针。分钟时间轮能表示的最大延迟时间为60分钟，刚好为小时时间轮的一格。

时间轮的精确时间是取决于最下层的时间轮。以钟表为例，它能精确的时间单位是秒。 

添加任务时，会根据任务的延迟时间，放到不同的时间轮里。比如当前时间是1点钟，现在添加一个延迟任务，它需要延迟1分钟1秒，添加的步骤如下：

1. 首先试图添加到秒时间轮里，但是秒时间轮最大的延迟时间是60秒，超过了最大延迟范围，所以会将任务尝试添加到分钟时间轮
2. 因为分钟时间轮的最大范围是60分钟，没有超过分钟时间轮的范围，所以任务添加到分钟时间轮的第二格。

 

当分针前进一格，到达1点1分，它会检测下一格的任务，会将该时间块的任务列表，取出来添加到秒时间轮。比如刚刚的延迟1分钟1秒的任务，会将它添加秒时间轮的二格。

当时间到达1点1分1秒，秒时间轮会执行该时间块的任务。



## 相关类介绍 ##

TimerTask类表示延迟任务，继承Runnable，子类需要实现run方法。

TimerTaskEntry类表示链表项，它封装了TimerTask。

TimerTaskList表示延迟任务链表，它支持链表的添加和删除操作。它还提供了flush方法，支持执行延迟任务。

```scala
private[timer] class TimerTaskList(taskCounter: AtomicInteger) extends Delayed {
    
  private[this] val root = new TimerTaskEntry(null, -1)

  // 传递的函数，用来执行任务
  def flush(f: (TimerTaskEntry)=>Unit): Unit = {
    synchronized {
      // 遍历链表，执行延迟任务
      var head = root.next
      while (head ne root) {
        remove(head)
        f(head)
        head = root.next
      }
      expiration.set(-1L)
    }
  }
}
```



TimingWheel表示时间轮，它有wheelSize格时间块，每块的时间长度为tickMs。每块对应着一个TimerTaskList，

```scala
// tickMs表示时间轮的时间单位，比如分钟时间轮的时间单位为1分钟
// wheelSize表示有多少格时间，比如分钟时间轮有60格
// startMs表示时间轮的开始时间
private[timer] class TimingWheel(tickMs: Long, wheelSize: Int, startMs: Long, taskCounter: AtomicInteger, queue: DelayQueue[TimerTaskList]) {
  // interval表示延迟的最大时间，比如分钟时间轮为60分钟
  private[this] val interval = tickMs * wheelSize
  // 每格时间都对应着一个任务队列
  private[this] val buckets = Array.tabulate[TimerTaskList](wheelSize) { _ => new TimerTaskList(taskCounter) }
  // 当前时间，它的时间单位为tickMs，这里采用向下取整
  private[this] var currentTime = startMs - (startMs % tickMs)
  // 父时间轮
  @volatile private[this] var overflowWheel: TimingWheel = null

  private[this] def addOverflowWheel(): Unit = {
    synchronized {
      if (overflowWheel == null) {
        overflowWheel = new TimingWheel(
          tickMs = interval,          // 父时间轮的时间单位，为当前时间轮的最大时间
          wheelSize = wheelSize,      // 父时间轮的格数相同
          startMs = currentTime,
          taskCounter = taskCounter,
          queue
        )
      }
    }
  }
    
  // 更新当前时间
  def advanceClock(timeMs: Long): Unit = {
    // 只有过了单位时间，才会更新当前时间
    if (timeMs >= currentTime + tickMs) {
      currentTime = timeMs - (timeMs % tickMs)
      // 更新父时间轮
      if (overflowWheel != null) overflowWheel.advanceClock(currentTime)
    }
  }
    
  // 添加任务，返回是否成功
  def add(timerTaskEntry: TimerTaskEntry): Boolean = {
    val expiration = timerTaskEntry.expirationMs
    if (timerTaskEntry.cancelled) {
      // 如果任务在添加之前，已经被取消掉了
      false
    } else if (expiration < currentTime + tickMs) {
      // 如果任务已经过期了
      false
    } else if (expiration < currentTime + interval) {
      // 计算添加到哪个时间格
      val virtualId = expiration / tickMs
      val bucket = buckets((virtualId % wheelSize.toLong).toInt)
      // 添加任务到对应的任务列表
      bucket.add(timerTaskEntry)

      // 设置任务列表的过期时间
      if (bucket.setExpiration(virtualId * tickMs)) {
        // 将列表添加到DepalyQueue
        queue.offer(bucket)
      }
      true
    } else {
      // 超出了最大延迟时间，需要添加到父时间轮
      if (overflowWheel == null) addOverflowWheel()
      overflowWheel.add(timerTaskEntry)
    }
  }    
}
```



Timer负责管理时间轮，对外提供接口。

```scala
trait Timer {
  // 添加任务
  def add(timerTask: TimerTask): Unit

  // 更新时间，并且提交延迟任务给线程池执行
  def advanceClock(timeoutMs: Long): Boolean
}
```

SystemTimer实现了Timer接口，它使用Java自带的DeplayQueue保存任务队列。这样虽然降低了时间的精准性，但是同样提高了效率，因为DeplayQueue的插入复杂度是O( n log(n) )，而时间轮的插入复杂度是O（1）

```scala
class SystemTimer(executorName: String,
                  tickMs: Long = 1,
                  wheelSize: Int = 20,
                  startMs: Long = Time.SYSTEM.hiResClockMs) extends Timer {

  // 执行任务的线程池
  private[this] val taskExecutor = Executors.newFixedThreadPool(1, new ThreadFactory() {
    def newThread(runnable: Runnable): Thread =
      KafkaThread.nonDaemon("executor-"+executorName, runnable)
  })
  // Java自带的延迟队列
  private[this] val delayQueue = new DelayQueue[TimerTaskList]()
  // 时间轮
  private[this] val timingWheel = new TimingWheel(
    tickMs = tickMs,
    wheelSize = wheelSize,
    startMs = startMs,
    taskCounter = taskCounter,
    delayQueue
  )
  
  // 添加任务
  def add(timerTask: TimerTask): Unit = {
    readLock.lock()
    try {
      addTimerTaskEntry(new TimerTaskEntry(timerTask, timerTask.delayMs + Time.SYSTEM.hiResClockMs))
    } finally {
      readLock.unlock()
    }
  }
  
    
  def advanceClock(timeoutMs: Long): Boolean = {
    // delayQueue查找过期的任务队列
    var bucket = delayQueue.poll(timeoutMs, TimeUnit.MILLISECONDS)
    if (bucket != null) {
      writeLock.lock()
      try {
        while (bucket != null) {
          // 调用时间轮的advanceClock方法，更新该时间轮的时间
          timingWheel.advanceClock(bucket.getExpiration())
          // 对任务列表依次执行reinsert操作
          bucket.flush(reinsert)
          // 继续查看是否还有超时的任务列表
          bucket = delayQueue.poll()
        }
      } finally {
        writeLock.unlock()
      }
      true
    } else {
      false
    }
  }
  
  private[this] val reinsert = (timerTaskEntry: TimerTaskEntry) => addTimerTaskEntry(timerTaskEntry)
  
  private def addTimerTaskEntry(timerTaskEntry: TimerTaskEntry): Unit = {
    
    if (!timingWheel.add(timerTaskEntry)) {
      // 如果是因为任务过期，导致添加失败，那么将任务丢到线程池执行
      if (!timerTaskEntry.cancelled)
        taskExecutor.submit(timerTaskEntry.timerTask)
    }
  }
  
```



## 延迟操作 ##

DelayedOperation表示延迟操作，它在TimerTask的基础上，提供了支持提前完成的功能。





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





DelayedOperationPurgatory提供了延迟操作的线程，它支持提前完成任务

DelayedOperationPurgatory的tryCompleteElseWatch方法用来添加延迟操作

用户会不定时的调用checkAndComplete方法用来尝试提前完成任务







