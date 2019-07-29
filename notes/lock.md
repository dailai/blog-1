乐观锁实现

使用了CAS原理，下面使用了AtomicBoolean类，因为它的原理也是使用了CAS，这里直接拿来使用。

```java
 class GlobalSessionSpinLock {
        
        private AtomicBoolean globalSessionSpinLock = new AtomicBoolean(true);

        public void lock() throws TransactionException {
            boolean flag;
            do {
                flag = this.globalSessionSpinLock.compareAndSet(true, false);
            }
            while (!flag);
        }


        public void unlock() {
            this.globalSessionSpinLock.compareAndSet(false, true);
        }
    }
```



支持超时限制

```java
public void lock() throws TransactionException {
            boolean flag;
            int times = 1;
            long beginTime = System.currentTimeMillis();
            long restTime = GLOBAL_SESSOION_LOCK_TIME_OUT_MILLS ;
            do {
                restTime -= (System.currentTimeMillis() - beginTime);
                if (restTime <= 0){
                    throw new TransactionException(TransactionExceptionCode.FailedLockGlobalTranscation);
                }
                // Pause every PARK_TIMES_BASE times,yield the CPU
                if (times % PARK_TIMES_BASE == 0){
                    // Exponential Backoff
                    long backOffTime =  PARK_TIMES_BASE_NANOS << (times/PARK_TIMES_BASE);
                    long parkTime = backOffTime < restTime ? backOffTime : restTime;
                    LockSupport.parkNanos(parkTime);
                }
                flag = this.globalSessionSpinLock.compareAndSet(true, false);
                times++;
            }
            while (!flag);
        }
```

- 引入了超时机制，一般来说一个要做好这种对临界区域加锁一定要做好超时机制，尤其是在这种对性能要求较高的中间件中。
- 引入了锁膨胀机制，这里没循环一定次数如果获取不到锁，那么会线程挂起`parkTime`时间，挂起之后又继续循环获取，如果再次获取不到，此时我们会对我们的parkTime进行指数退避形式的挂起，将我们的挂起时间逐渐增长，直到超时。



 



Thread的isInterrupted成员方法，简单的判断是否被中断

Thread的interrupted静态方法，返回当前线程是否被中断，并且会将interrupt状态复位

当线程进入阻塞状态，被调用interrupt方法，会抛出InterruptedException异常。在catch异常后，会自动将interrupt状态复位

synchronized 不支持超时，也不支持中断

wait方法会将监控锁释放，并且将当前线程放入监控锁的等待池中。

notify只通知等待池中的一个线程进入到锁池，而notifyall会通知等待池中的所有线程进入到锁池。

wait方法需要在while循环里，while的条件需要再次判断条件是否满足。因为有多个线程在等待池中，被notifyall都唤醒，这些线程都会进入锁池去竞争锁，这样第一个获取的线程是满足条件的。但之后的线程不一定满足，所以需要在while里面再次判断条件，如果不满足，继续调用wait方法。

wait原理

```java
wait() {
   unlock(mutex);//解锁mutex
   wait_condition(condition);//等待内置条件变量condition
   lock(mutex);//竞争锁
}
```

  

一般使用方式

```java
synchronized(obj) { // 此处相当于mutex=obj，lock(mutex)
    while(!cond) {// 判断外部条件cond，不满足时让线程wait();
        obj.wait();
    }
    
    ..... // 执行满足条件cond时的逻辑过程
}
```



它首先将当前线程加入到唤醒队列，然后旋即解锁mutex，最后等待被唤醒。被唤醒后，又对mutex加锁（可能是看起来没有对用户的行为作任何的改变）。

这里的关键在于：将线程加入唤醒队列后方可解锁。保证了线程在陷入wait后至被加入唤醒队列这段时间内是原子的。以免出现唤醒丢失问题。