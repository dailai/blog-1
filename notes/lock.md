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



 

