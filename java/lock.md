# Java 内置锁的使用情况





## CountDownLatch

CountDownLatch 适合一次性使用的场景，用在线程同步的场景。比如线程 A 需要满足三种条件，才能进行下一步。这三个条件由其他线程来完成，这个情形就特别适合 CountDownLatch。

首先实例化数值为 3 的 CountDownLatch 实例，然后线程 A 调用 CountDownLatch 的 await 方法。然后其余线程在完成条件时，调用 CountDownLatch 的 countDown 方法。

CountDownLatch 属于共享锁



## Semaphore

适用于资源共享的场景。比如我们有 5 个资源，多个线程会抢着使用资源。当资源不够时，会等待别人用完。

Semaphore 属于共享锁





## ReentrantLock

用于保证临界区的原子性。它属于独享锁，只有获得锁的线程，才能执行释放操作。









## AbstractQueuedSynchronizer

这些锁都是 AbstractQueuedSynchronizer 的子类，提供了 CAS 操作，和等待队列的方法。

```java
public abstract class AbstractQueuedSynchronizer {
    
    private volatile int state;

    // 双向链表节点
    static final class Node {
        volatile int waitStatus;     // 等待状态，有已取消，等待中， PROPAGATE，初始状态
        volatile Node prev;		    // 前节点
        volatile Node next;         // 后节点
        volatile Thread thread;     // 线程
        Node nextWaiter;            // 表示当前锁类型，是独占锁还是共享锁
    }
    
    // 链表头节点
    private transient volatile Node head;
    // 链表尾节点
    private transient volatile Node tail;
}
```



独享锁

AbstractQueuedSynchronizer 提供了两个方法，供独享锁调用。

```java
public abstract class AbstractQueuedSynchronizer
    
    // 获取资源
    public final void acquire(int arg) {
        if (!tryAcquire(arg) &&
            acquireQueued(addWaiter(Node.EXCLUSIVE), arg))
            selfInterrupt();
    }

    // 释放资源
    public final boolean release(int arg) {
        if (tryRelease(arg)) {
            Node h = head;
            if (h != null && h.waitStatus != 0)
                unparkSuccessor(h);
            return true;
        }
        return false;
    }
}
```



独享锁必须实现 tryAcquire 方法，实现获取锁的功能。以 ReentrantLock 为例，它获取锁的操作，是通过 CAS 操作使 state 属性的值加一。state 的值为 0 时，表示没有线程获取锁。否则表示已经有其他线程获取到锁了。

如果没有成功获取到锁，就需要将此线程添加到链表中，当然操作链表也需要实现并发。首先会实例化一个节点，然后通过 CAS 操作 tail 属性的值，来添加到链表的尾部。

成功添加到链表后，还会将从当前节点向前遍历，遍历的同时会将状态为已取消的节点删除掉。如果前节点的状态不是已取消，那么就需要通过CAS 操作将前节点的状态更改为等待中。如果前节点的状态为等待中，那么就调用 LockSupport.park 方法使自身线程休眠。 

这里需要注意下，线程调用 park 方法会将自身阻塞，虽然能响应interrupt事件，但不会抛出InterruptedException异常。如果发生了interrupt 事件， 线程被重新唤醒后，会去检查 interrupt 标识，并且自身调用 interrupt 方法。



独享锁还必须实现 tryRelease 方法，实现释放锁的功能。以 ReentrantLock 为例，它释放锁的操作，是通过 CAS 操作将state 属性的值减一。当 state 的值为 0时，就会返回 true。

如果成功释放了锁，那么就从头部开始查找，找到第一个状态不为已取消的节点。然后调用 LockSupport.unpark 方法唤醒节点对应的线程。





共享锁

AbstractQueuedSynchronizer 提供了两个方法，供共享锁调用。

```java
public abstract class AbstractQueuedSynchronizer {
    // 获取资源
    public final void acquireShared(int arg) {
        if (tryAcquireShared(arg) < 0)
            doAcquireShared(arg);
    }

    // 释放资源
    public final boolean releaseShared(int arg) {
        if (tryReleaseShared(arg)) {
            doReleaseShared();
            return true;
        }
        return false;
    }    
}
```



共享锁必须实现 tryAcquireShared 方法，实现获取资源的功能。如果获取资源失败，那么就会将此线程添加到链表中。添加过程和阻塞线程的原理，同独享锁是一样。



共享锁必须实现 tryReleaseShared 方法，实现释放资源的功能。这里如果成功释放资源，那么就会依次释放所有等待的线程。这就是共享锁的特点，所有等待的线程都会共享通知。

