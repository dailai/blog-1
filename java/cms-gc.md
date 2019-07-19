# CMS GC



<https://www.jianshu.com/p/2329d1c43ceb>

<https://blog.csdn.net/hutongling/article/details/69908443>





## 3.7 CMS算法注意事项:

### 3.7.1 Concurrent Mode Failures

当CMS算法在并发的过程中堆空间无法满足用户程序对新空间的需求时，Stop-the-World的Full GC就会被触发，这就是Concurrent Mode Failures，这通常会造成一个长时间停顿。这种情况通常是因为老年代没有足够的空间供青年代对象promote。（包括没有足够的连续空间）

### 3.7.2 CMS相关JVM参数

- -XX:+UseConcMarkSweepGC：激活CMS收集器，默认情况下使用ParNew + CMS +  Serial Old的收集器组合进行内存回收，Serial Old作为CMS出现“Concurrent Mode Failure”失败后的后备收集器使用。
- -XX:CMSInitiatingOccupancyFraction={x}：在老年代的空间被占用{x}%时，调用CMS算法对老年代进行垃圾回收。
- -XX:CMSFullGCsBeforeCompaction={x}：在进行了{x}次CMS算法之后，对老年代进行一次compaction
- -XX:+CMSPermGenSweepingEnabled & -XX:+CMSClassUnloadingEnabled：让CMS默认遍历永久代（Perm区）
- -XX:ParallelCMSThreads={x}：设置CMS算法中并行线程的数量为{x}。（默认启动(CPU数量+3) / 4个线程。）
- -XX:+ExplicitGCInvokesConcurrent：用户程序中可能出现利用System.gc()触发系统Full GC（将会stop-the-world），利用这个参数可以指定System.gc()直接调用CMS算法做GC。
- -XX:+DisableExplicitGC：该参数直接让JVM忽略用户程序中的System.gc()

