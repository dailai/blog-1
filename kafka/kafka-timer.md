# Kafka 延迟任务 #



TimerTaskList表示 TimerTask的链表，提供了链表的添加和删除。还提供了链表中所有任务的回调。它有expiration属性，表示这个链表的过期时间

TimerTaskEntry表示链表项，它封装了TimerTask。

TimingWheel表示时间轮，它有wheelSize块，每块的时间长度为tickMs。每块对应着一个TimerTaskList，

它有一个变量currentTime，表示当前时间。这个当前时间的单位是tickMs。



比如我们有个时间轮，它的时间块长度为一小时。那么



时间轮可以分等级的，类似于钟表一样。钟表有三个指针，时针，分针，秒针。每个指针对应着一个时间轮，当子时间轮旋转一圈后，父时间轮就会前进一格。



