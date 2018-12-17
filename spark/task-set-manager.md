# TaskSetManager 分配任务 #

## TaskLocation ##

TaskLocation表示 task运行的计算节点。它有下列值：

* ExecutorCacheTaskLocation， 表示在指定的executor里运行
* HostTaskLocation， 表示在指定服务器上运行
* HDFSCacheTaskLocation， 表示在指定的服务器上运行，这台服务器有着hdfs文件数据的缓存

## TaskLocality ##

TaskLocality表示 task 的计算节点和 task 的输入数据的节点位置关系，它有下列值：

* PROCESS_LOCAL， 表示在同一个Executor进程
* NODE_LOCAL， 表示在同一台服务器
* NO_PREF， 表示PROCESS_LOCAL或者NODE_LOCAL都可以
* RACK_LOCAL， 表示在同一个机架
* ANY， 表示任意位置都行

## TaskSetManager 相关属性 ##

TaskSetManager有一些集合，存储着TaskLocality对应的Task。

* tasks， 类型为Array[ Task[ _ ] ]。表示要执行的任务，这个值在TaskSetManager初始化的时候，就已经指定了，不可修改。

* pendingTasksForExecutor， 类型为HashMap[String, ArrayBuffer[Int]]。Key为 executorId， ArrayBuffer存储着这个Task在tasks数组的索引。这个集合表示每个Executor运行的任务。
* pendingTasksForHost，  类型为HashMap[String, ArrayBuffer[Int]]。Key为host，这个集合表示每台服务器运行的任务
* pendingTasksForRack， 类型为HashMap[String, ArrayBuffer[Int]]。Key为rackId，这个集合表示每个机架运行的任务
* pendingTasksWithNoPrefs, 类型为 ArrayBuffer[Int]。这个集合包含没有指定位置的 task
* allPendingTasks， 类型为 ArrayBuffer[Int]。这个集合包含了所有的 task，注意到这里面Task的顺序是与tasks相反

## 初始化任务集合 ##

一个TaskSetManager对应着一个TaskSet，而TaskSet是对应着stageId的Task集合。TaskSetManager类初始化时，会接收TaskSet参数。然后会根据Task的执行位置，更新相应的集合。

```
private[spark] class TaskSetManager(
    sched: TaskSchedulerImpl,
    val taskSet: TaskSet,
    val maxTaskFailures: Int,
    blacklistTracker: Option[BlacklistTracker] = None,
    clock: Clock = new SystemClock()) extends Schedulable with Logging {

  val tasks = taskSet.tasks
  val numTasks = tasks.length
  
  // 调用addPendingTask， 依次添加任务
  // 注意这里是逆序添加，所以allPendingTasks的顺序与tasks相反
  for (i <- (0 until numTasks).reverse) {
    addPendingTask(i)
  }

  private def addPendingTask(index: Int) {
    // 根据Task的TaskLocation，更新集合
    for (loc <- tasks(index).preferredLocations) {
      loc match {
        // 如果指定了executor上运行，则添加到pendingTasksForExecutor集合
        case e: ExecutorCacheTaskLocation =>
          pendingTasksForExecutor.getOrElseUpdate(e.executorId, new ArrayBuffer) += index
        // 如果是HDFSCacheTaskLocation类型，则获取此台服务器上的所有executor
        // 然后将executor添加到pendingTasksForExecutor
        case e: HDFSCacheTaskLocation =>
          // 调用TaskSchedulerImpl获取这台服务器上的所有executor
          val exe = sched.getExecutorsAliveOnHost(loc.host)
          exe match {
            case Some(set) =>
              for (e <- set) {
                pendingTasksForExecutor.getOrElseUpdate(e, new ArrayBuffer) += index
              }
            case None => logDebug(s"Pending task $index has a cached location at ${e.host} " +
                ", but there are no executors alive there.")
          }
        case _ =>
      }
      // 根据Task所在的host，将Task添加到pendingTasksForHost集合
      pendingTasksForHost.getOrElseUpdate(loc.host, new ArrayBuffer) += index
      // 根据host返回其所在的rack， 然后添加到pendingTasksForRack集合里
      for (rack <- sched.getRackForHost(loc.host)) {
        pendingTasksForRack.getOrElseUpdate(rack, new ArrayBuffer) += index
      }
    }
	// 如果task没有指定运行位置，则添加到pendingTasksWithNoPrefs集合
    if (tasks(index).preferredLocations == Nil) {
      pendingTasksWithNoPrefs += index
    }
	// 添加到allPendingTasks集合
    allPendingTasks += index
  }
```

