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



## TaskLocality 相关类 ##

* myLocalityLevels， 类型  ArrayBuffer[TaskLocality.TaskLocality]。表示tasks里的TaskLocality集合。
* localityWaits， 类型 Array[Long]。表示TaskLocaolity的停留时间，索引与myLocalityLevels相同
* currentLocalityIndex， 表示此时的TaskLocality在myLocalityLevels中对应的索引

myLocalityLevels是有存储顺序的，它是按照上面TaskLocality列表的顺序。每个Task有一个TaskLocality，当运行环境迟迟不能满足时，它的TaskLocality就会自动降级。

当新增或减少一个资源的时候，就会触发上面属性的更新。



## 任务管理 ##

TaskSetManager负责管理一组Task的执行情况。它有下列重要属性：

* tasks : Array[Task]，管理的任务集合
*  successful : Array[Boolean] ，索引和tasks相同，表示此任务是否完成，注意这里完成包含两种情况： 失败和成功
* tasksSuccessful ： Int， 完成的任务数量
* numFailures :  Array[Int]， 索引和tasks相同， 表示此失败的次数
* taskAttempts : Array[  List[ TaskInfo ]  ]，索引和tasks相同，表示此任务每次重试的信息列表
* taskInfos : HashMap[Long, TaskInfo]，key为taskId
* runningTasksSet :  HashSet[Long]，表示正在运行任务的taskId
* isZombie : bool，表示暂停状态。进入到这个状态，就不能启动新的任务。当所有的任务已经完成的时候，或者调用abort还拿书，或者遇到FetchFailed异常时，都会触发进入暂停状态。

### 任务失败 ###

当Task任务失败时，会调用handleFailedTask。

```
def handleFailedTask(tid: Long, state: TaskState, reason: TaskFailedReason) {
    val info = taskInfos(tid)
    removeRunningTask(tid)
    // 标记taskInfo失败
    info.markFinished(state, clock.getTimeMillis())
    // 获取任务在tasks数组中的的索引
    val index = info.index
    val failureReason = s"Lost task ${info.id} in stage ${taskSet.id} (TID $tid, ${info.host}," +
      s" executor ${info.executorId}): ${reason.toErrorString}"
    val failureException: Option[Throwable] = reason match {
      // 如果是FetchFailed一次，标记此任务完成
      case fetchFailed: FetchFailed =>
        logWarning(failureReason)
        if (!successful(index)) {
          successful(index) = true
          tasksSuccessful += 1
        }
        // 进入暂停状态
        isZombie = true
        None
    case ef: ExceptionFailure =>
      // 如果是数据不能序列化，则调用abort退出
      if (ef.className == classOf[NotSerializableException].getName) {
        abort("Task %s in stage %s (TID %d) had a not serializable result: %s".format(
            info.id, taskSet.id, tid, ef.description))
      }
      ef.exception
    case e: ExecutorLostFailure if !e.exitCausedByApp =>
      // 由于非用户程序造成的问题,不计入失败次数
      None
    // TaskResultLost, TaskKilled 和其余的异常
    case e: TaskFailedReason =>  
      logWarning(failureReason)
      None
   }
   //通知dagScheduler任务完成
   sched.dagScheduler.taskEnded(tasks(index), reason, null, accumUpdates, info)
   
   if (successful(index)) {
      logInfo(s"Task ${info.id} in stage ${taskSet.id} (TID $tid) failed, but the task will not" +
        s" be re-executed (either because the task failed with a shuffle data fetch failure," +
        s" so the previous stage needs to be re-run, or because a different copy of the task" +
        s" has already succeeded).")
   } else {
      // 重新添加到任务队列里
      addPendingTask(index)
   }
   
   
   if (!isZombie && reason.countTowardsTaskFailures) {
     // 记录任务失败次数
     numFailures(index) += 1
     // 检测任务重试的次数是否大于指定值
     if (numFailures(index) >= maxTaskFailures) {
       // 调用abort终止任务
       abort("Task %d in stage %s failed %d times, most recent failure: %s\nDriver stacktrace:"
          .format(index, taskSet.id, maxTaskFailures, failureReason), failureException)
       return
     }
   }
   // 检测是否整个任务集完成
   maybeFinishTaskSet()
 }
     
   
   
    
    
    
```

### 任务成功 ###

当Task任务成功时，会调用handleSuccessfulTask。

```
def handleSuccessfulTask(tid: Long, result: DirectTaskResult[_]): Unit = {
  val info = taskInfos(tid)
  // 获取任务在tasks数组中的的索引
  val index = info.index
  // 标记taskInfo成功
  info.markFinished(TaskState.FINISHED, clock.getTimeMillis())
  // 从runningTasksSet集合中，删除掉此任务
  removeRunningTask(tid)
  // 如果有还有重试任务，则把重试任务kill掉
  for (attemptInfo <- taskAttempts(index) if attemptInfo.running) {
    // 调用SchedulerBackend的killTask接口，杀死任务
   	sched.backend.killTask(attemptInfo.taskId, attemptInfo.executorId,
   	  interruptThread = true, reason = "another attempt succeeded")
  }
  // 如果任务之前没有成功，则更新tasksSuccessful数目，和successful数组
  // 如果任务都已经成功，则标记isZombie为true
  if (!successful(index)) {
    tasksSuccessful += 1
   	successful(index) = true
   	if (tasksSuccessful == numTasks) {
   	  isZombie = true
   	}
  }
  // 调用DagScheduler通知任务已经成功
  sched.dagScheduler.taskEnded(tasks(index), Success, result.value(), result.accumUpdates, info)
  // 每一次任务成功，都会调用maybeFinishTaskSet查看是否整个任务集是否成功
  
  maybeFinishTaskSet()
}

def maybeFinishTaskSet() {
  // 只有当所有的任务都已完成，并且重试的任务都已经结束，才认为是完成
  if (isZombie && runningTasks == 0) {
    sched.taskSetFinished(this)
  }
}
   	
```

### 获取任务 ###

当有新的资源添加进来时，会调用resourceOffer获取任务。任务分配的原理：

1. 获取tasks的最低条件TaskLocality

2. 依次检查各个TaskLocality（按照上面TaskLocality列表的顺序）对应的task，是否有任务要执行

3. 依次从各个TaskLocality（按照上面TaskLocality列表的顺序）对应的task，取出任务




## 疑问 ##

executorId怎么跟PROCESS_LOCAL相关。指定PROCESS_LOCAL，需要绑定在哪个executor上吗

