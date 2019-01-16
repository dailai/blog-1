# Spark Executor 节点运行原理 #

Spark Executor负责执行从driver发来的任务，它的启动类是CoarseGrainedExecutorBackend。



## CoarseGrainedExecutorBackend ##

CoarseGrainedExecutorBackend负责和driver的通信，包括向driver注册exectuor，接收driver发过来的任务。

CoarseGrainedExecutorBackend的启动过程如下图所示：



CoarseGrainedExecutorBackend继承ThreadSafeRpcEndpoint，实现了Rpc服务，支持下列接口：

- RegisteredExecutor， 注册executor
- LaunchTask， 执行task
- KillTask， 停止task

```scala
private[spark] class CoarseGrainedExecutorBackend
  extends ThreadSafeRpcEndpoint with ExecutorBackend with Logging {
      
  override def receive: PartialFunction[Any, Unit] = {
    // 接收从driver发送来的消息，实例化Executor
    case RegisteredExecutor =>
      logInfo("Successfully registered with driver")
      try {
        executor = new Executor(executorId, hostname, env, userClassPath, isLocal = false)
      } catch {
        case NonFatal(e) =>
          exitExecutor(1, "Unable to create executor due to " + e.getMessage, e)
      }

    // 接收从driver发来的注册失败的消息
    case RegisterExecutorFailed(message) =>
      // 退出进程
      exitExecutor(1, "Slave registration failed: " + message)

    // 接收从driver发来的task
    case LaunchTask(data) =>
      if (executor == null) {
        exitExecutor(1, "Received LaunchTask command but executor was null")
      } else {
        // 反序列化task
        val taskDesc = TaskDescription.decode(data.value)
        logInfo("Got assigned task " + taskDesc.taskId)
        // 提交task给executor执行
        executor.launchTask(this, taskDesc)
      }
      
    case KillTask(taskId, _, interruptThread, reason) =>
      if (executor == null) {
        exitExecutor(1, "Received KillTask command but executor was null")
      } else {
        // 调用executor杀死任务
        executor.killTask(taskId, interruptThread, reason)
      }

    case StopExecutor =>
      stopping.set(true)
      logInfo("Driver commanded a shutdown")
      // Cannot shutdown here because an ack may need to be sent back to the caller. So send
      // a message to self to actually do the shutdown.
      self.send(Shutdown)

    case Shutdown =>
      stopping.set(true)
      new Thread("CoarseGrainedExecutorBackend-stop-executor") {
        override def run(): Unit = {
          // executor.stop() will call `SparkEnv.stop()` which waits until RpcEnv stops totally.
          // However, if `executor.stop()` runs in some thread of RpcEnv, RpcEnv won't be able to
          // stop until `executor.stop()` returns, which becomes a dead-lock (See SPARK-14180).
          // Therefore, we put this line in a new thread.
          executor.stop()
        }
      }.start()
  }
} 
```



## Executor类 ##

上面的CoarseGrainedExecutorBackend类，负责接收driver发过来的Task，然后转交给Executor执行。

###  执行任务 ###

Executor有一个线程池threadPool，负责执行任务。该线程池使用newCachedThreadPool类型，没有线程数目限制。

```scala
class Executor() {
    private val threadPool = {
    	val threadFactory = new ThreadFactoryBuilder()
      		.setDaemon(true)
      		.setNameFormat("Executor task launch worker-%d")
      		.setThreadFactory(new ThreadFactory {
        		override def newThread(r: Runnable): Thread =
          			new UninterruptibleThread(r, "unused")
      		})
      	.build()
    	Executors.newCachedThreadPool(threadFactory).asInstanceOf[ThreadPoolExecutor]
    }
    
    // 执行任务
    def launchTask(context: ExecutorBackend, taskDescription: TaskDescription): Unit = {
        // 实例化TaskRunner
        val tr = new TaskRunner(context, taskDescription)
        runningTasks.put(taskDescription.taskId, tr)
        // 将TaskRunner丢给线程池执行
        threadPool.execute(tr)
  	}
}
```

接下来看看TaskRunner的实现，代码简化如下

```scala
class TaskRunner(
    execBackend: ExecutorBackend,
    private val taskDescription: TaskDescription)
  extends Runnable {
      override def run(): Unit = {
          //  
          
          // 通过ExecutorBackend向driver汇报task已开始运行
          execBackend.statusUpdate(taskId, TaskState.RUNNING, EMPTY_BYTE_BUFFER)
          // 反序列化task
          val ser = env.closureSerializer.newInstance()
          task = ser.deserialize[Task[Any]](
          taskDescription.serializedTask, Thread.currentThread.getContextClassLoader)
          
          // 调用Task的run方法
          val value = task.run(
            taskAttemptId = taskId,
            attemptNumber = taskDescription.attemptNumber,
            metricsSystem = env.metricsSystem)
          
          // 结果序列化
          val resultSer = env.serializer.newInstance()
          val valueBytes = resultSer.serialize(value)
          
		 // 将结果通过ExecutorBackend，发送给driver
          execBackend.statusUpdate(taskId, TaskState.FINISHED, serializedResult)
      }
  }
}
          
```



### 心跳服务 ###

Executor还负责与driver的心跳连接，它启动了后台单线程，定时发送给心跳信息。每次心跳都会携带任务执行的状态信息。

```scala
class Executor() {
    
    // 后台单线程
	private val heartbeater = ThreadUtils.newDaemonSingleThreadScheduledExecutor("driver-heartbeater")
    
    // 构建Heartbeat的rpc客户端
    private val heartbeatReceiverRef = RpcUtils.makeDriverRef(HeartbeatReceiver.ENDPOINT_NAME, conf, env.rpcEnv)
    
    // 启动心跳定时线程
    private def startDriverHeartbeater(): Unit = {
        val heartbeatTask = new Runnable() {
            override def run(): Unit = Utils.logUncaughtExceptions(reportHeartBeat())
    	}
        heartbeater.scheduleAtFixedRate(heartbeatTask, initialDelay, intervalMs, TimeUnit.MILLISECONDS)
    }
    
    // 向driver汇报心跳
    private def reportHeartBeat(): Unit = {
        // 获取所有task运行的信息
        val accumUpdates = new ArrayBuffer[(Long, Seq[AccumulatorV2[_, _]])]()
        for (taskRunner <- runningTasks.values().asScala) {
            if (taskRunner.task != null) {
                accumUpdates += ((taskRunner.taskId, taskRunner.task.metrics.accumulators()))
            }
        }
        // 构建心跳消息
        val message = Heartbeat(executorId, accumUpdates.toArray, env.blockManager.blockManagerId)
        // 向driver发送
        val response = heartbeatReceiverRef.askSync[HeartbeatResponse](
          message, RpcTimeout(conf, "spark.executor.heartbeatInterval", "10s"))
        
}
```



