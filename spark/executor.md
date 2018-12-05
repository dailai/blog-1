# executor原理 #

spark的driver将任务分发给各个executor节点，然后由executor执行任务。这个过程主要有两个主要的类，Executor和CoarseGrainedExecutorBackend。

## CoarseGrainedExecutorBackend ##

CoarseGrainedExecutorBackend负责和driver的通信，包括向driver注册exectuor，接收driver发过来的任务。

CoarseGrainedExecutorBackend继承ThreadSafeRpcEndpoint，实现了Rpc服务，支持下列接口：

* RegisteredExecutor， 注册executor
* LaunchTask， 执行task
* KillTask， 停止task

CoarseGrainedExecutorBackend的启动过程

1. 向driver拉取spark 配置
2. 创建 executor的 SparkEnv
3. 注册rpc服务CoarseGrainedExecutorBackend
4. 向driver请求RegisterExecutor
5. 接收dirver的RegisteredExecutor回应，实例化Executor
6. 接收driver的LaunchTask的请求， 执行Task

## Executor类 ##

上面的CoarseGrainedExecutorBackend类，负责接收driver发过来的Task，然后转交给Executor执行。

Executor有一个线程池threadPool，负责执行任务。该线程池使用newCachedThreadPool类型，没有线程数目限制。

```scala
private[spark] class Executor() {
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

 TaskRunnable的执行原理，简化如下

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
          
```



Executor还负责与driver的心跳连接，每次心跳都会携带任务执行的状态信息。它启动了后台单线程，定时发送给心跳信息。

```scala
private[spark] class Executor() {
    // 后台单线程
	private val heartbeater = ThreadUtils.newDaemonSingleThreadScheduledExecutor("driver-heartbeater")
    // 构建Heartbeat的rpc客户端
    private val heartbeatReceiverRef = RpcUtils.makeDriverRef(HeartbeatReceiver.ENDPOINT_NAME, conf, env.rpcEnv)
    
    private def startDriverHeartbeater(): Unit = {
        val heartbeatTask = new Runnable() {
            override def run(): Unit = Utils.logUncaughtExceptions(reportHeartBeat())
    	}
        heartbeater.scheduleAtFixedRate(heartbeatTask, initialDelay, intervalMs, TimeUnit.MILLISECONDS)
    }
    
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

