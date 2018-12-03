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

