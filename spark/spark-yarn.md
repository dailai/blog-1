# Spark 运行在 Yarn #

yarn client模式

ApplicationMaster的启动类为 org.apache.spark.deploy.yarn.ExecutorLauncher





Driver  <<----------------------- ApplicationMaster







ApplicationMaster 运行 AMEndpoint， 接收请求

RequestExecutors： 调用YarnAllocator.requestTotalExecutorsWithPreferredLocalities方法

KillExecutors： 调用 YarnAllocator.killExecutor方法

GetExecutorLossReason ： YarnAllocator.enqueueGetLossReasonRequest方法