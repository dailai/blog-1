# Scheduler 初始化 #

spark会根据master配置的值，实例化不同的Scheduler。spark支持下列几种master :

* local
* standalone
* yarn
* mesos

实例化的代码在SparkContext的createTaskScheduler方法里。它会对master进行字符串匹配。

```scala
private def createTaskScheduler(
    sc: SparkContext,
    master: String,
    deployMode: String): (SchedulerBackend, TaskScheduler) = {
    
    val MAX_LOCAL_TASK_FAILURES = 1
    master match {
      // 匹配local模式
      case "local" =>
      	val scheduler = new TaskSchedulerImpl(sc, MAX_LOCAL_TASK_FAILURES, isLocal = true)
        val backend = new LocalSchedulerBackend(sc.getConf, scheduler, 1)
        scheduler.initialize(backend)
        (backend, scheduler)
      // 匹配spark://(.*)正则，表示standalone模式
      case SPARK_REGEX(sparkUrl) =>
        val scheduler = new TaskSchedulerImpl(sc)
        val masterUrls = sparkUrl.split(",").map("spark://" + _)
        val backend = new StandaloneSchedulerBackend(scheduler, sc, masterUrls)
        scheduler.initialize(backend)
        (backend, scheduler)
     // 匹配其余的模式，比如yarn，mesos
     case masterUrl =>
        val cm = getClusterManager(masterUrl) match {
          case Some(clusterMgr) => clusterMgr
          case None => throw new SparkException("Could not parse Master URL: '" + master + "'")
        }
        val scheduler = cm.createTaskScheduler(sc, masterUrl)
          val backend = cm.createSchedulerBackend(sc, masterUrl, scheduler)
          cm.initialize(scheduler, backend)
          (backend, scheduler)
          
    }
}
```

注意到上面的getClusterManager方法，它会根据masterurl，返回ExternalClusterManager类。ExternalClusterManager类是实例化Scheduler和SchedulerBackend的工厂类。

getClusterManager类使用了ServiceLoader类，来加载所有的ExternalClusterManager类，从中寻找出可以实例化master的对象。

```scala
private def getClusterManager(url: String): Option[ExternalClusterManager] = {
  val loader = Utils.getContextOrSparkClassLoader
  // 加载ExternalClusterManager类
  // 然后调用canCreate方法，挑选出对应的ExternalClusterManager
  val serviceLoaders =
    ServiceLoader.load(classOf[ExternalClusterManager], loader).asScala.filter(_.canCreate(url))
  if (serviceLoaders.size > 1) {
    throw new SparkException(
      s"Multiple external cluster managers registered for the url $url: $serviceLoaders")
  }
  serviceLoaders.headOption
}
```



说起ServiceLoader可能有点陌生，它的作用是根据resource的资源文件，找到指定类的子类。以yarn为例用法如下：

首先会在resources创建ExternalClusterManager类的配置文件，目录如下:

```shell
resources/
└── META-INF
    └── services
        └── org.apache.spark.scheduler.ExternalClusterManager
```

文件名是要指定的类名，内容格式为每行对应着一个子类名。内容如下

```shell
org.apache.spark.scheduler.cluster.YarnClusterManager
```

再接着看看YarnClusterManager的定义，canCreate用于是否接受此种类型的master。createSchedulerBackend方法和createTaskScheduler方法，都是根据deployMode的类型，返回对应的SchedulerBackend和TaskScheduler。

```scala
private[spark] class YarnClusterManager extends ExternalClusterManager {
   // 匹配master是否等于yarn
   override def canCreate(masterURL: String): Boolean = {
      == "yarn"
   }
}
```

