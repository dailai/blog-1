# scheduler 调度原理 #

Spark提交将任务的时候，有自己的调度算法，来负责优先执行哪些任务。

## 基本数据结构 ##

调度的基类由Schedulable表示，它可以包含多个Schedulable，他们之间的关系是树状结构。Schedulable有两个子类，TaskSetManager表示叶子节点，Pool表示非叶子节点。

Schedulable有几个重要的方法

* schedulableQueue， 子节点列表

* schedulingMode， 调度算法， Fair，FIFO

* weight， 资源分配的权重比

* minShare， 最小分配的资源数

* runningTasks， 正在运行的任务数

* priority， 优先值

* stageId， 所属的stage

* addSchedulable，添加schedulable子节点

* getSortedTaskSetQueue， 获取排序的结果

* minShare， 最小资源数

* runningTasks， 运行任务的数目


## Scheduler 初始化 ##

 TaskSchedulerImpl负责实例化 scheduler 算法。TaskSchedulerImpl首先会新建一个rootPool，然后使用SchedulableBuilder来配置rootPool。

```scala
private[spark] class TaskSchedulerImpl {
    
	val rootPool: Pool = new Pool("", schedulingMode, 0, 0)
	
	def initialize(backend: SchedulerBackend) {
    	this.backend = backend
        schedulableBuilder = {
          schedulingMode match {
            case SchedulingMode.FIFO =>
              new FIFOSchedulableBuilder(rootPool)
            case SchedulingMode.FAIR =>
              new FairSchedulableBuilder(rootPool, conf)
            case _ =>
              throw new IllegalArgumentException(s"Unsupported $SCHEDULER_MODE_PROPERTY: " +
              s"$schedulingMode")
          }
        }
        schedulableBuilder.buildPools()
      }
}
```

FIFO实例化很简单，只有一个rootPool，所有的任务都添加到这个Pool里。

```scala
private[spark] trait SchedulableBuilder {
  def rootPool: Pool

  def buildPools(): Unit

  def addTaskSetManager(manager: Schedulable, properties: Properties): Unit
}
```

FairSchedulableBuilder会根据配置文件，由spark.scheduler.allocation.file配置指定路径， 生成相应的Pool，添加到顶层Pool。文件定义了Pool的name，weight， minShare属性。

```scala
private def buildFairSchedulerPool(is: InputStream, fileName: String) {
  // 读取配置xml文件
  val xml = XML.load(is)
  //遍历pool配置列表
  for (poolNode <- (xml \\ POOLS_PROPERTY)) {
    // 获取对应的属性值
    val poolName = (poolNode \ POOL_NAME_PROPERTY).text
    val schedulingMode = getSchedulingModeValue(poolNode, poolName,
      DEFAULT_SCHEDULING_MODE, fileName)
    val minShare = getIntValue(poolNode, poolName, MINIMUM_SHARES_PROPERTY,
      DEFAULT_MINIMUM_SHARE, fileName)
    val weight = getIntValue(poolNode, poolName, WEIGHT_PROPERTY,
      DEFAULT_WEIGHT, fileName)
	// 实例化Pool， 并且添加到rootPool里
    rootPool.addSchedulable(new Pool(poolName, schedulingMode, minShare, weight))

    logInfo("Created pool: %s, schedulingMode: %s, minShare: %d, weight: %d".format(
      poolName, schedulingMode, minShare, weight))
  }
}
```

## 调度算法 ##

### 添加任务 ###

当配置好rootPool后，会通过schedulableBuilder的addTaskSetManager提交任务给rootPool。不同的调度算法，添加也有区别。

FIFOSchedulableBuilder仅仅是简单的添加到rootPool

```scala
class FIFOSchedulableBuilder(val rootPool: Pool) {
  override def addTaskSetManager(manager: Schedulable, properties: Properties) {
    rootPool.addSchedulable(manager)
  }
}
```

FairSchedulableBuilder会根据指定的pool name，插入到对应的Pool里面

```scala
private[spark] class FairSchedulableBuilder() {
  override def addTaskSetManager(manager: Schedulable, properties: Properties) {
    // propertie指定了要插入到哪个pool的name
    val poolName = if (properties != null) {
        properties.getProperty(FAIR_SCHEDULER_PROPERTIES, DEFAULT_POOL_NAME)
      } else {
        DEFAULT_POOL_NAME
      }
    // 查看rootPool是否有这个Pool
    // 如果没有，则需要创建
    var parentPool = rootPool.getSchedulableByName(poolName)
    if (parentPool == null) {
      parentPool = new Pool(poolName, DEFAULT_SCHEDULING_MODE,
        DEFAULT_MINIMUM_SHARE, DEFAULT_WEIGHT)
      rootPool.addSchedulable(parentPool)
   
    }
    parentPool.addSchedulable(manager)
  }
}
```

### 排序原理 ###

当添加完任务后，需要调用Pool的getSortedTaskSetQueue方法，返回排序后的任务列表。

Pool会先对子节点排序，然后依次获取子节点的排序结果。它的终止条件是遇到叶子节点TaskSetManager

```scala
override def getSortedTaskSetQueue: ArrayBuffer[TaskSetManager] = {
  var sortedTaskSetQueue = new ArrayBuffer[TaskSetManager]
  val sortedSchedulableQueue =
    schedulableQueue.asScala.toSeq.sortWith(taskSetSchedulingAlgorithm.comparator)
  for (schedulable <- sortedSchedulableQueue) {
    sortedTaskSetQueue ++= schedulable.getSortedTaskSetQueue
  }
  sortedTaskSetQueue
}
```

对于TaskSetManager的获取排序任务，这里是仅仅返回了包含自身的列表

```scala
override def getSortedTaskSetQueue(): ArrayBuffer[TaskSetManager] = {
  var sortedTaskSetQueue = new ArrayBuffer[TaskSetManager]()
  sortedTaskSetQueue += this
  sortedTaskSetQueue
}
```

注意到上面的taskSetSchedulingAlgorithm，这里定义了排序的规则。



FIFO调度的比较原理，首先比较priority值，priority越小，则优先执行。如果priority相同，则比较stageId，stageId越小，则优先执行。

```scala
class FIFOSchedulingAlgorithm extends SchedulingAlgorithm {
  override def comparator(s1: Schedulable, s2: Schedulable): Boolean = {
    val priority1 = s1.priority
    val priority2 = s2.priority
    var res = math.signum(priority1 - priority2)
    if (res == 0) {
      val stageId1 = s1.stageId
      val stageId2 = s2.stageId
      res = math.signum(stageId1 - stageId2)
    }
    res < 0
  }
}
```

Fair调度相对来说比较复杂，它的中心思想是，根据资源的使用情况来排序的。这里有两个衡量指标

资源的饱和度 = runningTasks /  minShare。当在资源充足时， 这个值越大，说明资源越紧张。

资源的使用率 = runningTasks /  weight，表示每个资源正在跑的任务数。当资源缺乏时， 这个值越大，说明资源越紧张。

```scala
private[spark] class FairSchedulingAlgorithm extends SchedulingAlgorithm {
  override def comparator(s1: Schedulable, s2: Schedulable): Boolean = {
    val minShare1 = s1.minShare
    val minShare2 = s2.minShare
    val runningTasks1 = s1.runningTasks
    val runningTasks2 = s2.runningTasks
    // 如果正在跑的任务，小于最小资源数，则说明这个Schedulable还有空闲资源
    val s1Needy = runningTasks1 < minShare1
    val s2Needy = runningTasks2 < minShare2
    // 计算资源的饱和度
    val minShareRatio1 = runningTasks1.toDouble / math.max(minShare1, 1.0)
    val minShareRatio2 = runningTasks2.toDouble / math.max(minShare2, 1.0)
    // 计算资源的使用率
    val taskToWeightRatio1 = runningTasks1.toDouble / s1.weight.toDouble
    val taskToWeightRatio2 = runningTasks2.toDouble / s2.weight.toDouble

    var compare = 0
    // 如果只有一个Schedulable有空闲资源
    if (s1Needy && !s2Needy) {
      return true
    } else if (!s1Needy && s2Needy) {
      return false
    // 如果两个都有空闲资源，则选择资源饱和度小的
    } else if (s1Needy && s2Needy) {
      compare = minShareRatio1.compareTo(minShareRatio2)
    } else {
    // 如果两个都没有空闲资源，则选择资源使用率低的
      compare = taskToWeightRatio1.compareTo(taskToWeightRatio2)
    }
    if (compare < 0) {
      true
    } else if (compare > 0) {
      false
    } else {
      s1.name < s2.name
    }
  }
}
```


