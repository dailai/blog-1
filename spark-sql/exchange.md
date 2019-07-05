

```scala
abstract class SparkPlan extends QueryPlan[SparkPlan] with Logging with Serializable {

  /** Specifies how data is partitioned across different nodes in the cluster. */
  def outputPartitioning: Partitioning = UnknownPartitioning(0) // TODO: WRONG WIDTH!
  
  /** Specifies any partition requirements on the input data for this operator. */
  def requiredChildDistribution: Seq[Distribution] =
    Seq.fill(children.size)(UnspecifiedDistribution)

  /** Specifies how data is ordered in each partition. */
  def outputOrdering: Seq[SortOrder] = Nil

  /** Specifies sort order for each partition requirements on the input data for this operator. */
  def requiredChildOrdering: Seq[Seq[SortOrder]] = Seq.fill(children.size)(Nil)
}
```





Distribution 描述的是输入要求

Partitioning 描述的是输出格式



| 输入要求 Distribution | 输出格式 Partitioning | 满足条件             |
| --------------------- | --------------------- | -------------------- |
| AllTuples             | SinglePartition       |                      |
| ClusteredDistribution | HashPartitioning      | 分区的字段相同       |
| OrderedDistribution   | RangePartitioning     | 排序的字段和方向相同 |







根据输入要求 Distribution， 创建出满足要求的输出 Partitioning。

```scala
private def createPartitioning(
    requiredDistribution: Distribution,
    numPartitions: Int): Partitioning = {
  requiredDistribution match {
    case AllTuples => SinglePartition
    case ClusteredDistribution(clustering) => HashPartitioning(clustering, numPartitions)
    case OrderedDistribution(ordering) => RangePartitioning(ordering, numPartitions)
    case dist => sys.error(s"Do not know how to satisfy distribution $dist")
  }
}
```







如果是全局排序，那么需要子节点的输出格式满足 OrderedDistribution 要求

```scala
case class SortExec() {
  override def requiredChildDistribution: Seq[Distribution] =
    if (global) OrderedDistribution(sortOrder) :: Nil else UnspecifiedDistribution :: Nil
  }
}
```

如果是广播表的 join 方式，那么需要对一个子节点的输出格式满足 BroadcastDistribution 要求

```scala
case class BroadcastHashJoinExec() {
  override def requiredChildDistribution: Seq[Distribution] = {
    val mode = HashedRelationBroadcastMode(buildKeys)
    buildSide match {
      case BuildLeft =>
        BroadcastDistribution(mode) :: UnspecifiedDistribution :: Nil
      case BuildRight =>
        UnspecifiedDistribution :: BroadcastDistribution(mode) :: Nil
    }
  }    
}
```



如果是基于shuffle 的 join 方式，那么对两个子节点的输出格式满足 ClusteredDistribution 要求。

```scala
case class ShuffledHashJoinExec() {
  override def requiredChildDistribution: Seq[Distribution] =
    ClusteredDistribution(leftKeys) :: ClusteredDistribution(rightKeys) :: Nil    
}
```



如果是基于排序 join 方式，那么对两个子节点的输出格式满足 ClusteredDistribution 要求。

```scala
case class SortMergeJoinExec() {
  override def requiredChildDistribution: Seq[Distribution] =
    ClusteredDistribution(leftKeys) :: ClusteredDistribution(rightKeys) :: Nil    
}
```



如果是基于hash方式的聚合，使用 ClusteredDistribution 要求

```scala
case class HashAggregateExec() {
  override def requiredChildDistribution: List[Distribution] = {
    requiredChildDistributionExpressions match {
      case Some(exprs) if exprs.isEmpty => AllTuples :: Nil
      case Some(exprs) if exprs.nonEmpty => ClusteredDistribution(exprs) :: Nil
      case None => UnspecifiedDistribution :: Nil
    }
  }
}
```



如果是 Limit 语句，那么要求子节点的输出格式为 AllTuples。

```scala
case class GlobalLimitExec(limit: Int, child: SparkPlan) extends BaseLimitExec {

  override def requiredChildDistribution: List[Distribution] = AllTuples :: Nil
}
```





如果子节点的输出格式，不能满足输入要求。就需要生成 Exchange，负责数据的重新分布，也就是 shuffle 过程。





BroadcastExchangeExec

BroadcastExchangeExec 仅仅在 join 的时候会使用到，

它的原理是利用了spark core 的广播机制，将小表作为 HashedRelation 类型的变量来广播出去。

小表的数据大小不能超过 8GB，并且行数不能超过 5.12 亿条。



ShuffleExchange

ShuffleExchange 首先根据 Partitioning 来创建分区器，然后创建出 ShuffleDependency。ShuffleDependency 是用来描述 shuffle 过程的。



RoundRobinPartitioning ，随机开始，然后轮询

```scala
var position = new Random(TaskContext.get().partitionId()).nextInt(numPartitions)

position += 1
position
```



HashPartitioning 对应的分区器，它的分区原理如下

分别计算每个表达式的值，然后根据这些表达式的值，计算出哈希值，最后对哈希值对分区数量取余。



RangePartitioning 对应于 RangePartitioner。先进行采样，生成每个区间的范围

```scala
val rddForSampling = rdd.mapPartitionsInternal { iter =>
  val mutablePair = new MutablePair[InternalRow, Null]()
  iter.map(row => mutablePair.update(row.copy(), null))
}
implicit val ordering = new LazilyGeneratedOrdering(sortingExpressions, outputAttributes)
new RangePartitioner(numPartitions, rddForSampling, ascending = true)
```



SinglePartition 对应着不分区，也就是所有的数据都在一个分区里

```scala
new Partitioner {
  override def numPartitions: Int = 1
  override def getPartition(key: Any): Int = 0
}
```



这些 shuffle 过程中的数据序列化，都是 UnsafeRowSerializer 类负责。



SparkPlan 执行顺序

```scala
prepare()
waitForSubqueries()
doExecute()
```





查看ShuffleExchange的执行流程，

prepare 会向ExchangeCoordinator 注册自身

waitForSubqueries 会等待子查询完成

doExecute，如果有ExchangeCoordinator，那么调用ExchangeCoordinator的方法生成ShuffledRowRDD。



