# Spark Sql Exchange 介绍

Spark Sql 会根据子节点的数据输出

## 数据分布描述

Distribution 描述的是输入要求，Partitioning 描述的是输出格式。下面是各种类型的格式与要求，以及它们之间满足的条件



| 输入要求 Distribution | 输出格式 Partitioning | 满足条件             |
| --------------------- | --------------------- | -------------------- |
| AllTuples             | SinglePartition       | 满足                 |
| ClusteredDistribution | HashPartitioning      | 分区的字段相同       |
| OrderedDistribution   | RangePartitioning     | 排序的字段和方向相同 |





## SparkPlan 基类

SparkPlan 作为基类，定义了返回输出格式与输入要求的方法。

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





## 对输入有要求的节点



### SortExec

SortExec 作为实现排序功能的节点。如果指定全局排序（比如使用 ORDER BY 语句），那么需要子节点的输出格式满足 OrderedDistribution 要求。

```scala
case class SortExec() {
  override def requiredChildDistribution: Seq[Distribution] =
    if (global) OrderedDistribution(sortOrder) :: Nil else UnspecifiedDistribution :: Nil
  }
}
```



### BroadcastHashJoinExec

BroadcastHashJoinExec 作为 Join 的一种实现，它表示会广播小表，然后按照hash的方式完成。

它需要子节点的输出格式满足 BroadcastDistribution 要求。

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



### ShuffledHashJoinExec

如果是基于shuffle 的 join 方式，那么对两个子节点的输出格式满足 ClusteredDistribution 要求。

```scala
case class ShuffledHashJoinExec() {
  override def requiredChildDistribution: Seq[Distribution] =
    ClusteredDistribution(leftKeys) :: ClusteredDistribution(rightKeys) :: Nil    
}
```



### SortMergeJoinExec

如果是基于排序 join 方式，那么对两个子节点的输出格式满足 ClusteredDistribution 要求。

```scala
case class SortMergeJoinExec() {
  override def requiredChildDistribution: Seq[Distribution] =
    ClusteredDistribution(leftKeys) :: ClusteredDistribution(rightKeys) :: Nil    
}
```



### HashAggregateExec

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



### GlobalLimitExec

如果是 Limit 语句，那么要求子节点的输出格式为 AllTuples。

```scala
case class GlobalLimitExec(limit: Int, child: SparkPlan) extends BaseLimitExec {

  override def requiredChildDistribution: List[Distribution] = AllTuples :: Nil
}
```



## 生成 Exchange

如果子节点的输出格式，不能满足输入要求。就需要生成 Exchange，负责数据的重新分布，也就是 shuffle 过程。



## BroadcastExchangeExec

BroadcastExchangeExec 仅仅在 join 的时候会使用到，

它的原理是利用了spark core 的广播机制，将小表作为 HashedRelation 类型的变量来广播出去。

小表的数据大小不能超过 8GB，并且行数不能超过 5.12 亿条。



## ShuffleExchange

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





## ExchangeCoordinator



如果开启了 spark.sql.adaptive.enabled，那么 ShuffleExchange 会被实例化为 ExchangeCoordinator。

但它不是所有的 ShuffleExchange 会被生成ExchangeCoordinator，需要满足下列任一条件

1. 子节点是 ShuffleExchange 实例，并且输出格式为 HashPartitioning
2. 子节点的输出格式为 HashPartitioning
3. 子节点是 PartitioningCollection 实例，且它的每个子节点的输出都是HashPartitioning

会将子节点都转换为ShuffleExchange实例





运行原理



首先等待所有的 ShuffleExchange 子节点，完成执行。然后根据这些 shuffle 的输出数据信息，来合并数据量小的分区。





target input size 的计算方式

根据 spark.sql.adaptive.minNumPostShufflePartitions 配置项的值， 它指定了最小的分区数。根据所有子节点的数据和，除以最小分区的数，即可得到每个分区的最大长度。然后同默认的分区大小（spark.sql.adaptive.shuffle.targetPostShuffleInputSize 配置项）相比，取最小值。





```
* stage 1: [100 MB, 20 MB, 100 MB, 10MB, 30 MB]
* stage 2: [10 MB,  10 MB, 70 MB,  5 MB, 5 MB]
```

target input size is 128 MB



110MB

分区0， 30MB ，因为 110MB + 30MB > 128MB，所以切片 110MB。

分区1， 170MB，因为 30MB + 170MB > 128MB，所以切片 30MB。

分区2， 15MB，因为 170MB + 15MB > 128MB，所以切片 170MB。

分区3， 35MB，因为 15MB + 35MB < 128MB，所以不切片。

已经遍历完了，所以最后一份切片是 15MB + 35MB  。



ShuffledRowRDDPartition 分区信息，包含了父RDD的起始索引和结束索引。

ShuffledRowRDD 会根据分区信息，从父RDD的多个分区中拉取数据。



```scala
private final class ShuffledRowRDDPartition(
    val postShufflePartitionIndex: Int,
    val startPreShufflePartitionIndex: Int,
    val endPreShufflePartitionIndex: Int) extends Partition {
  override val index: Int = postShufflePartitionIndex
}
```

