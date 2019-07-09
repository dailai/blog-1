# Spark Sql Exchange 介绍

Exchange 实例在 Spark Sql 中代表着 shuffle 或 broadcast 操作，一般在下列情况下使用：

* broadcast 操作用于 join 操作，当一些 join 实现是采用广播小表的方式，那么就需要broadcast 操作。
* shuffle 操作用于多种情况，比如 GROUP BY 语句需要对某些字段分区，那么就需要对数据进行shuffle。



## 数据分布描述

Distribution 描述的是输入要求，Partitioning 描述的是输出格式。下面是各种类型的格式与要求，以及它们之间满足的条件

| 输入要求 Distribution | 输出格式 Partitioning | 满足条件             |
| --------------------- | --------------------- | -------------------- |
| AllTuples             | SinglePartition       | 满足                 |
| ClusteredDistribution | HashPartitioning      | 分区的字段相同       |
| OrderedDistribution   | RangePartitioning     | 排序的字段和方向相同 |
| BroadcastDistribution | 无                    | 无                   |



## SparkPlan 基类

SparkPlan 作为基类，定义了返回输出格式与输入要求的方法。如果子节点的数据输出格式，不满足输入要求，那么就会触发 Exchange。

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



### GlobalLimitExec

如果是 Limit 语句，那么要求子节点的输出格式为 AllTuples。

```scala
case class GlobalLimitExec(limit: Int, child: SparkPlan) extends BaseLimitExec {

  override def requiredChildDistribution: List[Distribution] = AllTuples :: Nil
}
```



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

BroadcastHashJoinExec 作为 Join 的一种实现，它表示会广播小表，然后按照哈希方式完成。

它需要小表的输出格式满足 BroadcastDistribution 要求。

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

如果是基于shuffle 的哈希方式，那么对两个子节点的输出格式满足 ClusteredDistribution 要求。

```scala
case class ShuffledHashJoinExec() {
  override def requiredChildDistribution: Seq[Distribution] =
    ClusteredDistribution(leftKeys) :: ClusteredDistribution(rightKeys) :: Nil    
}
```



### SortMergeJoinExec

如果是基于排序方式，那么对两个子节点的输出格式满足 ClusteredDistribution 要求。

```scala
case class SortMergeJoinExec() {
  override def requiredChildDistribution: Seq[Distribution] =
    ClusteredDistribution(leftKeys) :: ClusteredDistribution(rightKeys) :: Nil    
}
```



### HashAggregateExec

如果是基于哈希方式的聚合，使用 ClusteredDistribution 要求

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



## 生成 Exchange

如果子节点的输出格式不能满足输入要求，就需要触发 shuffle 过程，生成 Exchange。

```scala
// 遍历子节点
children = children.zip(requiredChildDistributions).map {
  // 检查子节点的数据输出格式是否满足输入要求
  case (child, distribution) if child.outputPartitioning.satisfies(distribution) =>
    child
  // 如果是广播要求，那么生成 BroadcastExchangeExec 实例
  case (child, BroadcastDistribution(mode)) =>
    BroadcastExchangeExec(mode, child)
  // 如果是其他要求，那么生成 ShuffleExchange 实例
  // 并且根据输入要求来生成输出格式
  case (child, distribution) =>
    ShuffleExchange(createPartitioning(distribution, defaultNumPreShufflePartitions), child)
}
```

createPartitioning 负责根据输入要求 Distribution，生成满足条件的输出格式 Partitioning。这里的分区数为 spark.sql.shuffle.partitions 配置项的值，默认为200。

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
```





## BroadcastExchangeExec 原理

BroadcastExchangeExec 仅仅在 join 的时候会使用到，它要求小表的数据大小不能超过 8GB，并且行数不能超过 5.12 亿条。

原理很简单，只是利用了spark core 的广播机制，将小表作为 HashedRelation 类型的变量来广播出去。



## ShuffleExchange 生成 Shuffle 信息

ShuffleExchange 的原理比较复杂， 首先根据输出格式 Partitioning 来创建分区器，然后创建 ShuffleDependency。ShuffleDependency 是用来描述 shuffle 过程的。下面依次介绍不同类型的 Partitioning 对应的分区器



### RoundRobinPartitioning

 它首先会随机挑选出一个分区，然后将数据轮询的分配到每个分区里。



### HashPartitioning

 它对应的分区器原理，是采用了哈希的原理。它会计算每个表达式的值，然后根据这些表达式的值，生成哈希值，最后将哈希值对分区数量取余。



### RangePartitioning

它会实例化 RangePartitioner 分区器，利用它进行采样，生成每个区间的范围。



### SinglePartition

 对应着不分区，也就是所有的数据都在一个分区里



## ExchangeCoordinator

ExchangeCoordinator 作为一个协调器，它会优化分区数量。比如当进行 shuffle 操作之后，有时候会因为分布不均，造成有很多的小分区。ExchangeCoordinator 会将这些小分区合并成一个分区，这样就会减少分区的数量，进而减少 reduce   数量。



### 生成 ExchangeCoordinator

如果开启了 spark.sql.adaptive.enabled，那么在生成完 ShuffleExchange 后，还会生成 ExchangeCoordinator。

但它不是所有的 ShuffleExchange 会被生成 ExchangeCoordinator，需要满足下列任一条件

1. 子节点是 ShuffleExchange 实例，并且输出格式为 HashPartitioning
2. 子节点的输出格式为 HashPartitioning
3. 子节点是 PartitioningCollection 实例（包含了多个Partitioning），且它的每个子节点的输出都是HashPartitioning

最后会将这些子节点全都转为 ShuffleExchange 类型。



### 运行原理

ExcExchangeCoordinator 会调用`sparkContext.submitMapStage(shuffleDependency)`方法，来提交每个ShuffleExchange 子节点生成的 shuffle 任务。然后等待所有的 ShuffleExchange 子节点执行完成，就可以获得这些 shuffle 的输出数据信息，然后根据这些信息来合并数据量小的分区。



我们需要先计算分区的大小阈值，所有合并之后的分区大小不能超过它。计算方法如下：

根据 spark.sql.adaptive.minNumPostShufflePartitions 配置项的值， 它指定了最小的分区数。根据所有子节点的数据和，除以最小分区的数，即可得到每个分区的最大长度。然后同默认的分区大小（spark.sql.adaptive.shuffle.targetPostShuffleInputSize 配置项）相比，取最小值。



### 合并小分区

假设我们有两个 shuffle 的输出信息，里面包含了每个分区的大小。分区的大小阈值为 128MB

```
target input size is 128 MB

* stage 1: [100 MB, 20 MB, 100 MB, 10MB, 30 MB]
* stage 2: [10 MB,  10 MB, 70 MB,  5 MB, 5 MB]
* result : [110MB, 30MB, 170MB, 50MB]
```



分区 0 的和为 110MB，因为 110MB < 128MB，所以等待合并。

分区 1 的和为 30MB ，因为 110MB + 30MB > 128MB，所以分区 0 不能合并。

分区 2， 170MB，因为 30MB + 170MB > 128MB，所以分区 1 不能合并。

分区 3， 15MB，因为 170MB + 15MB > 128MB，所以分区 2  不能合并。

分区 4， 35MB，因为 15MB + 35MB < 128MB，所以等待合并。·

最后遍历完成，所以将分区3 和 分区 4 合并 。



### ShuffledRowRDD

ExchangeCoordinator 会根据合并分区信息，生成 ShuffledRowRDD。它的分区由 ShuffledRowRDDPartition 类表示，它包含了父RDD的起始索引和结束索引。

```scala
private final class ShuffledRowRDDPartition(
    val postShufflePartitionIndex: Int,
    val startPreShufflePartitionIndex: Int,
    val endPreShufflePartitionIndex: Int) extends Partition {
  override val index: Int = postShufflePartitionIndex
}
```

