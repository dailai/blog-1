# Spark Sql 数据统计



Statistics 类表示数据统计，包含数据源的大小，条数和各列的统计数

```scala
case class Statistics(
    sizeInBytes: BigInt,
    rowCount: Option[BigInt] = None,
    attributeStats: AttributeMap[ColumnStat] = AttributeMap(Nil),
    hints: HintInfo = HintInfo()) 
```

hints 用于需要 join 的时候，表示是否要采用广播表的方式。

ColumnStat 类表示各列的数据统计

```scala
case class ColumnStat(
    distinctCount: BigInt,  // 值不重复的的条数
    min: Option[Any],  // 最大值
    max: Option[Any],  // 最小值
    nullCount: BigInt, // 值为null的条数
    avgLen: Long,  // 平均字节长度
    maxLen: Long)  // 最大字节长度
```



computeStats 会计算出当前 plan 的数据统计，用于指导后续的优化。

```scala
abstract class LogicalPlan extends QueryPlan[LogicalPlan] with Logging {

  protected def computeStats(conf: SQLConf): Statistics = {
    if (children.isEmpty) {
      throw new UnsupportedOperationException(s"LeafNode $nodeName must implement statistics.")
    }
    Statistics(sizeInBytes = children.map(_.stats(conf).sizeInBytes).product)
  }
}
```



LogicalPlan 的默认实现是计算子节点的数据统计，然后将他们的数据大小值相乘。所以叶子节点必须要实现computeStats 方法。

子类 UnaryNode 表示它只有一个子节点。它复写了 computeStats方法

```scala
abstract class UnaryNode extends LogicalPlan {

  override def computeStats(conf: SQLConf): Statistics = {
    // 计算子节点的行数据大小
    val childRowSize = child.output.map(_.dataType.defaultSize).sum + 8
    // 计算当前节点的输出行数据大小
    val outputRowSize = output.map(_.dataType.defaultSize).sum + 8
    // 按照比例，计算统计数据
    var sizeInBytes = (child.stats(conf).sizeInBytes * outputRowSize) / childRowSize
    if (sizeInBytes == 0) {
      // sizeInBytes can't be zero, or sizeInBytes of BinaryNode will also be zero
      // (product of children).
      sizeInBytes = 1
    }

    // 返回数据统计，这里只是统计了数据大小这一个值
    Statistics(sizeInBytes = sizeInBytes, hints = child.stats(conf).hints)
  }
}
```





首先来看看各种叶子节点的数据统计原理



### Project 节点

```scala
case class Project(projectList: Seq[NamedExpression], child: LogicalPlan) extends UnaryNode {
  override def computeStats(conf: SQLConf): Statistics = {
    if (conf.cboEnabled) {
      // 如果允许cbo优化，那么就调用ProjectEstimation的统计方法
      ProjectEstimation.estimate(conf, this).getOrElse(super.computeStats(conf))
    } else {
      // 调用了UnaryNode父类的统计方法
      super.computeStats(conf)
    }
  }
}
```

ProjectEstimation 的统计原理，它必须知道子节点的数据行数，才能进完成统计。

```scala
object ProjectEstimation {
  import EstimationUtils._

  def estimate(conf: SQLConf, project: Project): Option[Statistics] = {
    if (rowCountsExist(conf, project.child)) {
      // 如果子节点有条数的统计值
      val childStats = project.child.stats(conf)
      val inputAttrStats = childStats.attributeStats
      // 从子节点的列中，生成 Alias 实例
      val aliasStats = project.expressions.collect {
        case alias @ Alias(attr: Attribute, _) if inputAttrStats.contains(attr) =>
          alias.toAttribute -> inputAttrStats(attr)
      }
      // 通过输出列的大小，计算出每行的数据大小。然后乘以行数，即可计算出总的大小
      val outputAttrStats =
        getOutputMap(AttributeMap(inputAttrStats.toSeq ++ aliasStats), project.output)
      Some(childStats.copy(
        sizeInBytes = getOutputSize(project.output, childStats.rowCount.get, outputAttrStats),
        attributeStats = outputAttrStats))
    } else {
      None
    }
  }
}
```



## Filter 节点

它的计算也分为两种

* 允许 cbo 优化，调用 FilterEstimation 的统计方法
* 不允许 cbo 优化，调用 UnaryNode 的统计方法

FilterEstimation 的统计方法有点复杂，



## Union 节点

计算子节点统计数据的和



## Join 节点

如果允许cbo优化，那么就会调用 JoinEstimation 的统计方法。否则就执行 simpleEstimation 方法。

```scala
case class Join(
    left: LogicalPlan,
    right: LogicalPlan,
    joinType: JoinType,
    condition: Option[Expression])
  extends BinaryNode with PredicateHelper {
  
  override def computeStats(conf: SQLConf): Statistics = {
    def simpleEstimation: Statistics = joinType match {
      case LeftAnti | LeftSemi =>
        // 因为LeftAnti或LeftSemi类型的join，生成的数据是子节点left的子集
        left.stats(conf)
      case _ =>
        // 调用UnaryNode的统计方法
        val stats = super.computeStats(conf)
        stats.copy(hints = stats.hints.resetForJoin())
    }

    if (conf.cboEnabled) {
      // 如果允许cbo优化，那么采用JoinEstimation的统计方法
      JoinEstimation.estimate(conf, this).getOrElse(simpleEstimation)
    } else {
      simpleEstimation
    }
  }
}  
```



JoinEstimation 的统计方法比较复杂



## Aggregate 节点

如果允许cbo优化，那么就会调用 AggregateEstimation 的统计方法。否则就执行 simpleEstimation 方法。

```scala
case class Aggregate(
    groupingExpressions: Seq[Expression],
    aggregateExpressions: Seq[NamedExpression],
    child: LogicalPlan)
  extends UnaryNode {
  
  override def computeStats(conf: SQLConf): Statistics = {
    def simpleEstimation: Statistics = {
      if (groupingExpressions.isEmpty) {
        // 这种是使用了GROUPING SETS语句
        Statistics(
          sizeInBytes = EstimationUtils.getOutputSize(output, outputRowCount = 1),
          rowCount = Some(1),
          hints = child.stats(conf).hints)
      } else {
        // 调用UnaryNode的统计方法
        super.computeStats(conf)
      }
    }

    if (conf.cboEnabled) {
      AggregateEstimation.estimate(conf, this).getOrElse(simpleEstimation)
    } else {
      simpleEstimation
    }
  }
}  
```



 AggregateEstimation 的统计方法

```scala
object AggregateEstimation {
  import EstimationUtils._

  /**
   * Estimate the number of output rows based on column stats of group-by columns, and propagate
   * column stats for aggregate expressions.
   */
  def estimate(conf: SQLConf, agg: Aggregate): Option[Statistics] = {
    val childStats = agg.child.stats(conf)
    // Check if we have column stats for all group-by columns.
    val colStatsExist = agg.groupingExpressions.forall { e =>
      e.isInstanceOf[Attribute] && childStats.attributeStats.contains(e.asInstanceOf[Attribute])
    }
    if (rowCountsExist(conf, agg.child) && colStatsExist) {
      // Multiply distinct counts of group-by columns. This is an upper bound, which assumes
      // the data contains all combinations of distinct values of group-by columns.
      var outputRows: BigInt = agg.groupingExpressions.foldLeft(BigInt(1))(
        (res, expr) => res * childStats.attributeStats(expr.asInstanceOf[Attribute]).distinctCount)

      outputRows = if (agg.groupingExpressions.isEmpty) {
        // If there's no group-by columns, the output is a single row containing values of aggregate
        // functions: aggregated results for non-empty input or initial values for empty input.
        1
      } else {
        // Here we set another upper bound for the number of output rows: it must not be larger than
        // child's number of rows.
        outputRows.min(childStats.rowCount.get)
      }

      val outputAttrStats = getOutputMap(childStats.attributeStats, agg.output)
      Some(Statistics(
        sizeInBytes = getOutputSize(agg.output, outputRows, outputAttrStats),
        rowCount = Some(outputRows),
        attributeStats = outputAttrStats,
        hints = childStats.hints))
    } else {
      None
    }
  }
}
```





