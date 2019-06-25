# Spark Sql 数据统计

## 前言

计算出当前 plan 的数据统计，用于指导后续的优化。

## 数据统计结果

Statistics 类表示数据统计，包含数据源的大小，条数和各列的统计数

```scala
case class Statistics(
    sizeInBytes: BigInt,    // 数据的字节大小
    rowCount: Option[BigInt] = None,    // 数据的总行数
    attributeStats: AttributeMap[ColumnStat] = AttributeMap(Nil),   // 各列的数据统计
    hints: HintInfo = HintInfo())   // 仅仅使用在join时，表示用户是否指定使用 broadcast join
```

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



## 统计原理

LogicalPlan 基类定义了默认的统计方法 computeStats，不过它只支持非叶子节点。它仅仅是将子节点的统计结果相乘，而且只能计算数据的字节大小。

```scala
abstract class LogicalPlan extends QueryPlan[LogicalPlan] with Logging {

  protected def computeStats(conf: SQLConf): Statistics = {
    // 叶子节点必须自己实现统计方法
    if (children.isEmpty) {
      throw new UnsupportedOperationException(s"LeafNode $nodeName must implement statistics.")
    }
    // 这里将子节点的数据大小值相乘
    Statistics(sizeInBytes = children.map(_.stats(conf).sizeInBytes).product)
  }
}
```



LogicalPlan 的子类 UnaryNode 表示它只有一个子节点，复写了 computeStats方法。它会根据输入的列类型，计算每行输入的字节长度。同时根据输出的列类型，计算出每行输出的字节长度，这样就可以根据输入源的大小粗略的计算出输出的大小。

```scala
abstract class UnaryNode extends LogicalPlan {

  override def computeStats(conf: SQLConf): Statistics = {
    // 计算子节点的行数据大小
    val childRowSize = child.output.map(_.dataType.defaultSize).sum + 8
    // 计算当前节点的输出行数据大小
    val outputRowSize = output.map(_.dataType.defaultSize).sum + 8
    // 按照每行输出和输入的大小比例，计算统计数据
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

这种根据列类型来估算每行的数据长度，很明显不准确。比如列类型是 String，那么这个列的每行的数据长度很明显不是相同的，所以这种估算方式是很粗略的。下面列举常见类型的默认长度，

| 列类型    | 默认长度（字节）              |
| --------- | ----------------------------- |
| Byte      | 1                             |
| Integer   | 4                             |
| String    | 20                            |
| Date      | 4                             |
| Timestamp | 8                             |
| Array     | 列表元素的类型大小            |
| Map       | Key类型大小  +  Value类型大小 |



接下来来看看各个叶子节点的数据统计原理，基本上这些统计方式分为两种，一种是简单的统计，另一种是允许cbo优化，比较复杂但相比准确。



### Project 节点

```scala
case class Project(projectList: Seq[NamedExpression], child: LogicalPlan) extends UnaryNode {
  override def computeStats(conf: SQLConf): Statistics = {
    if (conf.cboEnabled) {
      // 如果允许cbo优化，那么就调用ProjectEstimation的统计方法
      ProjectEstimation.estimate(conf, this).getOrElse(super.computeStats(conf))
    } else {
      // 调用了UnaryNode父类的默认统计方法
      super.computeStats(conf)
    }
  }
}
```



ProjectEstimation 统计数据时，必须知道子节点的数据行数，才能进完成统计。它

```scala
object ProjectEstimation {
  import EstimationUtils._

  def estimate(conf: SQLConf, project: Project): Option[Statistics] = {
    // 判断是否知道子节点的行数
    if (rowCountsExist(conf, project.child)) {
      // 获取子节点的统计结果
      val childStats = project.child.stats(conf)
      val inputAttrStats = childStats.attributeStats
      // 遍历Alias 表达式，为列的别名也生成统计信息
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

object EstimationUtils {
  // 计算输出的数据大小
  // 参数 attributes：输出的列
  // 参数 outputRowCount：输出行数
  // 参数 attrStats：输出列的统计信息
  def getOutputSize(attributes: Seq[Attribute], outputRowCount: BigInt, 
                    attrStats: AttributeMap[ColumnStat] = AttributeMap(Nil)): BigInt = {
    val sizePerRow = 8 + attributes.map { attr =>
      if (attrStats.contains(attr)) {
        attr.dataType match {
          case StringType =>
            // UTF8String: base + offset + numBytes
            attrStats(attr).avgLen + 8 + 4
          case _ =>
            attrStats(attr).avgLen
        }
      } else {
        attr.dataType.defaultSize
      }
    }.sum

    if (outputRowCount > 0) outputRowCount * sizePerRow else 1
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





