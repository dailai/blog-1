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
      // 根据输出列，构建出每列的统计信息
      val outputAttrStats =
        getOutputMap(AttributeMap(inputAttrStats.toSeq ++ aliasStats), project.output)
      Some(childStats.copy(
        // 调用getOutputSize方法，计算数据字节大小
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
    // 遍历每个列的数据大小，计算它们的平均大小
    val sizePerRow = 8 + attributes.map { attr =>
      
      if (attrStats.contains(attr)) {
        attr.dataType match {
          case StringType =>
            // UTF8String: base + offset + numBytes
            attrStats(attr).avgLen + 8 + 4
          case _ =>
            // 该列的平均大小
            attrStats(attr).avgLen
        }
      } else {
        // 如果没有此列的统计信息，则计算该列类型的默认大小
        attr.dataType.defaultSize
      }
    }.sum
    // 每行数据的平均大小，乘以数据行数
    if (outputRowCount > 0) outputRowCount * sizePerRow else 1
  }
}
```



## Filter 节点

Filter 节点主要是起过滤作用，所以它只会更新行数相关的统计。它的计算也分为两种

* 允许 cbo 优化，调用 FilterEstimation 的统计方法
* 不允许 cbo 优化，调用 UnaryNode 的统计方法

FilterEstimation 的统计方法有点复杂，因为 Filter 里的表达式会很复杂，有And，Or，Not等语句。

首先我们来从简单的表达式开始分析，目前 spark sql 支持可以统计的格式很简单。比较操作符只能是大于号，小于号，等于号的一种或两种结合，比较的两个变量必须是常量或列名。

下面了为了阐述方便，列名使用 attribute 表示，常量使用 literal 表示。并且该列的统计结果，min 代表最小值，max代表最大值，distinctCount代表不重复的行数。

假设该列的数据类型为数值型，Date，Timestamp 或 Boolean，并且比较符号是  =, <, <=, >, >=，



### attribute = literal

outputRowCount = 1.0 / distinctCount  * inputRowCount



### attribute < literal

1. literal <= min，那么 outputRowCount = 0
2. literal > max，那么 outputRowCount = inputRowCount
3. literal == max，那么 outputRowCount = (1.0 - 1.0 / distinctCount)  * inputRowCount
4. 其他情况，outputRowCount = (literal - min) / (max - min) * inputRowCount



### attribute <= literal

1. literal < min，那么 outputRowCount = 0
2. literal >= max，那么 outputRowCount = inputRowCount
3. literal == min，那么 outputRowCount = 1.0 / distinctCount  * inputRowCount
4. 其他情况，outputRowCount = (literal - min) / (max - min) * inputRowCount



### attribute > literal

1. literal >= max，那么 outputRowCount = 0
2. literal < min，那么 outputRowCount = inputRowCount
3. literal == min，那么 outputRowCount = (1.0 - 1.0 / distinctCount)  * inputRowCount
4. 其他情况，outputRowCount = (literal - min) / (max - min) * inputRowCount



### attribute >= literal

1. literal > max，那么 outputRowCount = 0
2. literal <= min，那么 outputRowCount = inputRowCount
3. literal == max，那么 outputRowCount = 1.0 / distinctCount  * inputRowCount
4. 其他情况，outputRowCount = (literal - min) / (max - min) * inputRowCount



### attribute 之间的比较

可以根据两个列的最大值和最小值，想象成两个区间取交集。然后结合比较符号，返回的结果可以分为三种，

1. outputRowCount = 0，两个集合没有交集，条件不可能成立
2. outputRowCount = inputRowCount，两个集合没有交集，条件肯定成立
3. outputRowCount = 1.0 / 3.0 * inputRowCount，两个集合有交集，这里取一个经验值 1.0 / 3.0



### attribute In [literal，.....]

首先根据该列的最大值和最小值，过滤掉集合中，不在此范围的值。然后计算集合中符合条件的值数目 num，返回行数为 outputRowCount = (num * 1.0 / distinctCount)* inputRowCount



### 复杂语句

上面介绍完基础的表达式，然后来看看如果包含And，Or 或 Not 的语句，怎么计算统计结果的。

如果是 And 语句，那么将两个子条件的比例，这个比例就是输出数据的行数 / 输入数据的行数。然后将两个比例相乘，就得到了 And 语句的比例。

如果是 Or 语句，那么将两个子条件的比例，percent1 和 percent2。Or 语句的比例为 percent1 + percent2 - (percent1 * percent2)

如果是Not 语句，那么计算出子条件的比例 percent，然后 Not 语句的比例为 1.0 - percent



## Union 节点

Union 节点的统计计算很简答，只是将各个子节点的统计数据取和



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
        // 所以直接返回left的统计结果
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



JoinEstimation 的统计方法比较复杂，它根据 join 的类型分成两种统计方法：

1. Inner ， Cross ， LeftOuter ， RightOuter ， FullOuter
2. LeftSemi， LeftAnti

下面依次介绍两种统计原理，我们定义需要 join 的两张表 left 和 right。

joinSelectivity 方法计算出比例，即 输出数据的行数 / 输入数据的行数

```scala
// 参数 joinKeyPairs，表示 join 字段
// 返回比例，
def joinSelectivity(joinKeyPairs: Seq[(AttributeReference, AttributeReference)]): BigDecimal = {
  var ndvDenom: BigInt = -1
  var i = 0
  // 遍历 join 字段
  while(i < joinKeyPairs.length && ndvDenom != 0) {
    val (leftKey, rightKey) = joinKeyPairs(i)
    // 获取字段对应的统计信息
    val leftKeyStats = leftStats.attributeStats(leftKey)
    val rightKeyStats = rightStats.attributeStats(rightKey)
    // 根据字段的最大值和最小值，生成区间
    val lRange = Range(leftKeyStats.min, leftKeyStats.max, leftKey.dataType)
    val rRange = Range(rightKeyStats.min, rightKeyStats.max, rightKey.dataType)
    // 如果两个区间没有交集，那么说明join后的结果是空
    if (Range.isIntersected(lRange, rRange)) {
      // 如果有交集，那么distinctCount的最大值
      val maxNdv = leftKeyStats.distinctCount.max(rightKeyStats.distinctCount)
      if (maxNdv > ndvDenom) ndvDenom = maxNdv
    } else {
      // 如果没有交集，则设置 ndvDenom 为0
      ndvDenom = 0
    }
    i += 1
  }

  if (ndvDenom < 0) {
    // We can't find any join key pairs with column stats, estimate it as cartesian join.
    1
  } else if (ndvDenom == 0) {
    // One of the join key pairs is disjoint, thus the two sides of join is disjoint.
    0
  } else {
    1 / BigDecimal(ndvDenom)
  }
}
```



计算出比例后，还会根据 join 类型计算出。

```scala
val joinKeyPairs = extractJoinKeysWithColStats(leftKeys, rightKeys)  // 提取join字段
val selectivity = joinSelectivity(joinKeyPairs)  // 计算比例
val leftRows = leftStats.rowCount.get  
val rightRows = rightStats.rowCount.get
val innerJoinedRows = ceil(BigDecimal(leftRows * rightRows) * selectivity)  // 计算join行数
val outputRows = joinType match {
  case LeftOuter =>
    // All rows from left side should be in the result.
    leftRows.max(innerJoinedRows)
  case RightOuter =>
    // All rows from right side should be in the result.
    rightRows.max(innerJoinedRows)
  case FullOuter =>
    // T(A FOJ B) = T(A LOJ B) + T(A ROJ B) - T(A IJ B)
    leftRows.max(innerJoinedRows) + rightRows.max(innerJoinedRows) - innerJoinedRows
  case _ =>
    // Don't change for inner or cross join
    innerJoinedRows
}
```



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





