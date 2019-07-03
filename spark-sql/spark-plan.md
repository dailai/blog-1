



QueryPlanner 类负责将 LogicalPlan  转换成 PhysicalPlan。





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









## SparkPlan 的种类



### Join相关

BroadcastHashJoinExec，用于使用广播方法，实现 Join。

ShuffledHashJoinExec，采用基于Hash分区的shuffle方法，实现join

SortMergeJoinExec，用于排序合并

BroadcastNestedLoopJoinExec

CartesianProductExec



### 聚合相关

HashAggregateExec

ObjectHashAggregateExec

SortAggregateExec





### 其他

LocalTableScanExec，负责内存表

SortExec， 负责Sort节点

ProjectExec， 负责Project节点

FilterExec， 负责Filter节点







```
LocalLimitExec	LocalLimit
GlobalLimitExec	GlobalLimit
Union	UnionExec

```

然后介绍如何生成的





最后讲SparkPlan的优化操作