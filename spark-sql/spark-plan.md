



QueryPlanner 类负责将 LogicalPlan  转换成 PhysicalPlan。













## SparkPlan 的种类



### Join相关

从上到下优先匹配

BroadcastHashJoinExec，用于使用广播方法，实现 Join。

ShuffledHashJoinExec，采用基于Hash分区的shuffle方法，实现join

SortMergeJoinExec，用于排序合并

BroadcastNestedLoopJoinExec，用于小表join大表，而且没有join条件限制

CartesianProductExec，没有join条件限制



Join 选择类型

是否可以广播表，如果该表通过hint 指明，或者该表的数据大小，小于spark.sql.autoBroadcastJoinThreshold 配置项（默认为10MB）

是否可以创建本地表，如果该表的数据大小，小于 spark.sql.autoBroadcastJoinThreshold * spark.sql.shuffle.partitions，默认为10MB * 200。

两张变的大小是否相差太大，如果一张表小于另一张表的 1 / 3，那么就认为相差过大

是否可以hash右表，

是否可以hash左表



```scala
object JoinSelection extends Strategy with PredicateHelper {
    
    private def canBroadcast(plan: LogicalPlan): Boolean = {
      plan.stats(conf).hints.isBroadcastable.getOrElse(false) ||
        (plan.stats(conf).sizeInBytes >= 0 &&
          plan.stats(conf).sizeInBytes <= conf.autoBroadcastJoinThreshold)
    }
    
    private def canBuildLocalHashMap(plan: LogicalPlan): Boolean = {
      plan.stats(conf).sizeInBytes < conf.autoBroadcastJoinThreshold * conf.numShufflePartitions
    }
    
    private def muchSmaller(a: LogicalPlan, b: LogicalPlan): Boolean = {
      a.stats(conf).sizeInBytes * 3 <= b.stats(conf).sizeInBytes
    }
    
    // 左连接或者内连接的表，都可以支持右表
    private def canBuildRight(joinType: JoinType): Boolean = joinType match {
      case _: InnerLike | LeftOuter | LeftSemi | LeftAnti => true
      case j: ExistenceJoin => true
      case _ => false
    }
    
    // 右连接的表，都可以支持左表
    private def canBuildLeft(joinType: JoinType): Boolean = joinType match {
      case _: InnerLike | RightOuter => true
      case _ => false
    }
    
```



如果连接类型支持左表hash，并且左边支持广播，那么就使用广播左表的方式

如果连接类型支持右表hash，并且右边支持广播，那么就使用广播右表的方式

使用 Shuffled Hash 方式，满足下面任一个条件

1. 配置spark.sql.join.preferSortMergeJoin 没有被设置为 true，并且连接类型支持右表hash，并且右表的大小可以支持创建本地表，并且右表比左表要小得多
2. 配置spark.sql.join.preferSortMergeJoin 没有被设置为 true，并且连接类型支持左表hash，并且左表的大小可以支持创建本地表，并且左表比右表要小得多
3.  join的字段类型不支持排序



SortMergeJoinExec，只要join的字段类型支持排序即可



BroadcastNestedLoopJoinExec，用于小表join大表，而且没有join条件限制

CartesianProductExec，没有join条件限制









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