

## LogicalPlan 子类的输出列

每个LogicalPlan 必须定义输出列，这样后面的 LogicalPlan才能计算。这个方法定义在QueryPlan中

```scala
abstract class QueryPlan[PlanType <: QueryPlan[PlanType]] extends TreeNode[PlanType] {
  self: PlanType =>

  def output: Seq[Attribute]
}
```

### Filter 类

```scala
case class Filter(condition: Expression, child: LogicalPlan)
  extends UnaryNode with PredicateHelper {
  override def output: Seq[Attribute] = child.output
}
```



### Join 类

```scala
case class Join(
    left: LogicalPlan,
    right: LogicalPlan,
    joinType: JoinType,
    condition: Option[Expression])
  extends BinaryNode with PredicateHelper {

  override def output: Seq[Attribute] = {
    joinType match {
      case j: ExistenceJoin =>
        left.output :+ j.exists
      case LeftExistence(_) =>
        left.output
      case LeftOuter =>
        left.output ++ right.output.map(_.withNullability(true))
      case RightOuter =>
        left.output.map(_.withNullability(true)) ++ right.output
      case FullOuter =>
        left.output.map(_.withNullability(true)) ++ right.output.map(_.withNullability(true))
      case _ =>
        left.output ++ right.output
    }
  }
}
```



### Aggregate 类

```scala
case class Aggregate(
    groupingExpressions: Seq[Expression],
    aggregateExpressions: Seq[NamedExpression],
    child: LogicalPlan)
  extends UnaryNode {
  override def output: Seq[Attribute] = aggregateExpressions.map(_.toAttribute)
}  
```





### Project 类

```scala
case class Project(projectList: Seq[NamedExpression], child: LogicalPlan) extends UnaryNode {
  override def output: Seq[Attribute] = projectList.map(_.toAttribute)
}
```



其余的子类

它的output 是它的字节点的output

```scala
override def output: Seq[Attribute] = child.output
```









## 解析列名



首先调用的是LogicalPlan 的 transformUp 方法遍历，意味着从底层的叶子节点开始遍历，然后逐渐向父节点遍历。

所以需要先解析表名，然后根据表名获取表结构，包括各个列名，类型。

然后再来解析表节点的父节点，比如 Project 节点是 表节点的父节点，Project 节点包含了 



table.column.access





```scala
// Resolver 只是用来比较字符串是否相等的，有区分大小写和不区分大小写两种实现

private def resolveAsTableColumn(
    nameParts: Seq[String],
    resolver: Resolver,
    attribute: Attribute): Option[(Attribute, List[String])] = {
  assert(nameParts.length > 1)
  if (attribute.qualifier.exists(resolver(_, nameParts.head))) {
    // At least one qualifier matches. See if remaining parts match.
    val remainingParts = nameParts.tail
    resolveAsColumn(remainingParts, resolver, attribute)
  } else {
    None
  }
}

// 这里只比较列名，所以要求传入的参数 nameParts 必须是剔除掉 qualified
// attribute 参数是子节点的输出列，也就是该节点的输入列
private def resolveAsColumn(
    nameParts: Seq[String],
    resolver: Resolver,
    attribute: Attribute): Option[(Attribute, List[String])] = {
  if (!attribute.isGenerated && resolver(attribute.name, nameParts.head)) {
    // 判断列名是否与attribute相等
    Option((attribute.withName(nameParts.head), nameParts.tail.toList))
  } else {
    None
  }
}

def resolveChildren(
    nameParts: Seq[String],
    resolver: Resolver): Option[NamedExpression] =
  resolve(nameParts, children.flatMap(_.output), resolver)

```







```scala
  protected def resolve(
      nameParts: Seq[String],
      input: Seq[Attribute],
      resolver: Resolver): Option[NamedExpression] = {

    // A sequence of possible candidate matches.
    // Each candidate is a tuple. The first element is a resolved attribute, followed by a list
    // of parts that are to be resolved.
    // For example, consider an example where "a" is the table name, "b" is the column name,
    // and "c" is the struct field name, i.e. "a.b.c". In this case, Attribute will be "a.b",
    // and the second element will be List("c").
    var candidates: Seq[(Attribute, List[String])] = {
      // If the name has 2 or more parts, try to resolve it as `table.column` first.
      if (nameParts.length > 1) {
        input.flatMap { option =>
          resolveAsTableColumn(nameParts, resolver, option)
        }
      } else {
        Seq.empty
      }
    }

    // If none of attributes match `table.column` pattern, we try to resolve it as a column.
    if (candidates.isEmpty) {
      candidates = input.flatMap { candidate =>
        resolveAsColumn(nameParts, resolver, candidate)
      }
    }

    def name = UnresolvedAttribute(nameParts).name

    candidates.distinct match {
      // One match, no nested fields, use it.
      case Seq((a, Nil)) => Some(a)

      // One match, but we also need to extract the requested nested field.
      case Seq((a, nestedFields)) =>
        // The foldLeft adds ExtractValues for every remaining parts of the identifier,
        // and aliased it with the last part of the name.
        // For example, consider "a.b.c", where "a" is resolved to an existing attribute.
        // Then this will add ExtractValue("c", ExtractValue("b", a)), and alias the final
        // expression as "c".
        val fieldExprs = nestedFields.foldLeft(a: Expression)((expr, fieldName) =>
          ExtractValue(expr, Literal(fieldName), resolver))
        Some(Alias(fieldExprs, nestedFields.last)())

      // No matches.
      case Seq() =>
        logTrace(s"Could not find $name in ${input.mkString(", ")}")
        None

      // More than one match.
      case ambiguousReferences =>
        val referenceNames = ambiguousReferences.map(_._1).mkString(", ")
        throw new AnalysisException(
          s"Reference '$name' is ambiguous, could be: $referenceNames.")
    }
  }

  /**
   * Refreshes (or invalidates) any metadata/data cached in the plan recursively.
   */
  def refresh(): Unit = children.foreach(_.refresh())
}
```