# Spark Sql  LogicalPlan 原理



## 前言

LogicalPlan 表示逻辑计划，它表示 spark sql 初步解析的成果。之后的验证和优化，都是基于逻辑计划的。spark sql 使用 antrl 解析 sql 语句生成语法树，然后遍历这棵树生成 LogicalPlan 二叉树。这篇文章介绍常见的 LogicalPlan 子类和是如何生成 LogicalPlan 树的。



## LogicalPlan 类

LogicalPlan 继承QueryPlan，它的子类根据子节点的数量，分为三类：

- 没有子节点，对应 LeafNode 类
- 只有一个子节点，对应 UnaryNode 类
- 只有两个子节点，对应 BinaryNode 类



## LogicalPlan 子类



### Project 节点

```scala
case class Project(projectList: Seq[NamedExpression], child: LogicalPlan) extends UnaryNode
```

Project 节点表示 SELECT 语句中选中列的那部分。它包含了选中列的表达式，这些表达式 由 NamedExpression的子类表示。NamedExpression 的子类如下所示：



Star 类表示星号，意味着选中了所有列。它有两个子类UnresolvedStar 和 ResolvedStar，分别表示analyse 之前和之后。









### 表节点

```scala
case class UnresolvedRelation(tableIdentifier: TableIdentifier) extends LeafNode
```

表表名由 UnresolvedRelation 类表示，在上个 sql 例子中，在 analyse 之后会转换成一个子查询 SubqueryAlias，而这个子查询包含了 Hive 表节点 HiveTableRelation。



### Join 节点

```scala
case class Join(
    left: LogicalPlan,
    right: LogicalPlan,
    joinType: JoinType,
    condition: Option[Expression])
```

Join 表示sql 的 join 操作，包含两个需要 join 的子节点，join 类型 和 join 条件。join type 有下列几种：

- inner join
- left outer join
- right outer join
- full outer join
- left semi join
- left anti join

具体含义可以参见此篇文章 <http://sharkdtu.com/posts/spark-sql-join.html>



### Filter 节点

```scala
case class Filter(condition: Expression, child: LogicalPlan)
```

Filter 节点包含了表达式，对应了 sql 语句中的 WHERE 条件。

 

### Sort 节点

```scala
case class Sort(
    order: Seq[SortOrder],  // 排序的字段或者表达式
    global: Boolean,       // 否为全局的排序，还是分区的排序。
    child: LogicalPlan)
```

如果 sql 语句中使用了 ORDER BY 语句，那么就是全局排序。

如果使用了 SORT BY 语句，那么就是局部排序，也就是只保证同个分区是有序的，但是不能保证分区合并后的结果是有序的。

### Distinct 节点

```scala
case class Distinct(child: LogicalPlan)
```

如果选中的列有 DISTINCT 关键字，那么就会生成 Distinct 节点，表示需要对最后的结果去重。





## 遍历语法规则

接下来我们以 antrl4 文件为主，按照从上到下的顺序来查看语法树，是如何生成 LogicalPlan 树。读者可以自行编写 sql 语句生成语法树，然后结合下面的程序一起看。



### singleStatement 语法规则

首先是 singleStatement 规则，它在 AstBuilder 定义了访问自身的方法。

```scala
override def visitSingleStatement(ctx: SingleStatementContext): LogicalPlan = withOrigin(ctx) {
  visit(ctx.statement).asInstanceOf[LogicalPlan]
}
```

它只是继续遍历了statement 子规则，注意到 statement 规则有多种格式，支持 USE，CREATE 等语句。而我们使用的 sql 示例语句，匹配了 statement 规则的 statementDefault 格式。statementDefault 语法规则只有一个 query 子规则，它没有定义访问方法，所以它使用了 AstBuilder 的默认访问方法，即访问 query 子节点。



### query 语法规则

query 节点定义了访问自身的方法。注意到 query 语法规则，它有 ctes 和 queryNoWith 两个语法规则组成。 ctes  语法是来匹配 WITH 语句的。

```scala
override def visitQuery(ctx: QueryContext): LogicalPlan = withOrigin(ctx) {
  // 首先遍历 queryNoWith 子节点，plan方法就是调用了typedVisit方法
  val query = plan(ctx.queryNoWith)

  // 如果由 ctes 子节点，则遍历它并且生成 With 节点，将queryNoWIth的结果当作With的子节点
  query.optional(ctx.ctes) {
    // 解析WITH语句中的表达式
    val ctes = ctx.ctes.namedQuery.asScala.map { nCtx =>
      val namedQuery = visitNamedQuery(nCtx)
      (namedQuery.alias, namedQuery)
    }
    // Check for duplicate names.
    checkDuplicateKeys(ctes, ctx)
    // 生成With实例
    With(query, ctes)
  }
}
```

这里的 With 类是 LogicalPlan 的子类，它对应 WITH 语句。因为 WITH 语句用得不多，这里不再详细介绍。



### queryNoWith 语法规则

我们使用的 示例 sql 匹配了 queryNoWith 语法的 singleInsertQuery 格式，在 AstBuilder 类也定义了访问此格式的方法。

```scala
override def visitSingleInsertQuery(
    ctx: SingleInsertQueryContext): LogicalPlan = withOrigin(ctx) {
  // 解析子节点 queryTerm，生成LogicalPlan子类
  plan(ctx.queryTerm).
    // Add organization statements.
    optionalMap(ctx.queryOrganization)(withQueryResultClauses).
    // Add insert.
    optionalMap(ctx.insertInto())(withInsertInto)
}
```



singleInsertQuery 规则有三部分组成

- insertInto 规则，匹配 INSERT INTO 语句
- queryTerm 规则，匹配 SELECT 语句
- queryOrganization 规则，匹配 ORDER BY，DISTRIBUTE BY，CLUSTER BY，SORT BY 或 LIMIT 语句

当访问此节点时，依次按照 queryTerm ，queryOrganization，insertInto 顺序遍历。



### queryTerm 语法规则

queryTerm 规则有两种格式，一种是需要合并查询结果的，另一种是不需要合并。

需要合并查询结果的语句，匹配了 UNION， INTERSECT，EXCEPT 或 MINUS语句， 这些被合并的查询结果的列数目和类型都必须相同。

假设需要合并两个查询结果分别是 left 和 right

| sql 语句                       | 返回类型                    | 含义                                 |
| ------------------------------ | --------------------------- | ------------------------------------ |
| UNION ALL                      | Union(left, right)          | 两个查询结果相加                     |
| UNION DISTINCT 或 UNION 语句   | Distinct(Union(lef, right)) | 两个查询结果相加并且去重             |
| INTERSECT DISTINCT或 INTERSECT | Intersect(left, right)      | 两个查询结果的相交集合               |
| EXCEPT DISTINCT或 EXCEPT       | Except(left, right)         | 两个查询结果相加，但是去掉相交的部分 |
| MINUS DISTINCT或 MINUS         | Except(left, right)         | 两个查询结果相加，但是去掉相交的部分 |



不需要合并查询结果的语句，是一个单表查询语句。这种情况只有一个子规则 queryPrimary，而且也并没有定义这种情况的访问方法。所以它会使用AstBuilder的默认访问方式，直接访问子规则 queryPrimary。



### queryPrimary 语法规则

queryPrimary 规则也有多种格式，分别对应了以下情况

- queryPrimaryDefault 格式，普通的 SELECT 语句
- table 格式，表名
- inlineTableDefault1 格式，支持使用VALUES 语句创建表
- subquery 格式，子查询



解析 table 格式，会生成 UnresolvedRelation 类，它表示表名

```scala
override def visitTable(ctx: TableContext): LogicalPlan = withOrigin(ctx) {
  UnresolvedRelation(visitTableIdentifier(ctx.tableIdentifier))
}
```



解析 subquery 格式，也只是继续遍历子规则 queryNoWith。

```scala
override def visitSubquery(ctx: SubqueryContext): LogicalPlan = withOrigin(ctx) {
  plan(ctx.queryNoWith)
}
```



解析 inlineTableDefault1 格式，这个用法不是很常见。举个简单的例子，

```sql
SELECT * FROM (VALUES(1, 'apple'), (2, 'orange')) AS FRUIT(ID, NAME)
```

这条语句就使用VALUES语句，创建了一张表 FRUIT，有 ID 和 NAME 两列。

```scala
override def visitInlineTable(ctx: InlineTableContext): LogicalPlan = withOrigin(ctx) {
  // 遍历每个表达式
  val rows = ctx.expression.asScala.map { e =>
    expression(e) match {
      // inline table comes in two styles:
      // style 1: values (1), (2), (3)  -- multiple columns are supported
      // style 2: values 1, 2, 3  -- only a single column is supported here
      case struct: CreateNamedStruct => struct.valExprs // style 1
      case child => Seq(child)                          // style 2
    }
  }
  // 是否指明了列名，如果没有指定则自动生成列名（col1,col2...）
  val aliases = if (ctx.identifierList != null) {
    visitIdentifierList(ctx.identifierList)
  } else {
    Seq.tabulate(rows.head.size)(i => s"col${i + 1}")
  }
  // 生成 UnresolvedInlineTable 实例
  val table = UnresolvedInlineTable(aliases, rows)
  // 这里没弄明白，为什么有identifier属性，需要再去查看下antrl4文档，因为antrl4文件的内容被改了
  // 如果指定了表名，那么就生成 SubqueryAlias 实例
  table.optionalMap(ctx.identifier)(aliasPlan)
}

private def aliasPlan(alias: ParserRuleContext, plan: LogicalPlan): LogicalPlan = {
  SubqueryAlias(alias.getText, plan)
}
```

可以看到 inlineTable 规则，生成了 UnresolvedInlineTable 和 SubqueryAlias 实例。



解析 queryPrimaryDefault 规则，因为它没有重新定义访问方法，所以它使用了默认访问，继续遍历它的子节点querySpecification。



### querySpecification 语法规则

querySpecification 规则匹配了基础的 SELECT 语句，它几乎是 sql 的核心了。querySpecification 规则有两种格式，一种是普通的SELECT 操作，另一种是TRANSFORM 类型，它支持执行外部脚本来处理数据。

```scala
override def visitQuerySpecification(
    ctx: QuerySpecificationContext): LogicalPlan = withOrigin(ctx) {
  // 如果有FROM语句,那么先调用visitFromClause方法解析
  // 否则返回OneRowRelation实例，表示没有FROM语句的情况
  val from = OneRowRelation.optional(ctx.fromClause) {
    visitFromClause(ctx.fromClause)
  }
  // 继续遍历其他部分
  withQuerySpecification(ctx, from)
}

override def visitFromClause(ctx: FromClauseContext): LogicalPlan = withOrigin(ctx) {
  // 如果 FROM 后面接了多张表，那么默认认为这些表都是 INNER JOIN
  val from = ctx.relation.asScala.foldLeft(null: LogicalPlan) { (left, relation) =>
    // 遍历relation的子节点relationPrimary
    val right = plan(relation.relationPrimary)
    val join = right.optionalMap(left)(Join(_, _, Inner, None))
    withJoinRelations(join, relation)
  }
  // 查看是否有 lateralView 语句
  ctx.lateralView.asScala.foldLeft(from)(withGenerate)
}

// table 规则是 relationPrimary最常见的一种类型
override def visitTable(ctx: TableContext): LogicalPlan = withOrigin(ctx) {
  // 返回UnresolvedRelation实例
  UnresolvedRelation(visitTableIdentifier(ctx.tableIdentifier))
}
```



继续查看withQuerySpecification方法的定义

```scala
private def withQuerySpecification(
    ctx: QuerySpecificationContext,
    relation: LogicalPlan): LogicalPlan = withOrigin(ctx) {
  import ctx._  // import QuerySpecificationContext 类的属性

  // 根据WHERE语句生成Filter节点
  def filter(ctx: BooleanExpressionContext, plan: LogicalPlan): LogicalPlan = {
    Filter(expression(ctx), plan)
  }

  // 遍历SELECT选中的列
  val expressions = Option(namedExpressionSeq).toSeq
    .flatMap(_.namedExpression.asScala)
    .map(typedVisit[Expression])

  // Create either a transform or a regular query.
  val specType = Option(kind).map(_.getType).getOrElse(SqlBaseParser.SELECT)
  specType match {
    case SqlBaseParser.MAP | SqlBaseParser.REDUCE | SqlBaseParser.TRANSFORM =>
      // Transform 类型，会生成 ScriptTransformation 实例。
      .......
    case SqlBaseParser.SELECT =>
      // 普通 select 类型

      // lateral views 用来将此列数据分开，生成多行数据
      val withLateralView = ctx.lateralView.asScala.foldLeft(relation)(withGenerate)

      // 如果有 where 语句，那么生成Filter实例
      val withFilter = withLateralView.optionalMap(where)(filter)

      // 生成NamedExpression或UnresolvedAlias实例，表示选中的列
      val namedExpressions = expressions.map {
        case e: NamedExpression => e
        case e: Expression => UnresolvedAlias(e)
      }
      // 这里的aggregation是属于QuerySpecificationContext的，因为前面已经import，所以直接使用
      val withProject = if (aggregation != null) {
        // 如果有聚合语句，那么调用withAggregation方法，生成聚合节点
        // 如果聚合语句包含GROUPING SETS用法，那么返回GroupingSets实例
        // 苟泽返回Aggregate实例
        withAggregation(aggregation, namedExpressions, withFilter)
      } else if (namedExpressions.nonEmpty) {
        // 否则生成Project实例
        Project(namedExpressions, withFilter)
      } else {
        // 如果聚合语句和选中列都没有，那么直接返回 withFilter
        withFilter
      }

      // 如果有Having语句，那么生成Filter节点
      val withHaving = withProject.optional(having) {
        // 提取HAVING语句的表达式
        val predicate = expression(having) match {
          case p: Predicate => p
          case e => Cast(e, BooleanType)
        }
        Filter(predicate, withProject)
      }

      // 如果有DISTINCT关键字，那么生成Distinct节点
      val withDistinct = if (setQuantifier() != null && setQuantifier().DISTINCT() != null) {
        Distinct(withHaving)
      } else {
        withHaving
      }

      // 如果有WINDOW语句，那么调用withWindows方法生成
      val withWindow = withDistinct.optionalMap(windows)(withWindows)

      // Hint 语句
      hints.asScala.foldRight(withWindow)(withHints)
  }
}
```

从上面可以看到生成了多个LogicalPlan的种类：

- Filter，表示Where语句或者Having语句
- GroupingSets，表示GROUPING SETS 语句
- Aggregate，表示普通的GROUP BY 语句
- Project，表示SELECT选择的列
- Distinct，表示对列需要去重
- WithWindowDefinition，表示WINDOW语句



## 解析 Sql 实例

这里通过解析常见的 sql 语句，来看看是如何生成 LogicalPlan 树。

```sql
 CREATE TABLE fruit (
     id INT, 
     name STRING, 
     price FLOAT,
     amount INT
 );

 CREATE TABLE orders (
 	id INT,
    fruit_id INT,
    create_time TIMESTAMP,
    consumer_name STRING
 );
```



### 查询语句一

执行下列语句

```sql
SELECT * FROM fruit ORDER BY ID LIMIT 10; -- 按序列出前面10个水果
```

解析过程如下：

```shell
== Parsed Logical Plan ==
'GlobalLimit 10
+- 'LocalLimit 10
   +- 'Sort ['ID ASC NULLS FIRST], true
      +- 'Project [*]
         +- 'UnresolvedRelation `fruit`

== Analyzed Logical Plan ==
id: int, name: string, price: float, amount: int
GlobalLimit 10
+- LocalLimit 10
   +- Sort [ID#0 ASC NULLS FIRST], true
      +- Project [id#0, name#1, price#2, amount#3]
         +- SubqueryAlias `default`.`fruit`
            +- HiveTableRelation `default`.`fruit`, org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe, [id#0, name#1, price#2, amount#3]
```



首先看初步生成的结果：

Limit 语句被解析成了 GlobalLimit 和 LocalLimit 两个节点，LocalLimit 节点表示每个分区的数目限制，GlobalLimit 节点表示总的数目限制。

ORDER BY 语句被解析成了 Sort 节点，里面包含了排序的字段和排序的方向。

SELECT 语句中选择的列名，被解析成了 Project 实例。

表名被解析成了 UnresolvedRelation 节点。



再来看看解析过后的结果：

上一步的 UnresolvedRelation 节点，被解析成了 SubqueryAlias 节点，它表示一个子查询。这个子查询就是一张 Hive 表，由 HiveTableRelation 节点表示。

上一步的 Project 节点，它表示的列由 * 号表示，但是经过解析，它被变成了表里的各个列。



### 查询语句二

执行下列语句

```sql
SELECT fruit.name, orders.create_time, orderes.consumer_name  FROM orders INNER JOIN  fruit ON orders.fruit_id = fruit.id WHERE create_time > '2019-06-17';
```

解析过程如下：

```shell
== Parsed Logical Plan ==
'Project ['fruit.name, 'orders.create_time, 'orders.consumer_name]
+- 'Filter ('create_time > 2019-06-17)
   +- 'Join Inner, ('orders.fruit_id = 'fruit.id)
      :- 'UnresolvedRelation `orders`
      +- 'UnresolvedRelation `fruit`

== Analyzed Logical Plan ==
name: string, create_time: timestamp, consumer_name: string
Project [name#41, create_time#38, consumer_name#39]
+- Filter (cast(create_time#38 as string) > 2019-06-17)
   +- Join Inner, (fruit_id#37 = id#40)
      :- SubqueryAlias `default`.`orders`
      :  +- HiveTableRelation `default`.`orders`, org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe, [id#36, fruit_id#37, create_time#38, consumer_name#39]
      +- SubqueryAlias `default`.`fruit`...
```

.

这里主要看看初步结果：

WHERE 语句被解析成了 Filter 节点，它包含了过滤的表达式

INNER JOIN 语句 被解析成了 Join 节点，它包含了 join 的表达式，并且它还有两个 UnresolvedRelation 子节点，代表着 需要 join 的两张表。

