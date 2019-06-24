# Spark Sql 解析原理 #



## 前言

Spark Sql 支持用户编写 sql 语句来完成复杂的操作，并且还会对 sql 语句自动优化。Spark Sql 接收到用户的 sql 语句，会先按照 sql 的语法规则，来将 sql 语句拆分成语法词，然后用树的方式来组织这些词。然后用特定的访问规则，来将这课语法树，编译成Spark Sql 的数据结构，最后才会生成 Spark 代码运行。

这篇博客讲述Spark Sql 时如何将 sql 语句来编译成 Spark Sql 的内部结构。因为这部分的内容涉及到 antrl4 的使用，需要先了解 antrl 的基本用法，如果不清楚，建议参考此篇博客。



## 查看解析过程

### 查看  sql  语法树

Spark Sql 利用 anrl4 来完成语法分词和生成语法树的。sql 语法的 antrl4 定义文件的位置是 sql/catalyst/src/main/antlr4/org/apache/spark/sql/catalyst/parser/SqlBase.g4。我们可以首先在 idea 编辑器安装 antrl 插件，然后打开 spark 项目并找到 sql 的语法文件。

打开文件后，我们看到 sql 语法的入口是 singleStatement 规则。然后右键点击 singleStatement 规则，选择 Test Rule singleStatement 这个选项。然后在底部的左边编辑框里，输入要解析的 sql 语句。注意 sql 语句，除了字符串，都必须要大写。这里使用一个简单的 sql 语句为例，

```sql
SELECT NAME, PRICE FROM FRUIT WHERE NAME = 'apple'
```

生成的语法树如下：



### 查看 LogicalPlan 树

使用下面的 scala 代码

```scala
import org.apache.spark.sql.SparkSession

object SparkSqlTutorial {

  def main(args: Array[String]):Unit = {
    val spark = SparkSession.builder().master("local").appName("SparkSqlParser").getOrCreate()
    val sql = "SELECT name, price FROM fruit WHERE name = 'apple'"
    val logical = spark.sessionState.sqlParser.parsePlan(sql)
    println(logical)
  }
}
```

logical 变量就是整个 LogicalPlan 树的根节点，我们在 println 那一行加上断点调试，就可以很清楚的看到解析后的结果。



## 编译结果

Spark Sql 在遍历语法树后，生成的内部数据结构也是一棵树只不过这颗树的节点类型是 LogicalPlan 的子类。语法树中不同的节点对应于 LogicalPlan 的不同子类。这些 LogicalPlan 表示 sql 的一些重要操作，它可能会会包含多个 Expression。Expression表示一些数值对象或数值表达式， 也是根据根据语法树中的节点生成的。

比如 where 过滤语句 由 Filter 类表示， where 过滤语句的表达式 BooleanExpression。LogicalPlan 会包含多个 Expression  ，比如 Filter 会包含多个BooleanExpression 。





## AstBuilder 

antrl4 会自动生成语法树，剩下的工作就需要自行编写遍历的程序。Spark Sql 遍历语法树的逻辑定义在 AstBuilder 类。AstBuilder 使用了 visitor 模式来遍历语法树，它还复写了遍历的默认方法。首先来看看两个很重要的方法，typedVisit 提供了遍历节点，并且结果强制转换。visitChildren 复写了父类的方法，当遍历非叶子节点时，如果该节点只有一个子节点，那么继续遍历，否则就停止遍历。

```scala
class AstBuilder(conf: SQLConf) extends SqlBaseBaseVisitor[AnyRef] with Logging {
    
  protected def typedVisit[T](ctx: ParseTree): T = {
    ctx.accept(this).asInstanceOf[T]
  }  
    
  override def visitChildren(node: RuleNode): AnyRef = {
    // 如果该节点只有一个子节点，那么继续遍历子节点
    // 否则返回null
    if (node.getChildCount == 1) {
      node.getChild(0).accept(this)
    } else {
      null
    }
  }

  // 访问该语法树节点，并且将结果类型转换为LogicalPlan
  protected def plan(tree: ParserRuleContext): LogicalPlan = typedVisit(tree)
    
  // 访问该语法树节点，并且将结果类型转换为Expression
  protected def expression(ctx: ParserRuleContext): Expression = typedVisit(ctx)    
}
```



## 遍历语法规则

接下来我们以 antrl4 文件为主，按照从上到下的顺序来查看语法树，是如何生成 LogicalPlan 树。读者可以自行编写 sql 语句生成语法树，然后结合下面的程序一起看。这里使用上述示例的 sql 为例

```sql
SELECT NAME, PRICE FROM FRUIT WHERE NAME = 'apple'
```



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

* insertInto 规则，匹配 INSERT INTO 语句
* queryTerm 规则，匹配 SELECT 语句
* queryOrganization 规则，匹配 ORDER BY，DISTRIBUTE BY，CLUSTER BY，SORT BY 或 LIMIT 语句

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

* queryPrimaryDefault 格式，普通的 SELECT 语句
* table 格式，表名
* inlineTableDefault1 格式，支持使用VALUES 语句创建表
* subquery 格式，子查询



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

* Filter，表示Where语句或者Having语句
* GroupingSets，表示GROUPING SETS 语句
*  Aggregate，表示普通的GROUP BY 语句
* Project，表示SELECT选择的列
* Distinct，表示对列需要去重
* WithWindowDefinition，表示WINDOW语句





## 示例

### 示例一

执行 sql 语句：

```sql
SELECT NAME, PRICE-1 AS DISCOUNT, 'favorite' FROM fruit WHERE PRICE > 2 AND NAME = 'apple'
```

解析结果如下：

```shell
'Project ['NAME, ('PRICE - 1) AS DISCOUNT#7, unresolvedalias(favorite, None)]
+- 'Filter (('PRICE > 2) && ('NAME = apple))
   +- 'UnresolvedRelation `fruit`
```

### 示例二

执行 sql 语句：

```sql
SELECT FRUIT_ID, COUNT(*) FROM ORDERS WHERE CONSUMER = 'John' GROUP BY FRUIT_ID
```

解析结果如下

```shell
'Aggregate ['FRUIT_ID], ['FRUIT_ID, unresolvedalias('COUNT(1), None)]
+- 'Filter ('CONSUMER = John)
   +- 'UnresolvedRelation `ORDERS`
```

### 示例三

执行 sql 语句：

```sql
SELECT FRUIT.NAME, ORDERS.CONSUMER, ORDERS.CREATE_TIME FROM FRUIT INNER JOIN ORDERS ON ORDERS.FUIRT_ID = FRUIT.ID WHERE CONSUMER = 'John'
```

解析结果如下

```shell
'Project ['FRUIT.NAME, 'ORDERS.CONSUMER, 'ORDERS.CREATE_TIME]
+- 'Filter ('CONSUMER = John)
   +- 'Join Inner, ('ORDERS.FUIRT_ID = 'FRUIT.ID)
      :- 'UnresolvedRelation `FRUIT`
      +- 'UnresolvedRelation `ORDERS`
```



## Expression 解析

上面介绍了 LogicalPlan 的生成，这些 LogicalPlan 可能包含了多个表达式。这些表达式由 Expression 的子类表示，也是遍历语法树生成的。

我们以下面的 sql 语句为例，

```sql
SELECT NAME, PRICE-1 AS DISCOUNT, 'favorite' FROM fruit WHERE PRICE > 2 AND NAME = 'apple'
```

它的语法树如下



当解析到 namedExpression 语法规则时，会生成表达式，保存到 Project 实例里。

visitNamedExpression 方法定义了访问原理，会返回 Alias，MultiAlias 等子类。

```scala
override def visitNamedExpression(ctx: NamedExpressionContext): Expression = withOrigin(ctx) {
  // 解析子表达式
  val e = expression(ctx.expression)
  if (ctx.identifier != null) {
    // 如果指定了一个别名，那么返回Alias实例
    Alias(e, ctx.identifier.getText)()
  } else if (ctx.identifierList != null) {
    // 如果有多个别名（需要以括号将这些别名包起来），那么返回MultiAlias实例
    MultiAlias(e, visitIdentifierList(ctx.identifierList))
  } else {
    e
  }
}
```



当解析到 WHERE 后面的过滤表达式，会匹配为 booleanExpression 规则。而 booleanExpression 规则主要有两类格式，一种是包含逻辑运算符的，另一种是基础的表达式。

如果是第一种格式，比如包含 AND 或 OR 关键字。这类语句的解析稍微复杂，因为spark sql 会做一部分的优化。我们知道antrl4 是匹配语法规则时，它是用左递归的方式匹配。下面以 booleanExpression 规则为例，

```shell
booleanExpression
    : NOT booleanExpression                                        #logicalNot
    | EXISTS '(' query ')'                                         #exists
    | valueExpression predicate?                                   #predicated
    | left=booleanExpression operator=AND right=booleanExpression  #logicalBinary
    | left=booleanExpression operator=OR right=booleanExpression   #logicalBinary
    ;
```

假设有一个表达式语句

```sql
NAME = 'pear' AND NAME = 'apple' AND NAME = 'orange' AND NAME = 'banana' AND NAME = 'strawberry'
```

那么它会被解析



很明显这颗树左右不对称，而且左子树的深度很大。这样递归遍历树的时候，容易造成栈溢出。spark sql 针对这种情况，会尽可能的平衡这棵树，比如上面连续的 AND 表达式，会被优化成如下图所示。不过 spark sql 只能优化，从跟节点开始，连续为AND 或者连续为OR的这一段路径。具体程序就不介绍了，定义在 visitLogicalBinary 方法中。









如果是基础表达式，则对应于 predicated 格式。predicated格式的 predicate 规则，用来匹配 IN，BETWEEN AND 等语句。

```scala
override def visitPredicated(ctx: PredicatedContext): Expression = withOrigin(ctx) {
  // 遍历子规则 valueExpression
  val e = expression(ctx.valueExpression)
  if (ctx.predicate != null) {
    // 如果满足 predicate 格式的语句，则调用 withPredicate 方法生成Expression实例
    withPredicate(e, ctx.predicate)
  } else {
    e
  }
}
```



继续看子规则 valueExpression 的原理，valueExpression 有多种规则，能够匹配四则运算，大小等于的比较操作，还有异或预算。对于这些运算的规则，访问的原理很简单，只是生成了对应运算符的实例。比如等于操作符生成了 EqualTo 实例，加法运算符生成了 Add 实例。



继续遍历子节点 primaryExpression，它的规则比较多，这里仅仅介绍常见的几种。

columnReference 规则负责匹配列名，会返回 UnresolvedAttribute 类

functionCall 规则负责匹配函数，会返回 UnresolvedFunction 类

star 规则负责匹配 * 号，用来表示选择所有列，会返回 UnresolvedStar 类

constantDefault 规则负责匹配常量，返回 Literal 类









我们再来现在回顾下之前的 sql 语句，

```sql
SELECT NAME, PRICE FROM FRUIT WHERE NAME = 'apple'
```

按照上述的原理，它被解析成了

```shell
'Project ['NAME, 'PRICE]
+- 'Filter ('NAME = apple)
   +- 'UnresolvedRelation `FRUIT`
```

Project 是根节点，它有一个子节点Filter。Filter也有一个子节点 UnresolvedRelation。

