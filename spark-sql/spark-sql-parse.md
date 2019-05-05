# Spark SQL 编译



```sql
SELECT ID, COUNT(*) FROM MY_TABLE1 INNER JOIN MY_TABLE2 WHERE ID < 100 AND MY_FUNC(NAME) = 200 GROUP BY ID
```

遇到 FROM 语句的时候，会跟据表名生成 UnresolvedRelation 节点。如果还有关联表，那么会生成Join 节点。

对于选择的列 ID，和COUNT(*) 每个表达式，生成 Expression。

对于 NOT 表达式，生成 Not 实例

对于 Exists 表达式，生成 Exists 实例

....



WHERE 语句，会生成 Filter 实例，它包含了多个 Expression， 表示 where里面的表达式。



MY_TABLE   -------------->   UnresolvedRelation

MY_TABLE2 ------------->    UnresolvedRelation





MY_TABLE1 INNER JOIN MY_TABLE2  ---------------->  Join  （包含两个UnresolvedRelation）



ID, COUNT(*) 对应着不同的 Expression



WHERE ID < 100 AND MY_FUNC(NAME) = 200 --------------------->    Filter



GROUP BY ID -------------------------------->    GroupingSets， 包含了 字段

Join ------------------> Filter   ---------------------> GroupingSets



如果没有聚合（GROUP），那么会生成 Project，  包含了 字段

Join ------------------> Filter   ---------------------> Project



如果有HAVING 语句，那么这个HAVING的条件，会生成 Filter

Join ------------------> Filter   ---------------------> Project  --------------------->  Filter



如果有 DISTINCT 关键词，那么会生成 Distinct，

Join ------------------> Filter   ---------------------> Project  --------------------->  Filter  ------------------------>  Distinct



如果有 WINDOW  关键词， 那么会生成 WithWindowDefinition

Join ------------------> Filter   ---------------------> Project  --------------------->  Filter  ------------------------>  Distinct

  -------------------> WithWindowDefinition



如果有注释( 以这样的格式，/* comment */），那么生成 UnresolvedHint

Join ------------------> Filter   ---------------------> Project  --------------------->  Filter  ------------------------>  Distinct

  -------------------> WithWindowDefinition  ----------------------> UnresolvedHint。





目标：学会编译Spark SQL







Analyzer 类 继承 RuleExecutor， 有多个 Batch， 每个 Batch 代表着多条 Rule



Rule 的子类需要实现 apply 方法











# Spark SQL 解析原理

Spark SQL 提供了 sql 语句，来方便开发。开发人员通过编写简单的 sql 语句，就可以实现 spark 计算，而不需要笨重的编写 spark 程序。而且 Spark SQL 还会自动将用户编写的 sql 语句进行优化，一般情况下，优化的效果比自己写程序还要好。

Spark SQL 使用 antrl4 解析 sql 语句，关于 antrl4 的使用原理，可以参考此篇博客。



## antrl4 相关文件

spark sql 的语法定义文件的路径为 sql/catalyst/src/main/antlr4/org/apache/spark/sql/catalyst/parser/SqlBase.g4。这里从中抽取了主要内容，

```
grammar SqlBase;
singleStatement
    : statement EOF
    ;
    
statement
    : query                                                            #statementDefault
    | ctes? dmlStatementNoWith                                         #dmlStatement
    | USE db=identifier                                                #use
    | CREATE database (IF NOT EXISTS)? identifier
        (COMMENT comment=STRING)? locationSpec?
        (WITH DBPROPERTIES tablePropertyList)?                         #createDatabase
    ........
    
query
    : ctes? queryNoWith
    ;
    
queryNoWith
    : queryTerm queryOrganization                                               #noWithQuery
    | fromClause selectStatement+                                               #queryWithFrom
    ;
    
    
queryOrganization
    : (ORDER BY order+=sortItem (',' order+=sortItem)*)?
      (CLUSTER BY clusterBy+=expression (',' clusterBy+=expression)*)?
      (DISTRIBUTE BY distributeBy+=expression (',' distributeBy+=expression)*)?
      (SORT BY sort+=sortItem (',' sort+=sortItem)*)?
      windows?
      (LIMIT (ALL | limit=expression))?
    ;

queryTerm
    : queryPrimary                                                                       #queryTermDefault
    | left=queryTerm {legacy_setops_precedence_enbled}?
        operator=(INTERSECT | UNION | EXCEPT | SETMINUS) setQuantifier? right=queryTerm  #setOperation
    | left=queryTerm {!legacy_setops_precedence_enbled}?
        operator=INTERSECT setQuantifier? right=queryTerm                                #setOperation
    | left=queryTerm {!legacy_setops_precedence_enbled}?
        operator=(UNION | EXCEPT | SETMINUS) setQuantifier? right=queryTerm              #setOperation
    ;
    
queryPrimary
    : querySpecification                                                    #queryPrimaryDefault
    | TABLE tableIdentifier                                                 #table
    | inlineTable                                                           #inlineTableDefault1
    | '(' queryNoWith  ')'                                                  #subquery
    ;
    
querySpecification
    : (((SELECT kind=TRANSFORM '(' namedExpressionSeq ')'
        | kind=MAP namedExpressionSeq
        | kind=REDUCE namedExpressionSeq))
       inRowFormat=rowFormat?
       (RECORDWRITER recordWriter=STRING)?
       USING script=STRING
       (AS (identifierSeq | colTypeList | ('(' (identifierSeq | colTypeList) ')')))?
       outRowFormat=rowFormat?
       (RECORDREADER recordReader=STRING)?
       fromClause?
       (WHERE where=booleanExpression)?)
    | ((kind=SELECT (hints+=hint)* setQuantifier? namedExpressionSeq fromClause?
       | fromClause (kind=SELECT setQuantifier? namedExpressionSeq)?)
       lateralView*
       (WHERE where=booleanExpression)?
       aggregation?
       (HAVING having=booleanExpression)?
       windows?)
    ;
    
fromClause
    : FROM relation (',' relation)* lateralView* pivotClause?
    ;

aggregation
    : GROUP BY groupingExpressions+=expression (',' groupingExpressions+=expression)* (
      WITH kind=ROLLUP
    | WITH kind=CUBE
    | kind=GROUPING SETS '(' groupingSet (',' groupingSet)* ')')?
    | GROUP BY kind=GROUPING SETS '(' groupingSet (',' groupingSet)* ')'
    ;
    
relation
    : relationPrimary joinRelation*
    ;

joinRelation
    : (joinType) JOIN right=relationPrimary joinCriteria?
    | NATURAL joinType JOIN right=relationPrimary
    ;
    
relationPrimary
    : tableIdentifier sample? tableAlias      #tableName
    | '(' queryNoWith ')' sample? tableAlias  #aliasedQuery
    | '(' relation ')' sample? tableAlias     #aliasedRelation
    | inlineTable                             #inlineTableDefault2
    | functionTable                           #tableValuedFunction
    ;    
```



我们以下面三条常见的 sql 语句为例，弄清楚它们是如何解析的，之后遇到别的sql，解析原理都是差不多的。

假设我们有两张表 consumer 顾客表， 和 shop_order  订单表

```sql
SELECT ID, NAME, AGE FROM CONSUMER WHERE NAME = 'Jack' ORDER BY ID; /* 查询名称为Jack的用户信息 */

SELECT PROVINCE, COUNT(*) FROM CONSUMER WHERE AGE > 18 GROUP BY PROVINCE HAVING COUNT(*) > 1000; /* 统计各省的成人数目 */

SELECT PEOPLE.NAME, SHOP_ORDER.PRODUCT_NAME FROM PEOPLE INNER JOIN SHOP_ORDER ON SHOP_ORDER.CONSUMER_ID = CONSUMER.ID WHERE  SHOP_ORDER.PRODUCT_ID = 1111; /* 查询购买某种商品的用户信息 */
```





我们知道 antrl4 会生成语法数，使用者需要实现 Visitor 类的方法，来完成解析的操作。Spark SQL 的 AstBuilder 类负责。

首先来看看两个很重要的方法，typedVisit 提供了遍历节点，并且结果强制转换。visitChildren 复写了父类的方法，当遍历非叶子节点时，如果该节点只有一个子节点，那么继续遍历，否则就停止遍历。

```scala
class AstBuilder(conf: SQLConf) extends SqlBaseBaseVisitor[AnyRef] with Logging {
  
  protected def typedVisit[T](ctx: ParseTree): T = {
    ctx.accept(this).asInstanceOf[T]
  }
    
  
  override def visitChildren(node: RuleNode): AnyRef = {
    if (node.getChildCount == 1) {
      node.getChild(0).accept(this)
    } else {
      null
    }
  }
}
```



接下来分析第一条语句是如何解析的

```sql
SELECT ID, NAME, AGE FROM CONSUMER WHERE NAME = 'Jack' ORDER BY ID
```

它会被 antrl4 解析成一颗语法数，如下图所示：



结合 SqlBase.g4 文件，我们一层层的分析。首先它匹配第一层规则 singleStatement ，Spark SQL 在处理这条规则时，仅仅是处理它的子节点 statement 规则。

```scala
class AstBuilder(conf: SQLConf) extends SqlBaseBaseVisitor[AnyRef] with Logging {
  override def visitSingleStatement(ctx: SingleStatementContext): LogicalPlan = withOrigin(ctx) {
    visit(ctx.statement).asInstanceOf[LogicalPlan]
  }
}
```

statement 规则分为多种情况，这条 sql 语句 匹配了它的第一个标签 statementDefault。再向下，这条语句匹配了query 规则。Spark SQL 在处理这条规则时，会生成 LogicalPlan。

```scala
class AstBuilder(conf: SQLConf) extends SqlBaseBaseVisitor[AnyRef] with Logging {

  override def visitQuery(ctx: QueryContext): LogicalPlan = withOrigin(ctx) {
    // 遍历 queryNoWith 节点，生成 LogicalPlan
    val query = plan(ctx.queryNoWith)

    // 如果语句包含了 ctes 规则，遍历 ctes 节点
    query.optional(ctx.ctes) {
      val ctes = ctx.ctes.namedQuery.asScala.map { nCtx =>
        val namedQuery = visitNamedQuery(nCtx)
        (namedQuery.alias, namedQuery)
      }
      // Check for duplicate names.
      checkDuplicateKeys(ctes, ctx)
      With(query, ctes)
    }
  }
    
  protected def plan(tree: ParserRuleContext): LogicalPlan = typedVisit(tree)    
}
```



继续分析 queryNoWith 规则，这条 sql 语句被分为两部分，

```sql
SELECT ID, NAME, AGE FROM CONSUMER WHERE NAME = 'Jack' 
```

这部分匹配了 queryTerm 规则，

```sql
ORDER BY ID
```

这部分匹配了 queryOrganization 规则。







```scala
override def visitQuerySpecification(
    ctx: QuerySpecificationContext): LogicalPlan = withOrigin(ctx) {
  // 解析FROM语句，生成UnresolvedRelation
  val from = OneRowRelation.optional(ctx.fromClause) {
    visitFromClause(ctx.fromClause)
  }
  // 遍历剩下的部分，生成其他的
  withQuerySpecification(ctx, from)
}
```









表名对应 UnresolvedRelation

where 条件 对应 Filter

输出列名 对应 Project 



JOIN 语句 对应JOIN



ORDER BY 语句 对应 Sort

DISTINCT 关键词 对应 Distinct







