# spark sql 解析 #



## 解析操作

Spark Sql 使用 antrl 工具来解析 sql，



### 查看 sql 语法树

sql 的语法定义文件的位置是 sql/catalyst/src/main/antlr4/org/apache/spark/sql/catalyst/parser/SqlBase.g4。我们可以首先在 idea 编辑器安装 antrl 插件，然后打开 spark 项目并找到 sql 的语法文件。

打开文件后，我们看到 sql 语法的入口是 singleStatement 规则。然后右键点击 singleStatement 规则，选择 Test Rule singleStatement 这个选项。然后在底部的左边编辑框里，输入要解析的 sql 语句。注意到 sql 语句中，除了字符串，都必须要大小。运行结果如下：





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





## 生成 LogicalPlan 原理

因为 spark sql 是使用 antrl 工具来解析 sql 的，所以读者需要先了解 antrl 的基本用法，可以参考此篇博客。

经过 antrl 生成了语法树之后，我们来看看 spark sql 是如何来遍历这颗树的，遍历语法树的逻辑定义在 AstBuilder 类。接下来我们通过几个简单的例子，来阐述解析的原理，也附带讲讲常见的 LogicalPlan 和 Expression 种类。

## AstBuilder 

AstBuilder 使用了 visitor 模式来遍历语法树，它还复写了遍历的默认方法。

```scala
class AstBuilder(conf: SQLConf) extends SqlBaseBaseVisitor[AnyRef] with Logging {
  override def visitChildren(node: RuleNode): AnyRef = {
    // 如果该节点只有一个子节点，那么继续遍历子节点
    // 否则返回null
    if (node.getChildCount == 1) {
      node.getChild(0).accept(this)
    } else {
      null
    }
  }
}
```



## 示例一

```sql
SELECT NAME, PRICE FROM FRUIT WHERE NAME = 'apple'
```

生成的语法树



我们从上面依次向下，来看看每个节点是如何被解析的。

### singleStatement 节点

首先是 singleStatement 节点，它在 AstBuilder 定义了访问自身的方法。

```scala
override def visitSingleStatement(ctx: SingleStatementContext): LogicalPlan = withOrigin(ctx) {
  visit(ctx.statement).asInstanceOf[LogicalPlan]
}
```

可以看到它只是继续遍历了statement 子节点，注意到 statement 语法有四种规则。而这条语句匹配了 statement 语法的 statementDefault 格式，所以这里的子节点实际是 statementDefault 节点。statementDefault节点有一个 query 子节点。



### query 节点

query 节点也定义了访问自身的方法。注意到 query 语法的定义，它有 ctes 语法和 queryNoWith 语法两部分组成。 ctes  语法是来匹配 WITH 语句的。

```scala
override def visitQuery(ctx: QueryContext): LogicalPlan = withOrigin(ctx) {
  // 首先遍历 queryNoWith 子节点 
  val query = plan(ctx.queryNoWith)

  // 如果由 ctes 子节点，则遍历它并且生成 With 节点，将queryNoWIth的结果当作With的子节点
  query.optional(ctx.ctes) {
    // 
    val ctes = ctx.ctes.namedQuery.asScala.map { nCtx =>
      val namedQuery = visitNamedQuery(nCtx)
      (namedQuery.alias, namedQuery)
    }
    // Check for duplicate names.
    checkDuplicateKeys(ctes, ctx)
    With(query, ctes)
  }
}
```

这儿可以看到有个LogicalPlan 的子类 With，它的子节点是 queryNoWith 的结果，并且还包含了 WITH 语句的表达式。因为 WITH 语句用得不多，这里不再详细介绍。



 ### queryNoWith 节点

我们使用的 sql 匹配了 queryNoWith 语法的 singleInsertQuery 规则，在 AstBuilder 类也定义了访问此节点的方法。

```scala
override def visitSingleInsertQuery(
    ctx: SingleInsertQueryContext): LogicalPlan = withOrigin(ctx) {
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

当访问此节点时，按照 queryTerm ，queryOrganization，insertInto 顺序遍历。































## parser相关类 ##

接口 ParserInterface

```scala
trait ParserInterface {
  def parsePlan(sqlText: String): LogicalPlan
  def parseExpression(sqlText: String): Expression
  def parseTableIdentifier(sqlText: String): TableIdentifier
  def parseFunctionIdentifier(sqlText: String): FunctionIdentifier
  def parseTableSchema(sqlText: String): StructType
  def parseDataType(sqlText: String): DataType
}
```

抽象类 AbstractSqlParser

```scala
abstract class AbstractSqlParser extends ParserInterface with Logging {

  override def parseDataType(sqlText: String): DataType = parse(sqlText) { parser =>
    astBuilder.visitSingleDataType(parser.singleDataType())
  }

  override def parseExpression(sqlText: String): Expression = parse(sqlText) { parser =>
    astBuilder.visitSingleExpression(parser.singleExpression())
  }

  override def parseTableIdentifier(sqlText: String): TableIdentifier = parse(sqlText) { parser =>
    astBuilder.visitSingleTableIdentifier(parser.singleTableIdentifier())
  }

  override def parseFunctionIdentifier(sqlText: String): FunctionIdentifier = {
    parse(sqlText) { parser =>
      astBuilder.visitSingleFunctionIdentifier(parser.singleFunctionIdentifier())
    }
  }

  override def parseTableSchema(sqlText: String): StructType = parse(sqlText) { parser =>
    StructType(astBuilder.visitColTypeList(parser.colTypeList()))
  }

  override def parsePlan(sqlText: String): LogicalPlan = parse(sqlText) { parser =>
    astBuilder.visitSingleStatement(parser.singleStatement()) match {
      case plan: LogicalPlan => plan
      case _ =>
        val position = Origin(None, None)
        throw new ParseException(Option(sqlText), "Unsupported SQL statement", position, position)
    }
  }

  protected def astBuilder: AstBuilder

}

```

AbstractSqlParser 实现了ParserInterface接口，可以看出它的实现都是转发给AstBuilder负责。值得注意的是parse方法，它接收了两个参数，第一个是sql语句，第二个是解析函数。parse方法实例化了 SqlBaseParser，并且为它添加了两个ParserListener, 在antrl4解析语法的时候，会调用。一个是ParseErrorListener， 用于当解析到不合法的sql语句，给出错误的信息。另一个是PostProcessor， 当sql语句中出现变量，并且变量是由两个&apos;包含，替换成一个&apos;

```scala
protected def parse[T](command: String)(toResult: SqlBaseParser => T): T = {
    logInfo(s"Parsing command: $command")

    val lexer = new SqlBaseLexer(new ANTLRNoCaseStringStream(command))
    lexer.removeErrorListeners()
    lexer.addErrorListener(ParseErrorListener)

    val tokenStream = new CommonTokenStream(lexer)
    val parser = new SqlBaseParser(tokenStream)
    parser.addParseListener(PostProcessor)
    parser.removeErrorListeners()
    parser.addErrorListener(ParseErrorListener)

    try {
      try {
        // first, try parsing with potentially faster SLL mode
        parser.getInterpreter.setPredictionMode(PredictionMode.SLL)
        toResult(parser)
      }
    ........
  }
```

子类 SparkSqlParser

SparkSqlParser继承AbstractSqlParser，它有两个作用。一个是实现了astBuilder的方法，返回SparkSqlAstBuilder实例。第二个是提供了sql语句中变量shell的替换，类似于${variable}的格式。

## sql 解析结果 ##

sql解析的入口是sqlContext.sql

```scala
class SQLContext {
	def sql(sqlText: String): DataFrame = sparkSession.sql(sqlText)
}

class SparkSession {
	def sql(sqlText: String): DataFrame = {
    	Dataset.ofRows(self, sessionState.sqlParser.parsePlan(sqlText))
  	}
}
```

最终是调用 SparkSqlParser的parsePlan方法。一般parsePlan是返回RunnableCommand， 表示要执行的命令，里面包含了解析的结果。 

从SparkSqlParser的parsePlan方法，可以看到返回的结果是LogicalPlan类。

LeafNode 是表示叶子节点的LogicalPlan，Command 继承LeafNode， RunnableCommand 继承 Command。

比如一些常见的命令都继承自RunnableCommand ： AddJarCommand， AddFileCommand， CreateTableCommand等等

## 解析步骤 ##

SparkSqlParser 的 parsePlan方法，调用了SparkSqlAstBuilder的visitSingleStatement方法，生成LogicalPlan。

SparkSqlAstBuidler是AstBuilder的子类。AstBuilder是antrl4生成的SqlBaseBaseVisitor的子类。

首先看看antrl4的定义：

```g4
singleStatement
    : statement EOF
    ;
statement
    : query                                                            #statementDefault
    | USE db=identifier                                                #use
    | CREATE DATABASE (IF NOT EXISTS)? identifier
        (COMMENT comment=STRING)? locationSpec?
        (WITH DBPROPERTIES tablePropertyList)?                         #createDatabase
........
```

以 create database语句为例 ,比如

```sql
CREATE DATABASE IF NOT EXISTS mydatabase COMMENT 'test database';
```

antrl4的匹配顺序是从顶层往下。首先它会匹配singleStatement，代码如下

```scala
override def visitSingleStatement(ctx: SingleStatementContext): LogicalPlan = withOrigin(ctx) {
    visit(ctx.statement).asInstanceOf[LogicalPlan]
  }
```

这里直接是遍历statement的子节点。然后进入statement的匹配规则。statement会识别这条语句属于createDatabase规则。

```scala
class SparkSqlAstBuilder(conf: SQLConf) extends AstBuilder(conf) {

   override def visitCreateDatabase(ctx: CreateDatabaseContext): LogicalPlan = withOrigin(ctx) {
    CreateDatabaseCommand(
      ctx.identifier.getText,
      ctx.EXISTS != null,
      Option(ctx.locationSpec).map(visitLocationSpec),
      Option(ctx.comment).map(string),
      Option(ctx.tablePropertyList).map(visitPropertyKeyValues).getOrElse(Map.empty))
  }
}
```

vistiCreateDatabase直接返回了CreateDatabaseCommand实例。

整个sql解析的原理，是使用了antrl4这个库，定义了sql的匹配标准。然后使用Visitor模式。自定义遍历sql语句的节点，生成相应的RunnableCommand。



visitQuerySpecification 方法很重要，它用来解析最常见的 SELECT 语句。











## Expression 解析

```sql
SELECT name, price-1, 'favorite' FROM test.fuit WHERE price > 2 AND name = 'apple';
```



