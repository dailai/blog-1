# spark sql 解析 #

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





