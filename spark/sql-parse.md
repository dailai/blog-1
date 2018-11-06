# spark sql 解析 #

## antrl4 文件 ##

## 解析步骤 ##

接口 ParserInterface

抽象类 AbstractSqlParser

子类 SparkSqlParser

```java
sqlContext.sql("SELECT * FROM mytable WHERE id = 13")
```

调用 SparkSqlParser的parsePlan方法，返回基类LogicalPlan

LeafNode 是表示叶子节点的LogicalPlan

Command 继承LeafNode

RunnableCommand 继承 Command

比如一些常见的命令都继承自RunnableCommand ： AddJarCommand， AddFileCommand， CreateTableCommand等等