# Spark Sql  LogicalPlan 原理



## 前言

Spark Sql 会使用 antrl 解析 sql 语句，生成一颗树，树的节点由 LogicalPlan 类表示。后面的验证和优化操作，都是基于这棵树进行的。

## UML 类

{% plantuml %}

@startuml
TreeNode <|-- QueryPlan
QueryPlan <|-- LogicalPlan
TreeNode <|-- Expression
@enduml

{% endplantuml %}

下面依次介绍这些类：

TreeNode： 表示树结构的节点，是一个泛型类

Expression：sql中的简单表达式

QueryPlan：拥有零个或多个 Expression

LogicalPlan ： 提供了解析操作   



## LogicalPlan 子类

这里通过解析常见的 sql 语句，来看看常见的 LogicalPlan 子类。假设有两张表，

```sql
 
 CREATE TABLE fruit (
     id INT, 
     name STRING, 
     price FLOAT, 
 );

 CREATE TABLE orders (
 	id INT,
    fruit_id INT,
    create_time TIMESTAMP,
    consumer_name STRING
 );
```



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

