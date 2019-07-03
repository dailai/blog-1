DISTINCT 转换为 GROUP BY

```sql
SELECT DISTINCT f1, f2 FROM t  ==>  SELECT f1, f2 FROM t GROUP BY f1, f2
```



SubqueryAliases 消除

SubqueryAliases



Union 优化



合并连续的 Union 节点

如果出现连续的 Union 节点，而且这些节点对去重性的要求都相同，那么可以将这些 Union 节点合并成一个。





Project 下推到 Union

对于 SELECT col1, col2 FROM (col1, col2, col3 UNION col1, col2, col3)，这种情况，可以改为

select col1, col2 FROM ... UNION select col1, col2 FROM ...





联合子查询优化













Join 优化

重排内连接查询

因为有些内连接查询之间，有些会有关联条件限制，而有些没有。我们需要将这些有关联条件的内连接，放在前面。



消除外连接查询

比如 LEFT OUTER JOIN 这种情况，相比 INNER JOIN 这这种情况，只是增加了右边为空值的行数。如果这时候恰好有一个过滤条件，要求右边的值不为空，那么就可以将这种情况转为 INNER JOIN。



where 条件下推到 Join

比如有一个 Filter 节点，下面有一个Join 节点

或者有一个Join 节点，它有对应的条件限制语句





PushPredicateThroughJoin

如果是INNER JOIN，那么将where 条件的表达式或 on 条件的表达式，按照所属表来分离，然后将表达式下推到子节点，转换为新的 Filter 节点。



PushDownPredicate

Filter 子节点为 Project，这种情况是子查询，这条语句 `SELECT * FROM fruit WHERE fruit.id IN (SELECT fruit_id FROM orders)`，

```shell
'Project [*]
+- 'Filter 'fruit.id IN (list#2 [])
   :  +- 'Project ['fruit_id]
   :     +- 'UnresolvedRelation `orders`
   +- 'UnresolvedRelation `fruit`

```



将Filter 下推到 UNION 的子节点。





Limit 条件下推 到 Union 节点 或Join 节点





列裁剪



优化 IN 节点

如果 IN 后面的集合都是常数类型，并且大于配置项 spark.sql.optimizer.inSetConversionThreshold，那么就会将集合转换为 HashSet 存储，加快查询速度。



ConstantFolding

常量值合并







优化的内容比较多，需要对 sql 业务比较熟悉，所以不再深入研究。

