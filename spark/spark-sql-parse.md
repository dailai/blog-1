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



示例：

两张表 consumer 顾客表， 和 shop_order  订单表



```sql
SELECT ID, NAME, AGE FROM CONSUMER WHERE NAME = 'Jack' ORDER BY ID; /* 查询名称为Jack的用户信息 */

SELECT PROVINCE, COUNT(*) FROM CONSUMER WHERE AGE > 18 GROUP BY PROVINCE HAVING COUNT(*) > 1000; /* 统计各省的成人数目 */

SELECT PEOPLE.NAME, SHOP_ORDER.PRODUCT_NAME FROM PEOPLE INNER JOIN SHOP_ORDER ON SHOP_ORDER.CONSUMER_ID = CONSUMER.ID WHERE  SHOP_ORDER.PRODUCT_ID = 1111; /* 查询购买某种商品的用户信息 */
```



