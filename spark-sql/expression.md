



NamedExpression

生成字段

解析字段

output



AggregationExpression ： COUNT， MAX， MIN

 



## 单值表达式



星号

UnresolvedStar



列名

UnresolvedAttribute



常数

Literal



函数

UnresolvedFunction



别名

Alias



多个别名

MultiAlias



## 逻辑表达式



## 断言表达式

BETWEEN  AND

IN

IS

IS NOT





## 多值表达式

  

### 逻辑运算

And

Or

Not



### 数值比较

EqualTo
LessThan
LessThanOrEqual
GreaterThan
GreaterThanOrEqual





## 数值运算

Add







## 聚合函数

MAX

MIN

COUNT

SUM







## Expression 基类

```scala
abstract class Expression extends TreeNode[Expression] {
  // 表达式的值类型
  def dataType: DataType

  // 引用的列
  def references: AttributeSet = AttributeSet(children.flatMap(_.references.iterator))

  // 检查输入类型，默认成功。子类如果对输入类型有要求，则需要重写
  def checkInputDataTypes(): TypeCheckResult = TypeCheckResult.TypeCheckSuccess

  // 根据输入计算表达式的值    
  def eval(input: InternalRow = null): Any

  // 是否结果确定性，也就是相同的输入必定有同样的值
  // 因为空列表的forall返回true，说明叶子节点默认是结果确定性
  def deterministic: Boolean = children.forall(_.deterministic)
    
}
```





## 逻辑表达式

当表达式返回的值为 Boolean 类型，那么就可以称为逻辑表达式，由 Predicate 接口表示。

```scala
trait Predicate extends Expression {
  override def dataType: DataType = BooleanType
}
```



为了更方便子类检查输入类型，spark sql 提供了 ExpectsInputTypes 接口。子类只需要复写 inputTypes 方法，返回各个输入值的类型

```scala
trait ExpectsInputTypes extends Expression {
  // 返回类型列表，第 i 个类型对应着第 i 个输入值
  def inputTypes: Seq[AbstractDataType]

  override def checkInputDataTypes(): TypeCheckResult = {
    // 检查类型是否不匹配
    val mismatches = children.zip(inputTypes).zipWithIndex.collect {
      case ((child, expected), idx) if !expected.acceptsType(child.dataType) =>
        s"..."
    }

    if (mismatches.isEmpty) {
      TypeCheckResult.TypeCheckSuccess
    } else {
      TypeCheckResult.TypeCheckFailure(mismatches.mkString(" "))
    }
  }
}
```



对于只有两个子表达式，由 BinaryExpression 类表示，它封装了 eval 方法，如果输入的值有一个为 null，那么这个表达式的值即为null。否则调用子类的 nullSafeEval 方法计算。

对于只有一个子表达式，由 UnaryExpression 类表示，它封装了 eval 方法，原理同 BinaryExpression 一样。



BinaryOperator 抽象类继承 BinaryExpression 类，并且两个子表达式的结果需要类型相同。它复写了checkInputDataTypes 方法，子类只需要复写 inputType 方法，返回一个类型即可。以 And 为例，

```scala
case class And(left: Expression, right: Expression) extends BinaryOperator with Predicate {
  // 子表达式的结果必须为 Boolean 类型
  override def inputType: AbstractDataType = BooleanType
 
  override def eval(input: InternalRow): Any = {
    // 计算第一个子表达式的值
    val input1 = left.eval(input)
    if (input1 == false) {
       false
    } else {
      // 计算第一个子表达式的值
      val input2 = right.eval(input)
      if (input2 == false) {
        false
      } else {
        if (input1 != null && input2 != null) {
          true
        } else {
          null
        }
      }
    }
  }
} 
```





下面我们以 Md5 函数为例，它的输出类型为String，输入类型为字节数组。并且它复写了 nullSafeEval 方法来实现MD5的计算。

```scala
case class Md5(child: Expression) extends UnaryExpression with ImplicitCastInputTypes {

  override def dataType: DataType = StringType

  override def inputTypes: Seq[DataType] = Seq(BinaryType)

  protected override def nullSafeEval(input: Any): Any =
    UTF8String.fromString(DigestUtils.md5Hex(input.asInstanceOf[Array[Byte]]))
}
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

