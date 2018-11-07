## Visitor 原理 ##


### 节点种类 ###
antlr4 会将语句解析成一棵树，树的每个节点由ParseTree基类表示。节点有不同的类型，由ParseTree的不同子类表示。

TerminalNode 表示叶子节点， 表示字符常量，或者在antrl4文件中的lexer 


RuleNode 表示非叶子节点，表示一个句子的语法，对应 antrl4文件中的 parser rule


### visitor ###

基本的遍历逻辑，在AbstractParseTreeVisitor类中，它是所有遍历相关的基类。

它是依照深度优先遍历，可以查看VisitChildren方法
```java
public abstract class AbstractParseTreeVisitor<T> implements ParseTreeVisitor<T> {
    public T visitChildren(RuleNode node) {
            // 生成默认值
        T result = defaultResult();
        int n = node.getChildCount();
        for (int i=0; i<n; i++) {
            // 检测是否继续遍历子节点
            if (!shouldVisitNextChild(node, result)) {
                break;
            }
            // 获取子节点
            ParseTree c = node.getChild(i);
            // 遍历子节点，返回子节点的结果
            T childResult = c.accept(this);
            // 合并子节点的结果
            result = aggregateResult(result, childResult);
        }

        return result;
    }

    // 默认值为null
    protected T defaultResult() {
        return null;
    }


    // 合并的远离，返回子节点的结果
    protected T aggregateResult(T aggregate, T nextResult) {
        return nextResult;
    }


    // 默认允许继续访问
    protected boolean shouldVisitNextChild(RuleNode node, T currentResult) {
        return true;
    }
}
```


visitChildren方法本质是使用了递归。它的结束条件是访问TerminalNode。

```java
public abstract class AbstractParseTreeVisitor<T> implements ParseTreeVisitor<T> {
    // 返回默认值
    public T visitTerminal(TerminalNode node) {
        return defaultResult();
    }
}
```


使用Visitor遍历ParserTree，是从ParseTree的accpet方法开始，accept方法会根据节点的类型，调用不同的visit方法。

##### TerminalNode 遍历 ####
```java
// 遍历TerminalNode，是调用了Visitor的visitTerminal方法
public class TerminalNodeImpl implements TerminalNode {
    @Override
	public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
		return visitor.visitTerminal(this);
	}
}
```

#### RuleNode遍历 ####
```java
// 遍历RuleNode，是调用了Visitor的visitChildren方法
public class RuleContext implements RuleNode {
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
        return visitor.visitChildren(this);
    }

}
```


### 计算器例子 ###

定义的Calculator.g4文件
```
grammar Calculator;

prog : stat+;

stat:
  expr NEWLINE          # print
  | ID '=' expr NEWLINE   # assign
  | NEWLINE               # blank
  ;

expr:
  expr op=('*'|'/') expr    # MulDiv
  | expr op=('+'|'-') expr        # AddSub
  | INT                           # int
  | ID                            # id
  | '(' expr ')'                  # parenthese
  ;

MUL : '*' ;
DIV : '/' ;
ADD : '+' ;
SUB : '-' ;
ID : [a-zA-Z]+ ;
INT : [0-9]+ ;
NEWLINE :'\r'? '\n' ;
DELIMITER : ';';
WS : [ \t]+ -> skip;
```
prog是整个语法的入口，它表示可以有多行语句。

stat就是一行语句的格式。

通过anltr4生成java的代码，关于Visitor相关的有两个文件 CalculatorBaseVisitor 和 CalculatorParser。

CalculatorParser包含了各个节点，都为每个节点封装了accept方法，接收visitor的遍历。以prog为例，
```java
public static class ProgContext extends ParserRuleContext {

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
        if ( visitor instanceof CalculatorVisitor )
            // 调用了visitor的visitProg方法
            return ((CalculatorVisitor<?extends T>)visitor).visitProg(this);
        else
            return visitor.visitChildren(this);
    }
}
```

CalculatorBaseVisitor提供了访问不同节点的默认方法
```java
public class CalculatorBaseVisitor<T> extends AbstractParseTreeVisitor<T> implements CalculatorVisitor<T> {

	@Override
    public T visitProg(CalculatorParser.ProgContext ctx) { return visitChildren(ctx); }
	
	@Override
    public T visitPrint(CalculatorParser.PrintContext ctx) { return visitChildren(ctx); }
	
	@Override
    public T visitAssign (CalculatorParser.AssignContext ctx) { return visitChildren(ctx); 
    }
	
    @Override
    public T visitBlank(CalculatorParser.BlankContext ctx) { return visitChildren(ctx); }
	
	@Override public T visitMulDiv(CalculatorParser.MulDivContext ctx) { return visitChildren(ctx); }
	
	@Override public T visitAddSub(CalculatorParser.AddSubContext ctx) { return visitChildren(ctx); }
	
	@Override public T visitParenthese(CalculatorParser.ParentheseContext ctx) { return visitChildren(ctx); }
	
	@Override public T visitId(CalculatorParser.IdContext ctx) { return visitChildren(ctx); }
	
	@Override public T visitInt(CalculatorParser.IntContext ctx) { return visitChildren(ctx); }
}
```



以下面的字符串为输入：
```javascript
a = 12
b = a * 2
a + b
```
它会被解析成一棵树

![parsetree](https://github.com/zhmin/blog/blob/antrl4/antrl4/images/calculator-parse-tree.png?raw=true)


上面的三行语句，对应三个stat节点。

a = 12，匹配assign这个类型规则，ID '=' expr NEWLINE 。assign这个节点拥有4个子节点， 依次为ID， =, int， NEWLINE。其中的int子节点是匹配了expr的 INT规则

b = a * 2， 也是匹配了assign这个类型规则，ID '=' expr NEWLINE 。它有四个节点 ID， =, MulDiv， NEWLINE。其中MulDiv，代表 a * 2这个表达式，匹配了 expr op=('*'|'/') expr规则。两个expr节点，分表表示变量 a 和 数字 2.


a + b， 匹配了assign这个类型规则expr NEWLINE。

遍历顺序为

![visittree](https://github.com/zhmin/blog/blob/antrl4/antrl4/images/calculator-visit-tree.png?raw=true)


现在继承CalculatorBaseVisitor，实现四则运算
```java
public class CalculatorVisitorImp extends CalculatorBaseVisitor<Integer> {

    // 存储变量的值
    private Map<String, Integer> variable;

    public CalculatorVisitorImp() {
        variable = new HashMap<>();
    }

    // 当遇到print节点，计算出exrp的值，然后打印出来
    @Override
    public Integer visitPrint(CalculatorParser.PrintContext ctx) {
        Integer result  = ctx.expr().accept(this);
        System.out.println(result);
        return null;
    }

    // 分别获取子节点expr的值，然后做加减运算
    @Override
    public Integer visitAddSub(CalculatorParser.AddSubContext ctx) {
        Integer param1 = ctx.expr(0).accept(this);
        Integer param2 = ctx.expr(1).accept(this);
        if (ctx.op.getType() == CalculatorParser.ADD) {
            return param1 + param2;
        }
        else {
            return param1 - param2;
        }
    }

    // 分别获取子节点expr的值，然后做乘除运算
    @Override
    public Integer visitMulDiv(CalculatorParser.MulDivContext ctx) {
        Integer param1 = ctx.expr(0).accept(this);
        Integer param2 = ctx.expr(1).accept(this);
        if (ctx.op.getType() == CalculatorParser.MUL) {
            return param1 * param2;
        }
        else {
            return param1 / param2;
        }
    }

    // 当遇到int节点，直接返回数据
    @Override
    public Integer visitInt(CalculatorParser.IntContext ctx) {
        return Integer.parseInt(ctx.getText());
    }

    // 当遇到Id节点，从变量表获取值
    @Override
    public Integer visitId(CalculatorParser.IdContext ctx) {
        return variable.get(ctx.getText());
    }

    // 分别获取右边expr的值，然后赋值给变量
    @Override
    public Integer visitAssign(CalculatorParser.AssignContext ctx) {   
        String name = ctx.ID().getText();
        Integer value = ctx.expr().accept(this);
        variable.put(name, value);
        return null;
    }
}
```

### 测试 ###
```java
public class Main {

    public static void main(String[] args) throws IOException {
        String expression = "a = 12\n" +
                "b = a * 2\n" +
                "a + b\n";
        CalculatorLexer lexer = new CalculatorLexer(CharStreams.fromString(expression));
        CommonTokenStream tokens = new CommonTokenStream(lexer);
        CalculatorParser parser = new CalculatorParser(tokens);
        parser.setBuildParseTree(true);
        ParseTree root = parser.prog();
        CalculatorVisitor<Integer> visitor = new CalculatorVisitorImp();
        root.accept(visitor);
    
    }
}
```
结果输出为：36
 