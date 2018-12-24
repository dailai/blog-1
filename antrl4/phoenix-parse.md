### phoenix 条件语句解析原理 ###

phoenix支持在hbase上运行sql，可以支持where语句。其原理是将where语句解析，生成对应的Filter，然后提交给Hbase。具体的代码是在 org.apache.phoenix.compile.WhereCompiler 里面

```java
public class WhereCompiler {

    private static void setScanFilter(StatementContext context, FilterableStatement statement, Expression whereClause, boolean disambiguateWithFamily) {
        Scan scan = context.getScan();


        Filter filter = null;
        // 这个数目记录了，有哪些列出现在where语句中
        final Counter counter = new Counter();
        whereClause.accept(new KeyValueExpressionVisitor() {

            @Override
            public Iterator<Expression> defaultIterator(Expression node) {
                // Stop traversal once we've found multiple KeyValue columns
                if (counter.getCount() == Counter.Count.MULTIPLE) {
                    return Collections.emptyIterator();
                }
                return super.defaultIterator(node);
            }

            @Override
            public Void visit(KeyValueColumnExpression expression) {
                counter.increment(expression);
                return null;
            }
        });
        
        ......
        // 根据count的数目，决定用哪种Filter
        switch (count) {
        case NONE:
            essentialCF = table.getType() == PTableType.VIEW 
                    ? ByteUtil.EMPTY_BYTE_ARRAY 
                    : SchemaUtil.getEmptyColumnFamily(table);
            // 使用RowKeyComparisonFilter
            filter = new RowKeyComparisonFilter(whereClause, essentialCF);
            break;
        case SINGLE:
            // 使用SingleCQKeyValueComparisonFilter
            filter = disambiguateWithFamily 
                ? new SingleCFCQKeyValueComparisonFilter(whereClause) 
                : new SingleCQKeyValueComparisonFilter(whereClause);
            break;
        case MULTIPLE:
            // 如果涉及到到多个cf， 则使用 MultiCFCQKeyValueComparisonFilter
            // 否则使用 MultiCQKeyValueComparisonFilter
            filter = isPossibleToUseEncodedCQFilter(encodingScheme, storageScheme) 
                ? new MultiEncodedCQKeyValueComparisonFilter(whereClause, encodingScheme, allCFs, essentialCF) 
                : (disambiguateWithFamily 
                    ? new MultiCFCQKeyValueComparisonFilter( whereClause, allCFs, essentialCF) 
                    : new MultiCQKeyValueComparisonFilter(whereClause, allCFs, essentialCF));
            break;
        }
        // scan设置filter
        scan.setFilter(filter);

    }
```


### phoenix filter 介绍 ###

uml 图：



BooleanExpressionFilter 类， 包含了Expression成员。并且实现了Writable接口，支持序列化。因为Filter最终运行在Hbase的RegionServer上的，所以phoenix需要序列化Filter，再发送给Hbase。

序列化的格式：
```
------------------------------------
expression id  |  expression data  
------------------------------------
```

```java
abstract public class BooleanExpressionFilter extends FilterBase implements Writable {

    protected Expression expression;

    @Override
    public void readFields(DataInput input) throws IOException {
        try {
            expression = ExpressionType.values()[WritableUtils.readVInt(input)].newInstance();
            expression.readFields(input);
            expression.reset(); // Initializes expression tree for partial evaluation
        } catch (Throwable t) { // Catches incompatibilities during reading/writing and doesn't retry
            ServerUtil.throwIOException("BooleanExpressionFilter failed during reading", t);
        }
    }

    @Override
    public void write(DataOutput output) throws IOException {
        try {
            WritableUtils.writeVInt(output, ExpressionType.valueOf(expression).ordinal());
            expression.write(output);
        } catch (Throwable t) { // Catches incompatibilities during reading/writing and doesn't retry
            ServerUtil.throwIOException("BooleanExpressionFilter failed during writing", t);
        }
    }

```



Expression 是所有表达式的基类。整个语句被解析成一棵树，数的节点就是Expression表示。
```java
public interface Expression extends PDatum, Writable {

    // 计算表达式的值
    boolean evaluate(Tuple tuple, ImmutableBytesWritable ptr);
    
    // 子节点
    List<Expression> getChildren();

}
```

然后回到BooleanExpressionFilter类，它也是调用Expression的evaluate方法，返回值
```java
    protected Boolean evaluate(Tuple input) {
        try {
            //  判断表达式，是否能够计算值
            if (!expression.evaluate(input, tempPtr)) {
                return null;
            }
        } catch (IllegalDataException e) {
            return Boolean.FALSE;
        }
        // 获取表达式的值
        return (Boolean)expression.getDataType().toObject(tempPtr);
    }

```

#### row key 过滤器 ####
Hbase的数据，在pheonix中是由Tuple类表示。 RowKey是由Tuple的子类RowKeyTuple表示。
```java
public class RowKeyComparisonFilter extends BooleanExpressionFilter {

    private boolean evaluate = true; 
    private boolean keepRow = false; // 表示是否保留此行
    private RowKeyTuple inputTuple = new RowKeyTuple() // RowKey数据

    @Override
    public ReturnCode filterKeyValue(Cell v) {
        // 如果还没计算keepRow的值，则计算。
        // 如果已经计算，则跳过
        if (evaluate) {
            // 保存RowKey数据
            inputTuple.setKey(v.getRowArray(), v.getRowOffset(), v.getRowLength());
            // 调用父类BooleanExpressionFilter的evaluate方法，计算是否保留此行
            this.keepRow = Boolean.TRUE.equals(evaluate(inputTuple));
            evaluate = false;
        }
        // 如果过滤此行数据，则直接跳往下一行。
        // 否则，跳往此行的下一列。
        return keepRow ? ReturnCode.INCLUDE_AND_NEXT_COL : ReturnCode.NEXT_ROW;
    }


    @Override
    public boolean filterRow() {
        // 返回是否过滤掉此行
        return !this.keepRow;
    }


}
```


#### 单值过滤器 ####

单值可以指定column qualifier。 column family 和 qualifier。
首先看看基类SingleKeyValueComparisonFilter
```java
public abstract class  SingleKeyValueComparisonFilter extends BooleanExpressionFilter {
        
    // Cell数据
    private final SingleKeyValueTuple inputTuple = new SingleKeyValueTuple();
    
    /*
        表示是否保留此行数据。
        如果为false，有两种情况：
            1. 此行数据没有指定cf或cq
            2. 有对应cf或cq的数据，但是不符合过滤条件
    */
    private boolean matchedColumn;  

    // 指定的cf
    protected byte[] cf;
    // 指定的cq
    protected byte[] cq;

    private boolean foundColumn() {
        return inputTuple.size() > 0;
    }

    // comapre方法用于比较，是否匹配cf或cq
    protected abstract int compare(byte[] cfBuf, int cfOffset, int cfLength, byte[] cqBuf, int cqOffset, int cqLength);


    @Override
    public ReturnCode filterKeyValue(Cell keyValue) {
        if (this.matchedColumn) {
          // 如果为true，则表示符合条件，保留。跳往下一列
          return ReturnCode.INCLUDE_AND_NEXT_COL;
        }
        if (this.foundColumn()) {
          // 找到了对应的列，但是不满足条件。跳往下一行
          return ReturnCode.NEXT_ROW;
        }

        // 比较当前值Cell是否匹配cq和cf
        if (compare(keyValue.getFamilyArray(), keyValue.getFamilyOffset(), keyValue.getFamilyLength(),
                keyValue.getQualifierArray(), keyValue.getQualifierOffset(), keyValue.getQualifierLength()) != 0) {
            // 保存数据到key里面，等待使用
            // We'll need it if we have row key columns too.
            inputTuple.setKey(keyValue);
           // 
            return ReturnCode.INCLUDE_AND_NEXT_COL;
        }
        inputTuple.setCell(keyValue);

        // 计算表达式的值
        if (!Boolean.TRUE.equals(evaluate(inputTuple))) {
            // 返回false，表示不符合过滤条件。跳往下一行
            return ReturnCode.NEXT_ROW;
        }
        
        this.matchedColumn = true;
        // 返回false，表示符合过滤条件。跳往下一列
        return ReturnCode.INCLUDE_AND_NEXT_COL;
    }
}
``` 



SingleCQKeyValueComparisonFilter类，表示只匹配column qualifier,不管这个记录来个哪个column family。

```java
public class SingleCQKeyValueComparisonFilter extends SingleKeyValueComparisonFilter {
    
    @Override
    protected final int compare(byte[] cfBuf, int cfOffset, int cfLength, byte[] cqBuf, int cqOffset, int cqLength) {
        // 比较column qualifier
        return Bytes.compareTo(cq, 0, cq.length, cqBuf, cqOffset, cqLength);
    }
}
```

SingleCFCQKeyValueComparisonFilter类，表示即需要匹配column family， 也要匹配column qualifier
```java
public class SingleCFCQKeyValueComparisonFilter extends SingleKeyValueComparisonFilter {

    @Override
    protected final int compare(byte[] cfBuf, int cfOffset, int cfLength, byte[] cqBuf, int cqOffset, int cqLength) {
        // 比较column family
        int c = Bytes.compareTo(cf, 0, cf.length, cfBuf, cfOffset, cfLength);
        if (c != 0) return c;
        // 再比较column qualifier
        return Bytes.compareTo(cq, 0, cq.length, cqBuf, cqOffset, cqLength);
    }
}
```