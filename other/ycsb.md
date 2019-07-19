# 基准测试框架 ycsb



## 测试流程

框架的测试流程很简单，如下图所示



Main ： 负责框架的启动，它会读取参数并且负责初始化，比如线程组的初始化，统计计算的初始化

工作线程组：负责实际的增删改查操作

统计计算：负责将请求的响应时间统计，计算出平均值，最大最小值，还有 tp95 和tp99。

统计结果导出：负责将上一部的计算结果，以文本格式或 json 格式导出到标准输出或文件



## 工作线程组

工作线程组有着下列多种类型的线程，

* 发送请求线程 ClientThread，它会自动构建出请求，发送到测试数据库，并且将响应时间汇报给统计计算
* 终止工作线程 TerminatorThread，如果指定了运行时间，那么它就会定时发送终止信号，通知 ClientThread 停止
* 汇报进度线程 StatusThread，它会定期输出 ClientThread 的请求已发送数和未发送数





## 数据库抽象层

因为 ycsb 框架支持多种数据库，所以它将底层的数据库操作封装了一层，由 DB 类表示。这样如果用户需要增加数据库的支持，只需要继承 DB 类，实现对应的增删改查接口就行。下面列举了操作接口，

```java
public abstract class DB {

  public abstract Status read(String table, String key, Set<String> fields, Map<String, ByteIterator> result);

  public abstract Status scan(String table, String startkey, int recordcount, Set<String> fields,
                              Vector<HashMap<String, ByteIterator>> result);
  
  public abstract Status update(String table, String key, Map<String, ByteIterator> values);

  public abstract Status insert(String table, String key, Map<String, ByteIterator> values);
    
  public abstract Status delete(String table, String key);
}
```



注意到DB 除了增删改查四个操作，还新增了一个 scan 操作。它表示范围查询，在一些 nosql 数据库（比如HBase）中有单独的 scan 操作。在 sql 数据库中，ycsb 使用语句  `WHERE id >= target_value ORDER BY id LIMIT number` 来表示。



## 数据生成

ycsb 支持生成请求，并且请求的数据还可以是不同的分布概率，比如均匀分布，热点分布，递增分布等。

ycsb 支持三种类型的数据生成，数字类型，字符串类型，时间戳类型。下面依次介绍这些类型的生成原理



### 基类

数据生成的基类是 Generator，子类需要实现它的两个接口。

```java
public abstract class Generator<V> {
  // 生成新的数据
  public abstract V nextValue();
 
  // 返回上一次的数据
  public abstract V lastValue();
}
```



### 数字类型

数字类型的生成由 NumberGenerator 类和它的子类构成。它的子类比较多，根据数值分布，分为规律型，概率型。



规律型

ConstantIntegerGenerator ，返回值都是一样的

CounterGenerator，返回值递增加一，可以通过 insertstart 参数指定起始值，默认为 0。

SequentialGenerator， 给定取值范围，轮询返回。







概率型

我们知道计算机的随机实现其实是伪随机，这里简单认为随机数的概率分布为均匀分布，记为随机变量 X。

ExponentialGenerator返回的数据值， constanct * log X。变量 X 为等概率分布，取值范围是 ( 0， 1 ] 。这种分布会造成数据倾向于大值。

HistogramGenerator ，初始化直方图， 假设随机一个整数 X，它会遍历每个 bucket 的数值，知道累积的值大于 X，就返回这个bucket 。如果遍历完还没达到条件，则返回最后一个。



HotspotIntegerGenerator， 给定值的分布范围，将其切割为热点数据和非热点数据两段。然后根据设置的热点数据概率，生成数据。



ZipfianGenerator，满足 zipfian 概率分布。

UniformLongGenerator， 给定区间范围，满足均匀分布

SkewedLatestGenerator， 在ZipfianGenerator基础上，增加了递增的偏移值





### 字符串类型

DiscreteGenerator， 给定取值集合和对应的权重，返回值的概率为权重比例

FileGenerator，给定数据文件，按照顺序依次读取并返回一行数据

IncrementingPrintableStringGenerator， 给定字符串的取值集合，然后生成多个字符串，连接成一个字符串。每条数据都是由上条数据递增加一的结果。

UniformGenerator，给定字符串的取值集合，然后等概率的从中取值。





### 时间戳类型

UnixEpochTimestampGenerator， 给定起始时间戳和时间单位。每个生成的值都是递增的，递增的差值为时间单元的固定倍数。



RandomDiscreteTimestampGenerator， 每次生成的值也是递增的，不过递增的差值为时间单元的随机倍数。







## 构建请求

在上面介绍了数据的生成，现在要介绍如何应用到构建请求。请求类型对应于DB 的5种类型。

Workload 的子类负责构建请求，并且会根据配置，合理的限制各个请求类型数量。



%plantuml%

@startuml
abstract class Workload
class CoreWorkload
class ConstantOccupancyWorkload
class TimeSeriesWorkload

Workload <|-- CoreWorkload
CoreWorkload <|-- ConstantOccupancyWorkload
Workload <|-- TimeSeriesWorkload
@enduml

%endplantuml%



Workload 有两种工作方式，如果指定了 load 参数，表示要写一些测试数据。

在准备完测试数据后，就可以发送各种请求。

doInsert 方法对应于第一种工作方式

doTransaction 方法对应于第二种工作方式

```java
public abstract class Workload {

  public abstract boolean doInsert(DB db, Object threadstate);

  public abstract boolean doTransaction(DB db, Object threadstate);
}
```



我们先来看看 CoreWorkload 类，

先来看看第一种工作方式，它主要使用到

```java
public class CoreWorkload extends Workload {
  protected NumberGenerator keysequence; // 生成主键
  
  private List<String> fieldnames; // 字段名集合
  protected NumberGenerator fieldlengthgenerator; // 在构建字段值，指定字段的数据长度
    
  public boolean doInsert(DB db, Object threadstate) {
    // 生成主键
    int keynum = keysequence.nextValue().intValue();
    String dbkey = buildKeyName(keynum);
    // 生成字段值，每个字段的长度都是通过 fieldlengthgenerator 生成的
    HashMap<String, ByteIterator> values = buildValues(dbkey);
    // 调用 DB 完成底层插入
	status = db.insert(table, dbkey, values);      
    // .....
}   
```



再来看看第一种工作方式，它主要使用到





