# RDD之间的联系 #

当RDD执行transform操作时(比如map，filter，groupby)，就会创造新的RDD。RDD之间的联系类型，就是由Dependency表示。根据rdd的分区情况，可以分为两大类，窄依赖NarrowDependency， 宽依赖ShuffleDependency。

## 窄依赖 ##

当父RDD的每个分区只对应于子RDD的一个分区，这种情况是窄依赖。这里分为两种情形：

### OneToOneDependency ###

父RDD的分区索引与子RDD的分区索引相同，比如map和filter操作

```
class OneToOneDependency[T](rdd: RDD[T]) extends NarrowDependency[T](rdd) {
  // 返回父RDD对应的partition
  override def getParents(partitionId: Int): List[Int] = List(partitionId)
}
```

### RangeDependency ###

父RDD的分区索引与子RDD的分区索引，存在线性关系，比如union操作

```
// inStart表示父RDD的起始索引，一般是0
// outStart表示父RDD对应子RDD的起始索引
// length表示父RDD的分区数目
class RangeDependency[T](rdd: RDD[T], inStart: Int, outStart: Int, length: Int)
  extends NarrowDependency[T](rdd) {
  // 线性计算出父RDD的分区索引
  override def getParents(partitionId: Int): List[Int] = {
    if (partitionId >= outStart && partitionId < outStart + length) {
      List(partitionId - outStart + inStart)
    } else {
      Nil
    }
  }
```



## 宽依赖 ##

当子RDD的分区数据来源于父RDD的多个分区时，两者之间的依赖关系就是宽依赖。ShuffleDependency有以下属性：

* rdd， 指向父RDD
* partitioner， 子RDD的分区器
* keyClassName， key值类型
* valueClassName， value值类型
* aggregator， 聚合



## RDD的种类 ##

### RDD的基本方法 ###

iterator，返回指定分区的数据，如果没有则调用compute方法

compute， 计算指定分区的数据

### 源数据RDD ###

spark支持读取不同的数据源，如下：

* 支持hdfs文件读取， HadoopRDD
* 支持jdbc读取数据库，JdbcRDD

### MapPartitionsRDD ###

当rdd调用map或filter操作时，会返回MapPartitionsRDD。MapPartitionsRDD对应的依赖关系是OneToOneDependency。

很明显MapPartitionsRDD的compute方法，调用父RDD的iterator方法获取数据，然后调用函数f计算值。

```scala
override def compute(split: Partition, context: TaskContext): Iterator[U] =
  f(context, split.index, firstParent[T].iterator(split, context))
```

### ShuffledRDD ###

```scala
override def compute(split: Partition, context: TaskContext): Iterator[(K, C)] = {
  val dep = dependencies.head.asInstanceOf[ShuffleDependency[K, V, C]]
  SparkEnv.get.shuffleManager.getReader(dep.shuffleHandle, split.index, split.index + 1, context)
    .read()
    .asInstanceOf[Iterator[(K, C)]]
}
```

