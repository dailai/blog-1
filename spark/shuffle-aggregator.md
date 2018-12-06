# Aggregator #

## 原理 ##

Aggregator表示根据key分组对value聚合。 数据源的格式是key，value类型。key的类型对应着K，value的类型对应着V。它有三个重要的属性，都是函数:

* createCombiner，V => C，接收V类型，返回C类型
* mergeValue，(C, V) => C， 将V类型的数据，合并到C类型
* mergeCombiners， (C, C) => C)， 合并C类型的数据

这个三个函数，都有着不同的作用。现在讲讲聚合操作的原理。聚合一般发生在shuffle的阶段。

这里演示一个例子

 (key_0, value_0), 	(key_0, value_1), 	(key_0, value_2)

(key_1, value_3),	(key_1, value_4)

首先将相同的Key分组。然后对每一个分组，做聚合。以key_0分组为例

第一步，将第一个value数据转化为Combiner类型（对应着C。比如遇到(key_0, value_0)，将value_0 生成 combine_0数据。这里调用了createCombiner函数

第二步，将后面的value，依次合并到C类型的数据。这就是mergeValue的作用。比如遇到(key_0, value_1)，讲述value_1添加到combine_0数据里

第三部，然后继续遇到(key_0, value_2),恰好这时候触发spill，会新建一个combiner_1数据, 将数据value_2添加combiner_1里。

第四部，最后将所有的combiner数据汇总，比如 合并combiner_0 和 combiner_1，这里调用了mergeCombiners函数。



## 示例 ##

当rdd触发reduce或groupby等一些操作时，就会触发聚合。下面依次来看看它们是怎么生成Aggregator。这里需要说明下，RDD指定了隐式转换 ，可以转换成PairRDDFunctions，OrderedRDDFunctions等。

### groupby ###

```scala
def groupBy[K](f: T => K)(implicit kt: ClassTag[K]): RDD[(K, Iterable[T])] = withScope {
  // 使用默认的分区器
  groupBy[K](f, defaultPartitioner(this))
}

def groupBy[K](f: T => K, p: Partitioner)(implicit kt: ClassTag[K], ord: Ordering[K] = null)
  : RDD[(K, Iterable[T])] = withScope {
    val cleanF = sc.clean(f)
    // 首先使用函数f，生成key。调用map返回（key， value）类型的RDD
    // 这里RDD隐式转换成PairRDDFunctions， 然后调用groupByKey方法
    this.map(t => (cleanF(t), t)).groupByKey(p)
}
```

 接下来看看groupByKey方法，这里生成了Aggregator的函数。它使用Combiner类型是CompactBuffer。CompactBuffer可以简单的理解成一个array，只不过将第一个和第二个元素存到单独的变量，将后面的元素存到array里，但是它对外提供了和array一样的接口。这样对于存储数量小的集合，减少了数组的分配。

```scala
def groupByKey(partitioner: Partitioner): RDD[(K, Iterable[V])] = self.withScope {
  // 生成CompactBuffer，然后将value添加到CompactBuffer里
  val createCombiner = (v: V) => CompactBuffer(v)
  // 将新的value添加到CompactBuffer
  val mergeValue = (buf: CompactBuffer[V], v: V) => buf += v
  // 将多个CompactBuffer合并
  val mergeCombiners = (c1: CompactBuffer[V], c2: CompactBuffer[V]) => c1 ++= c2
  // 调用combineByKeyWithClassTag 生成RDD
  val bufs = combineByKeyWithClassTag[CompactBuffer[V]](
    createCombiner, mergeValue, mergeCombiners, partitioner, mapSideCombine = false)
  bufs.asInstanceOf[RDD[(K, Iterable[V])]]
}
```

combineByKeyWithClassTag根据分区器是否一样，来判断是否需要shuffle

```scala
def combineByKeyWithClassTag[C](
    createCombiner: V => C,
    mergeValue: (C, V) => C,
    mergeCombiners: (C, C) => C,
    partitioner: Partitioner,
    mapSideCombine: Boolean = true,
    serializer: Serializer = null)(implicit ct: ClassTag[C]): RDD[(K, C)] = self.withScope {
  // 生成aggregator
  val aggregator = new Aggregator[K, V, C](
    self.context.clean(createCombiner),
    self.context.clean(mergeValue),
    self.context.clean(mergeCombiners))
  if (self.partitioner == Some(partitioner)) {
    // 如果新的分区器和原有的一样，则表示key的分布是一样。所以没必要shuffle，直接调用mapPartitions
    self.mapPartitions(iter => {
      val context = TaskContext.get()
      new InterruptibleIterator(context, aggregator.combineValuesByKey(iter, context))
    }, preservesPartitioning = true)
  } else {
    // 否则，生成ShuffledRDD
    new ShuffledRDD[K, V, C](self, partitioner)
      .setSerializer(serializer)
      .setAggregator(aggregator)
      .setMapSideCombine(mapSideCombine)
  }
}
```



### reduceByKey ##

```
def reduceByKey(partitioner: Partitioner, func: (V, V) => V): RDD[(K, V)] = self.withScope {
  // 这里可以看到createCombiner函数，只是返回value值
  // mergeValues函数，是传入的func函数
  // mergeCombiners函数，也还是传入的func函数
  combineByKeyWithClassTag[V]((v: V) => v, func, func, partitioner)
}

def reduceByKey(func: (V, V) => V, numPartitions: Int): RDD[(K, V)] = self.withScope {
  reduceByKey(new HashPartitioner(numPartitions), func)
}

def reduceByKey(func: (V, V) => V): RDD[(K, V)] = self.withScope {
  reduceByKey(defaultPartitioner(self), func)
}
```

### countByKey ###

```scala
def countByKey(): Map[K, Long] = self.withScope {
  // 调用mapValues生成(key, 1)类型的RDD，然后定义了相加的函数
  self.mapValues(_ => 1L).reduceByKey(_ + _).collect().toMap
}
```

