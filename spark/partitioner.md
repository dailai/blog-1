# 分区器 #

当触发到shuffle的时候，就需要分区器指定每个数据的对应的分区地址。分区器的接口很简单，

```scala
abstract class Partitioner extends Serializable {
  // 分区的数目
  def numPartitions: Int
  // 计算key对应的分区索引
  def getPartition(key: Any): Int
}
```

分区器的实现有两种，HashPartitioner和RangePartitioner。

## HashPartitioner ##

HashPartitioner的原理很简单，只是计算key的hashcode，然后以分区数目取余数。注意HashPartition而不能支持key为数组类型。

```scala
def getPartition(key: Any): Int = key match {
  case null => 0
  case _ => Utils.nonNegativeMod(key.hashCode, numPartitions)
}

def nonNegativeMod(x: Int, mod: Int): Int = {
  val rawMod = x % mod
  rawMod + (if (rawMod < 0) mod else 0)
}
```



## RangePartitioner ##

RangePartitioner的原理会稍微复杂一些，会遍历rdd的所有分区，从每个分区都会采样，然后根据样本，生成每个新的分区的范围值。根据范围值，就可以把key分布到对应的分区索引。

采样算法使用的是蓄水池采样。它的使用场景是从不知道大小的集合里，能够等概率的提取k个元素。算法原理是：

1. 先从集合遍历k个元素，存储起来
2. 依次遍历余下的元素，比如遍历第m个元素，然后生成[0, m)de随机数 i，如果小于k，则替换掉原来的第i个元素

```
def reservoirSampleAndCount[T: ClassTag](
    input: Iterator[T],
    k: Int,
    seed: Long = Random.nextLong())
  : (Array[T], Long) = {
  val reservoir = new Array[T](k)
  // Put the first k elements in the reservoir.
  var i = 0
  while (i < k && input.hasNext) {
    val item = input.next()
    reservoir(i) = item
    i += 1
  }

  
  if (i < k) {
    // 如果分区的数据都已经遍历完，则直接返回
    val trimReservoir = new Array[T](i)
    System.arraycopy(reservoir, 0, trimReservoir, 0, i)
    (trimReservoir, i)
  } else {
    // 如果数据还有剩余，则进行蓄水池采样
    var l = i.toLong
    val rand = new XORShiftRandom(seed)
    while (input.hasNext) {
      val item = input.next()
      l += 1
      // 生成随机数
      val replacementIndex = (rand.nextDouble() * l).toLong
      if (replacementIndex < k) {
      	// 如果随机数小于k， 则替代原来的元素
        reservoir(replacementIndex.toInt) = item
      }
    }
    // 返回采样结果和已遍历的数目
    (reservoir, l)
  }
}
```

上述reservoirSampleAndCount方法是从一个分区里采样。sketch会调用rdd的mapPartitionsWithIndex，对每个分区采样，并且调用collect返回采样结果。

```
def sketch[K : ClassTag](
    rdd: RDD[K],
    sampleSizePerPartition: Int): (Long, Array[(Int, Long, Array[K])]) = {
  val shift = rdd.id
  val sketched = rdd.mapPartitionsWithIndex { (idx, iter) =>
    val seed = byteswap32(idx ^ (shift << 16))
    val (sample, n) = SamplingUtils.reservoirSampleAndCount(
      iter, sampleSizePerPartition, seed)
    Iterator((idx, n, sample))
  }.collect()
  val numItems = sketched.map(_._2).sum
  // 返回采样结果的数目和采样结果
  (numItems, sketched)
}
```

采样结果出来后，还需要处理。这里首先会将分区的采样结果分成两种情况，一种是采样的数目小于预期，这种情况需要重新采样，获取足够的样本。

```scala
// 计算平均系数，表明每遍历 1个元素，应该获得的采样数目
val fraction = math.min(sampleSize / math.max(numItems, 1L), 1.0)
// 存储结果， 存储类型为 (key, 权重)
val candidates = ArrayBuffer.empty[(K, Float)]
// 采样数目小于预期的partition
val imbalancedPartitions = mutable.Set.empty[Int]
// 遍历采样结果，idx表示分区索引，n表示遍历数目，sample为该分区的采样结果
sketched.foreach { case (idx, n, sample) =>
    if (fraction * n > sampleSizePerPartition) {
        // 如果预期的采样结果，大于实际的采样结果，则该分区需要重新采样
        imbalancedPartitions += idx
    } else {
        // 计算权重
        val weight = (n.toDouble / sample.length).toFloat
        // 添加到candidates
        for (key <- sample) {
            candidates += ((key, weight))
        }
    }
    // 分区重新采样
    if (imbalancedPartitions.nonEmpty) {
        // 实例PartitionPruningRDD，使用imbalancedPartitions挑选出需要采样的分区
        val imbalanced = new PartitionPruningRDD(rdd.map(_._1), imbalancedPartitions.contains)
        // 调用rdd的sample取样
        val reSampled = imbalanced.sample(withReplacement = false, fraction, seed).collect()
        // 计算权重
        val weight = (1.0 / fraction).toFloat
        // 添加到candidates
        candidates ++= reSampled.map(x => (x, weight))
    }
    // 排序candidates，生成rangeBounds
    RangePartitioner.determineBounds(candidates, partitions)
}

```

生成rangeBounds的原理比较简单，首先将candidates按照key排序。然后根据weight的总和，计算每个分区的元素的weight和。

```scala
def determineBounds[K : Ordering : ClassTag](
    candidates: ArrayBuffer[(K, Float)],
    partitions: Int): Array[K] = {
    // 按照key排序
    val ordered = candidates.sortBy(_._1)
    // 计算权重和
    val sumWeights = ordered.map(_._2.toDouble).sum
    // 计算每个分区的weight和
    val step = sumWeights / partitions
    // 遍历元素
    var cumWeight = 0.0
    while ((i < numCandidates) && (j < partitions - 1)) {
        val (key, weight) = ordered(i)
        cumWeight += weight
        if (cumWeight >= target) {
            // 更新rangeBounder值
            ....
        }
        .....
    }
}
         
    
```



## 默认分区器 ##

当有时候触发shuffle，但没有指定partitioner。spark会自动生成默认的分区器。

首先去寻找父类rdd的partitioner，找到其中的最大值(按照分区数量排序)。

如果父类rdd没有，但是spark.default.parallelism有在配置中指定，则使用该数值，创建HashPartitioner。

否则，就找到父类rdd的最大分区数目，使用该数值，创建HashPartitioner。

```scala
def defaultPartitioner(rdd: RDD[_], others: RDD[_]*): Partitioner = {
  val rdds = (Seq(rdd) ++ others)
  val hasPartitioner = rdds.filter(_.partitioner.exists(_.numPartitions > 0))
  if (hasPartitioner.nonEmpty) {
    hasPartitioner.maxBy(_.partitions.length).partitioner.get
  } else {
    if (rdd.context.conf.contains("spark.default.parallelism")) {
      new HashPartitioner(rdd.context.defaultParallelism)
    } else {
      new HashPartitioner(rdds.map(_.partitions.length).max)
    }
  }
}
```

