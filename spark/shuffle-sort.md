# shuffle 排序 #

在进行shuffle之前，map端会先将数据进行排序。排序的规则，根据不同的场景，会分为两种。首先会根据Key将元素分成不同的partition。第一种只需要保证元素的partitionId排序，但不会保证同一个partitionId的内部排序。第二种是既保证元素的partitionId排序，也会保证同一个partitionId的内部排序。

因为有些shuffle操作涉及到聚合，对于这种需要聚合的操作，使用PartitionedAppendOnlyMap来排序。

对于不需要聚合的，则使用PartitionedPairBuffer排序。



## 排序规则 ##

排序的规则，根据不同的场景，分为是否rdd指定了order两种。

### 指定order ###

如果rdd指定了order，元素的partitionId排序，并且也会保证同一个partitionId的内部排序。

这里比较的数据格式为（partitionId, key）。

```scala
def partitionKeyComparator[K](keyComparator: Comparator[K]): Comparator[(Int, K)] = {
  new Comparator[(Int, K)] {
    override def compare(a: (Int, K), b: (Int, K)): Int = {
      // 优先比较partition
      val partitionDiff = a._1 - b._1
      if (partitionDiff != 0) {
        partitionDiff
      } else {
        // 如果partition相等，则继续比较key
        keyComparator.compare(a._2, b._2)
      }
    }
  }
}
```

继续看keyComparator的定义, 在ExternalSorter里定义。这里只是比较了两个的hashCode。

```scala
  private val keyComparator: Comparator[K] = ordering.getOrElse(new Comparator[K] {
    override def compare(a: K, b: K): Int = {
      val h1 = if (a == null) 0 else a.hashCode()
      val h2 = if (b == null) 0 else b.hashCode()
      if (h1 < h2) -1 else if (h1 == h2) 0 else 1
    }
  })
```

### 不指定order ###

如果rdd没有指定order，则只需要比较partitionId排序，而不需要对同一个partitionId内部排序。

这里比较的数据格式为（partitionId, key）

```scala
def partitionComparator[K]: Comparator[(Int, K)] = new Comparator[(Int, K)] {
    override def compare(a: (Int, K), b: (Int, K)): Int = {
      a._1 - b._1
    }
  }
```



## 排序种类 ##

### 非聚合排序 ###

