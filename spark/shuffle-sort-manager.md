## shuffle sort manager ##

根据不同的情形，提供三个shuffle sort writer选择。

* BypassMergeSortShuffleWriter ： 当前rdd没有聚合， 并且分区数小于spark.shuffle.sort.bypassMergeThreshold（默认200）
* UnsafeShuffleWriter ： 当前rdd的数据支持序列化（即UnsafeRowSerializer），并且没有聚合， 并且分区数小于  2^24。

* SortShuffleWriter ： 其余

