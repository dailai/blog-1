## shuffle sort manager ##

根据不同的情形，提供三个shuffle sort writer选择。

* BypassMergeSortShuffleWriter ： 当前shuffle没有聚合， 并且分区数小于spark.shuffle.sort.bypassMergeThreshold（默认200）
* UnsafeShuffleWriter ： 当前rdd的数据支持序列化（即UnsafeRowSerializer），并且没有聚合， 并且分区数小于  2^24。

* SortShuffleWriter ： 其余



ShuffleManager 确定了shuffle的算法。它根据shuffle dependcy，来选择ShuffleWriter和ShuffleReader。





@startuml

abstract class ShuffleHandle

class  BaseShuffleHandle

class BypassMergeSortShuffleHandle

class SerializedShuffleHandle

ShuffleHandle <|-- BaseShuffleHandle
BaseShuffleHandle <|-- BypassMergeSortShuffleHandle
BaseShuffleHandle <|-- SerializedShuffleHandle

interface ShuffleManager

class SortShuffleManager

ShuffleManager <|-- SortShuffleManager

abstract class ShuffleWriter

class BypassMergeSortShuffleWriter

class SortShuffleWriter

class UnsafeShuffleWriter

ShuffleWriter <|-- BypassMergeSortShuffleWriter

ShuffleWriter <|-- SortShuffleWriter

ShuffleWriter <|-- UnsafeShuffleWriter

interface ShuffleReader

class BlockStoreShuffleReader

ShuffleReader <|-- BlockStoreShuffleReader

ShuffleManager --> ShuffleHandle : registerShuffle

ShuffleManager --> ShuffleWriter : getWriter

ShuffleManager --> ShuffleReader : getReader

@enduml



ShuffleHandle 会保存shuffle writer算法需要的信息。根据ShuffleHandle的类型，来选择ShuffleWriter的类型。

ShuffleWriter负责在map端生成中间数据，ShuffleReader负责在reduce端读取和整合中间数据。



ShuffleManager 提供了registerShuffle方法，根据shuffle的dependency情况，选择出ShuffleHandler。

registerShuffle方法比较简单，这里简单说下原理。它对于不同的ShuffleHandler，有着不同的条件

* BypassMergeSortShuffleHandle :  该shuffle不需要聚合，并且reduce端的分区数目小于配置项spark.shuffle.sort.bypassMergeThreshold，默认为200
* SerializedShuffleHandle  :  该shuffle支持数据不需要聚合，并且必须支持序列化时seek位置，还需要reduce端的分区数目小于16777216（1 << 24 + 1）
* BaseShuffleHandle  :  其余情况



对于返回的ShuffleHandler， 会在getWriter方法中有用到。getWriter方法提供了选择shuffle writer，原理比较简单

如果是BypassMergeSortShuffleHandle， 则选择BypassMergeSortShuffleWriter

如果是SerializedShuffleHandle， 则选择UnsafeShuffleWriter

如果是BaseShuffleHandle， 则选择SortShuffleWriter



ShuffleWriter的使用方法

首先调用write方法，添加数据，完成排序

最后调用stop方法，返回MapStatus结果





BypassMergeSortShuffleHandle 原理



DiskBlockObjectWriter 原理 

它装饰了文件输出流，FileOutputStream --> TimeTrackingOutputStream --> ManualCloseBufferedOutputStream  --> 压缩流 --> 序列化流

TimeTrackingOutputStream增加统计写入花费时间

ManualCloseBufferedOutputStream 继承 OutputStream， 更改了close方法，必须调用manualClose方法手动关闭。



DiskBlockObjectWriter 支持添加写，并且返回FileSegment。FileSegment包含了添加写的数据信息。



BypassMergeSortShuffleHandle会为每个reduce的分区，创建DiskBlockObjectWriter。根据Key判断出所在的分区索引，然后添加到对应的DiskBlockObjectWriter，写入到磁盘临时文件。

最后所有的DiskBlockObjectWriter的数据，按照分区索引，汇合到同一个文件，保存在ShuffleBlock中。

并且创建索引文件，记录分区数据对应的文件所在位置。

结果保存在MapStatus







UnsafeShuffleWriter 原理





LongArray可以看作是一个Long类型的数组，不过它支持堆内和堆外内存。

这里Long类型包含了三部分的数组，分区索引，所在的内存块，所在内存块中的偏移位置。一个Long类型占据64bit，格式如下：

```shell
---------------------------------------------------------------
     24 bit           |    13 bit        |      27 bit
---------------------------------------------------------------
   partitionId        |   memoryBlock    |      offset
--------------------------------------------------------------
```





ShuffleInMemorySorter

ShuffleInMemorySorter包含了LongArray， 并且提供了替换LongArray接口。

ShuffleExternalSorter 包含ShuffleInMemorySorter， 支持申请内存和磁盘溢写。



ShuffleExternalSorter指定了申请内存块的最大容量，不能超过 1<< 27，也就是不能超过27位。

