# ExternalShuffleService



## 前言

Spark 默认 shuffle 数据都已 Block 的形式存到 BlockManager 里面，每个 Executor 进程都会运行一个 BlockManager 服务。Executor 进程不仅需要负责大量计算，还需要负责 shuffle 数据的保存和提供读取的服务，如果Executor 进程挂掉了，那么这个Executor 上的 shuffle 数据也就丢弃掉了。为了分离这些服务，Spark 支持 shuffle 数据保存和读取的服务放在 Executor 外运行。

YarnShuffleService 服务是在每个主机上运行，它主要消耗磁盘和网络，所以相比多个 Executor 并发，没有影响。

而且分离之后，Spark 可以基于此实现动态分配，意思是空闲的 Executor 需要及时释放掉。如果没有分离，那么就会造成

## 架构图





## Yarn AuxServices

Yarn NodeManager 服务启动的时候，会启动一些额外的辅助服务，这些辅助服务的运行周期同 NodeManager 相同。每个辅助服务由 AuxiliaryService 类表示，用户需要继承并实现 AuxiliaryService 类，并且还需要配置 Yarn 的 yarn.nodemanager.aux-services选项，这样才能被 NodeManager 发现。当需要配置多个辅助服务时，使用逗号隔开。

```xml
  <property>
    <name>yarn.nodemanager.aux-services</name>
    <value>mapreduce_shuffle, custom_service</value>
  </property>
```



## YarnShuffleService

YarnShuffleService 本质是运行了一个 Spark Rpc 服务，它处理两种 Rpc 消息：

```
OpenBlocks
RegisterExecutor
```





## Shuffle 数据存储位置

我们知道 Spark 每次执行 shuffle 操作，都会为它生成一个唯一的 shuffleId。每个 Map 端也有一个唯一的 mapId，它生成的每份 shuffle 数据，也有一个唯一的 reduceId。那么通过 shuffleId，mapId，reduceId 就可以在单个 Job 内，确定哪份数据。如果加上 appId，那么即使运行了多个 Job，也可以唯一确定数据。



executerId 是在同个 app 中用来标识 Executor 的 id 号。

但是Shuffle 数据都会被存放到磁盘中，我们上面只是确定了逻辑上的地址，但是不能确定文件存储的具体位置，那么如何找到这些数据的位置呢。



每个 Executor 都会创建一个DiskBlockManager 的实例。它会将数据文件，分散到不同的目录下。



appId 和 executorId 可以确定存储的目录，shuffle 文件名格式 `shuffle_{shuffleId}_{mapId}_0.data`，对应的索引文件名格式 `shuffle_{shuffleId}_{mapId}_0.index`。

因为所有 shuffle 数据都会汇集成一个大文件，并且生成了索引文件。这样只需要根据 reduceId ，从索引中找到对应文件中的位置，然后返回 FileSegmentManagedBuffer 实例。







​			

## DiskBlockManager 原理

DiskBlockManager 负责将数据文件分散到不同的目录下。它的目录分为多级，第一级称为 localdir 列表，每个 localDirs 有多个二级目录，称为 subDirs。

第一级目录列表，需要根据 spark 运行的运行模式（比如运行在 Yarn，Mesos 上），是否开启了 spark.shuffle.service.enabled ，才能确定。如果 spark 运行在 Yarn 上，首先找到 Yarn 的配置项 yarn.nodemanager.local-dirs ，然后在这些目录下创建一个特殊的目录，这些目录名有 blockmgr 前缀名加上 uuid 号组成。 

然后在每个 localDir 目录下，生成数量固定的子目录，子目录的名称格式为十六进制，不足两位则补零。如下所示，

```bash
blockmgr-417dc935-ee3e-4a70-831c-fcadd7ee9a7c/
├── 00
├── 01
├── 02
└── 03
```



```scala
val hash = Utils.nonNegativeHash(filename)  // 计算文件名的hash值
val dirId = hash % localDirs.length  // 取模计算出localDir的位置
// 取模计算出subDir的位置，subDirsPerLocalDir表示每个localDir的子目录数
val subDirId = (hash / localDirs.length) % subDirsPerLocalDir  
```







