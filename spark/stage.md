# Stage #

## ShuffleMapStage ##

因为shuffle涉及到数据输出，所以它保存shuffle的数据输出地址。它有一个重要的属性outputLocs，就是保存了每个分区的输出地址，数据类型是Array[ List[MapStatus] ]，数组的索引是分区索引，值为MapStatus列表。当一个分区的任务完成后，就会更新outputLocs。

findMissingPartitions是获取当前stage，有哪些分区还没有计算出结果。这里直接根据outputLocs的数据，就可以判断

```scala
override def findMissingPartitions(): Seq[Int] = {
  // 找到outputLocs对应数据为空的分区
  val missing = (0 until numPartitions).filter(id => outputLocs(id).isEmpty)
  assert(missing.size == numPartitions - _numAvailableOutputs,
    s"${missing.size} missing, expected ${numPartitions - _numAvailableOutputs}")
  missing
}
```





### MapStatus ###

MapStatus 是 ShuffleMapTask的执行结果。它有两个方法，location指明数据存放位置，getSizeForBlock返回指定reduceID的数据大小，注意这个大小是有一定差值的。MapStatus有两个子类，CompressedMapStatus 和 HighlyCompressedMapStatus。当reduceId超过了2000， 就使用HighlyCompressedMapStatus。否则使用CompressedMapStatus 。

MapStatus会将结果进行压缩，因为一个MapStatus会包含多个reduce的数据长度，所以对于Long类型的长度，转换为Byte类型。虽然结果压缩了，但是精确度却有一定的损失。算法如下，通过对数的方式压缩成整数类型，最大值为255。最大可以表示35GB的长度。

```scala
private[spark] object MapStatus {
    private[this] val LOG_BASE = 1.1
    
    def compressSize(size: Long): Byte = {
    	if (size == 0) {
      		0
    	} else if (size <= 1L) {
      		1
    	} else {
      		math.min(255, math.ceil(math.log(size) / math.log(LOG_BASE)).toInt).toByte
    	}
  }
```

CompressedMapStatus的原理比较简单，只是将长度压缩保存。

```scala
private[spark] class CompressedMapStatus(
    private[this] var loc: BlockManagerId,
    private[this] var compressedSizes: Array[Byte])
  extends MapStatus with Externalizable {
      // 接收Long类型的数组，数组的索引是reduceId，索引项是对应的长度
      def this(loc: BlockManagerId, uncompressedSizes: Array[Long]) {
          // 调用上面的compressSize方法， 将Long类型的长度，转换为Byte类型
          this(loc, uncompressedSizes.map(MapStatus.compressSize))
      }
      
      override def getSizeForBlock(reduceId: Int): Long = {
          // 获取压缩的数据长度，然后还原成Long类型
          MapStatus.decompressSize(compressedSizes(reduceId))
      }
```

HighlyCompressedMapStatus也会将结果压缩，只不过原理不同。它将那些长度特别大的reduce，会将长度压缩起来。对于别的reduce，直接返回平均值。

```scala
private[spark] class HighlyCompressedMapStatus private (
    private[this] var loc: BlockManagerId,
    // 哪些reduce数据不为空的数目
    private[this] var numNonEmptyBlocks: Int,
    // reduce数据为空的集合，这里使用了RoaringBitmap，也是为了节省内存
    private[this] var emptyBlocks: RoaringBitmap,
    // 平均长度
    private[this] var avgSize: Long,
    // 长度特别大的reduce集合
    private var hugeBlockSizes: Map[Int, Byte]) {
    
    override def getSizeForBlock(reduceId: Int): Long = {
    	assert(hugeBlockSizes != null)
        // 如果reduce对应的数据为空，则直接返回0
    	if (emptyBlocks.contains(reduceId)) {
      		0
    	} else {
      		hugeBlockSizes.get(reduceId) match {
                 // 如果该reduceId特别大，则从 hugeBlockSizes集合中，获取长度并解压
        		case Some(size) => MapStatus.decompressSize(size)
                // 否则，返回平局长度
        		case None => avgSize
      		}
    	}
  	}
}
```

## ResultStage ##



## Stage 生成 Task ##

当DAGScheduler将RDD划分为Stage之后，会将Stage生成Task，提交给TaskScheduler。

首先获取当前stage需要计算的分区，然后为每个分区生成Task。

```scala
private def submitMissingTasks(stage: Stage, jobId: Int) {
    val partitionsToCompute: Seq[Int] = stage.findMissingPartitions()
    val taskBinaryBytes: Array[Byte] = stage match {
        case stage: ShuffleMapStage =>
        	JavaUtils.bufferToArray(
                closureSerializer.serialize((stage.rdd, stage.shuffleDep): AnyRef))
        case stage: ResultStage =>
        	JavaUtils.bufferToArray(closureSerializer.serialize((stage.rdd, stage.func): AnyRef))
    }
    
    taskBinary = sc.broadcast(taskBinaryBytes)
    
    val tasks: Seq[Task[_]] = try {
        val serializedTaskMetrics = closureSerializer.serialize(stage.latestInfo.taskMetrics).array()
        stage match {
            case stage: ShuffleMapStage =>
            	partitionsToCompute.map { id =>
                    val part = stage.rdd.partitions(id)
                    stage.pendingPartitions += id
                    new ShuffleMapTask(stage.id, stage.latestInfo.attemptId,
                                       taskBinary, part, locs, properties, serializedTaskMetrics, Option(jobId),
                                       Option(sc.applicationId), sc.applicationAttemptId)
                }
            case stage: ResultStage =>
            	partitionsToCompute.map { id =>
                    val p: Int = stage.partitions(id)
                    val part = stage.rdd.partitions(p)
                    val locs = taskIdToLocations(id)
                    new ResultTask(stage.id, stage.latestInfo.attemptId,
                         taskBinary, part, locs, id, properties, serializedTaskMetrics,
              			Option(jobId), Option(sc.applicationId), sc.applicationAttemptId)
                }
        }
    }
    
    taskScheduler.submitTasks(new TaskSet(
        tasks.toArray, stage.id, stage.latestInfo.attemptId, jobId, properties))
        
                    
```