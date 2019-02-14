# Spark Streaming WAL 原理 #



spark streaming 为了实现了高可用，引入了 wal （write ahead log）。当receiver端 存储数据的时候，如果配置了wal选项，会先将数据以wal的格式存储起来。



要支持wal，必须指定 checkpoint 的目录

wal的读写由WriteAheadLog类负责，它有一个子类FileBasedWriteAheadLog，实现了以文件的方式存储。还有一个子类BatchedWriteAheadLog，基于FileBasedWriteAheadLog实现了批次的存储。



目录格式如下：

```shell
checkpointDir/
├── receivedData
│   ├── streamId0
|   |   |── log-starttime0-endtime0
|   |   |── log-starttime1-endtime1
|   |   |── log-starttime1-endtime1
│   ├── streamId1
│   └── streamId2
```

wal的根目录是checkpoint 目录下的receivedData目录。每个receiver都有独立的目录，目录名为它的 id 号。FileBasedWriteAheadLog还会按照时间间隔存储wal，文件名称包括起始时间和结束时间。 



FileBasedWriteAheadLogWriter负责wal的写入。它会将数据写入到hdfs里

```scala
class FileBasedWriteAheadLogWriter(path: String, hadoopConf: Configuration)
  extends Closeable {
  // 根据配置，返回hdfs的客户端或者本地文件的客户端，并且创建文件
  private lazy val stream = HdfsUtils.getOutputStream(path, hadoopConf)

  // 获取当前文件的偏移量
  private var nextOffset = stream.getPos()
  private var closed = false

  /** Write the bytebuffer to the log file */
  def write(data: ByteBuffer): FileBasedWriteAheadLogSegment = synchronized {
    data.rewind() // 准备读
    val lengthToWrite = data.remaining()
    // 记录本次数据的位置信息，WAL文件路径，起始位置，数据长度
    val segment = new FileBasedWriteAheadLogSegment(path, nextOffset, lengthToWrite)
    // 写入数据长度
    stream.writeInt(lengthToWrite)
    // 将bytebuffer的数据写入到outputstream
    Utils.writeByteBuffer(data, stream: OutputStream)
    // 刷新缓存
    flush()
    // 更新当前文件的偏移量
    nextOffset = stream.getPos()
    // 返回此次数据的信息
    segment
  }

  private def flush() {
    stream.hflush()
    // Useful for local file system where hflush/sync does not work (HADOOP-7844)
    stream.getWrappedStream.flush()
  }
}
```



FileBasedWriteAheadLogRandomReader负责wal的读取



##  WAL 创建 ##

```scala
private[streaming] class FileBasedWriteAheadLog {
  
  // 记录当前WAL文件的路径
  private var currentLogPath: Option[String] = None
  // 记录当前WAL writer
  private var currentLogWriter: FileBasedWriteAheadLogWriter = null
  // 当前WAL文件的开始时间
  private var currentLogWriterStartTime: Long = -1L
  // 当前WAL文件的结束时间
  private var currentLogWriterStopTime: Long = -1L
  // 记录完成的WAL文件信息
  private val pastLogs = new ArrayBuffer[LogInfo]
  
  def write(byteBuffer: ByteBuffer, time: Long): FileBasedWriteAheadLogSegment = synchronized {
    var fileSegment: FileBasedWriteAheadLogSegment = null
    var failures = 0
    var lastException: Exception = null
    var succeeded = false
    // 尝试最多maxFailures次数
    while (!succeeded && failures < maxFailures) {
      try {
        // 首先调用getLogWriter获取writer，
        // 然后通过writer写入数据
        fileSegment = getLogWriter(time).write(byteBuffer)
        if (closeFileAfterWrite) {
          resetWriter()
        }
        succeeded = true
      } catch {
        case ex: Exception =>
          lastException = ex
          logWarning("Failed to write to write ahead log")
          resetWriter()
          failures += 1
      }
    }
    if (fileSegment == null) {
      logError(s"Failed to write to write ahead log after $failures failures")
      throw lastException
    }
    fileSegment
  }
  
  private def getLogWriter(currentTime: Long): FileBasedWriteAheadLogWriter = synchronized {
    // 每个WAL文件都有对应的时间范围，如果超过了，则需要创建新的WAL文件
    if (currentLogWriter == null || currentTime > currentLogWriterStopTime) {
      // 关闭当前writer
      resetWriter()
      // 添加当前WAL的信息到currentLogPath列表，
      // 相关信息包括起始时间，结束时间，文件路径
      currentLogPath.foreach {
        pastLogs += LogInfo(currentLogWriterStartTime, currentLogWriterStopTime, _)
      }
      // 更新currentLogWriterStartTime为新的WAL文件的开始时间
      currentLogWriterStartTime = currentTime
      // 更新currentLogWriterStopTime为新的WAL文件的结束时间
      currentLogWriterStopTime = currentTime + (rollingIntervalSecs * 1000)
      // 生成新的WAL文件的路径
      val newLogPath = new Path(logDirectory,
        timeToLogFile(currentLogWriterStartTime, currentLogWriterStopTime))
      // 更新currentLogPath为新的WAL文件的路径
      currentLogPath = Some(newLogPath.toString)
      // 更新currentLogWriter为新的writer
      currentLogWriter = new FileBasedWriteAheadLogWriter(currentLogPath.get, hadoopConf)
    }
    currentLogWriter
  }  
```



## WAL 读取 ##





## WAL 删除 ##

spark streaming处理的是流数据，所以它不可能会将所有的数据都保存下来。对于处理过的数据，它会定期删除掉。

FileBasedWriteAheadLog提供了clean接口，来处理过期的数据

```scala
private[streaming] class FileBasedWriteAheadLog {
  // WAL文件信息列表
  private val pastLogs = new ArrayBuffer[LogInfo]
  
  def clean(threshTime: Long, waitForCompletion: Boolean): Unit = {
    // 找到结束时间小于threshTime的WAL文件
    val oldLogFiles = synchronized {
      val expiredLogs = pastLogs.filter { _.endTime < threshTime }
      pastLogs --= expiredLogs
      expiredLogs
    }
      
    def deleteFile(walInfo: LogInfo): Unit = {
      try {
        // 获取WAL文件的路径
        val path = new Path(walInfo.path)
        // 获取FileSystem
        val fs = HdfsUtils.getFileSystemForPath(path, hadoopConf)
        // 删除WAL文件
        fs.delete(path, true)
      } catch {
        case ex: Exception =>
          logWarning(s"Error clearing write ahead log file $walInfo", ex)
      }
    }
    
    // 遍历需要删除的WAL文件列表，调用deleteFile方法删除
    oldLogFiles.foreach { logInfo =>
      if (!executionContext.isShutdown) {
        try {
          // 使用线程池删除文件
          val f = Future { deleteFile(logInfo) }(executionContext)
          if (waitForCompletion) {
            import scala.concurrent.duration._
            // scalastyle:off awaitready
            Await.ready(f, 1 second)
            // scalastyle:on awaitready
          }
        } catch {
          case e: RejectedExecutionException =>
            logWarning("Execution context shutdown before deleting old WriteAheadLogs. " +
              "This would not affect recovery correctness.", e)
        }
      }
    }
  }
}
```



## BatchedWriteAheadLog ##

BatchedWriteAheadLog只运行在driver端，而且需要spark配置中指定spark.streaming.driver.writeAheadLog.allowBatching选项为true。

BatchedWriteAheadLog将每次请求写入的数据，先缓存到一个队列。

```scala
class BatchedWriteAheadLog(val wrappedLog: WriteAheadLog, conf: SparkConf) {
  // WAL数据队列
  private val walWriteQueue = new LinkedBlockingQueue[Record]()
  
  override def write(byteBuffer: ByteBuffer, time: Long): WriteAheadLogRecordHandle = {
    val promise = Promise[WriteAheadLogRecordHandle]()
    val putSuccessfully = synchronized {
      if (active) {
        // 实例化Record，并且添加到walWriteQueue队列里
        walWriteQueue.offer(Record(byteBuffer, time, promise))
        true
      } else {
        false
      }
    }
    ......
  }    
}
```



BatchedWriteAheadLog还有一个后台线程，会一直从队列中获取数据，然后封装成一个批次保存起来。

```scala
private[util] class BatchedWriteAheadLog(val wrappedLog: WriteAheadLog, conf: SparkConf) {
  
  // WAL数据队列
  private val walWriteQueue = new LinkedBlockingQueue[Record]()
  // 数据缓存，保存即将保存的batch数据
  private val buffer = new ArrayBuffer[Record]()
  // batch write 线程
  private val batchedWriterThread = startBatchedWriterThread()
    
  private def startBatchedWriterThread(): Thread = {
    // 循环的调用flushRecords方法
    val thread = new Thread(new Runnable {
      override def run(): Unit = {
        while (active) {
          flushRecords()
        }
      }
    }, "BatchedWriteAheadLog Writer")
    thread.setDaemon(true)
    thread.start()
    thread
  }

  /** Write all the records in the buffer to the write ahead log. */
  private def flushRecords(): Unit = {
    try {
      // 这里调用take会阻塞，一直到队列中有数据
      buffer += walWriteQueue.take()
      // 调用drainTo能够高效率的导出数据
      val numBatched = walWriteQueue.drainTo(buffer.asJava) + 1
    } catch {
      case _: InterruptedException =>
        logWarning("BatchedWriteAheadLog Writer queue interrupted.")
    }
    try {
      var segment: WriteAheadLogRecordHandle = null
      if (buffer.length > 0) {
        // 依照时间排序
        val sortedByTime = buffer.sortBy(_.time)
        // 取WAL列表中最后的时间，作为batch的结束时间
        val time = sortedByTime.last.time
        // wrappedLog是FileBasedWriteAheadLog类型，在初始化的时候指定
        // aggregate方法将这一批次的数据，汇合到一个ByteBuffer里，
        // 然后通过FileBasedWriteAheadLog写入数据
        segment = wrappedLog.write(aggregate(sortedByTime), time)
      }
      buffer.foreach(_.promise.success(segment))
    } catch {
      ......
    } finally {
      // 清空buffer列表  
      buffer.clear()
    }
  }
}
```









