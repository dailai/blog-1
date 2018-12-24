# ShuffleBlockFetcherIterator #

负责读取Block的数据，统一了本地Block和远程Block的接口。它首先会根据Block的位置，分成两种请求。如果是本地Block，那么直接通过BlockManager读取。如果是远端Block，那么会通过ShuffleClient请求远程的数据。



## 请求流程 ##

ShuffleBlockFetcherIterator有两个队列

* ```scala
  fetchRequests = new Queue[FetchRequest]
  ```

* ```scala
  results = new LinkedBlockingQueue[FetchResult]
  ```

请求队列fetchRequests，在initialize方法中，就会生成。

响应队列results，在数据请求完成时，会添加。



## 生成请求 ##

分离远端本地请求。对于远端请求，会有一个请求的速度限制。所有正在请求中的Block的数据大小，不能超过maxBytesInFlight。这个参数在spark配置spark.reducer.maxReqsInFlight指定。这里将要请求的Block，切分成FetchRquest。每个FetchRquest包含了多个Block。切分的原理是，每个FetchRquest包含的Block的大小之和，刚好超过maxBytesInFlight / 5。这里还要注意一点，每个FetchRquest的Block都属于同一个BlockManager，也就是在同一个节点上。

```scala
  blocksByAddress: Seq[(BlockManagerId, Seq[(BlockId, Long)])]

  private[this] def splitLocalRemoteBlocks(): ArrayBuffer[FetchRequest] = {
      // 每个FetchRquest的大小的限值
      val targetRequestSize = math.max(maxBytesInFlight / 5, 1L)
      for ((address, blockInfos) <- blocksByAddress) {
          // 本地Block
          if (address.executorId == blockManager.blockManagerId.executorId) {
              // 剔除size等于0的Block
              localBlocks ++= blockInfos.filter(_._2 != 0).map(_._1)
              numBlocksToFetch += localBlocks.size
          } else {
            
              val iterator = blockInfos.iterator
              var curRequestSize = 0L
              var curBlocks = new ArrayBuffer[(BlockId, Long)]
              while (iterator.hasNext) {
                  val (blockId, size) = iterator.next()
                  if (size > 0) {
                      curBlocks += ((blockId, size))
                      remoteBlocks += blockId
                      numBlocksToFetch += 1
                      // 累加Block的size
                      curRequestSize += size
                  } else if (size < 0) {
                      throw new BlockException(blockId, "Negative block size " + size)
                  }
                  // 如果当前请求的大小，大于指定的大小，
                  // 则生成新的FetchRequest
                  if (curRequestSize >= targetRequestSize) {
                      remoteRequests += new FetchRequest(address, curBlocks)
                      curBlocks = new ArrayBuffer[(BlockId, Long)]
                      logDebug(s"Creating fetch request of $curRequestSize at $address")
                      // 更新curRequestSize计数
                      curRequestSize = 0
                  }
              }
              // 将剩余的Block，生成新的请求
              if (curBlocks.nonEmpty) {
                  remoteRequests += new FetchRequest(address, curBlocks)
              }
          }
    }
    remoteRequests
  }
          
```



## 读取Block ##

### 本地读取  ###

```scala
private[this] def fetchLocalBlocks() {
    val iter = localBlocks.iterator
    while (iter.hasNext) {
        val buf = blockManager.getBlockData(blockId)
        buf.retain()
        results.put(new SuccessFetchResult(blockId, blockManager.blockManagerId, 0, buf, false))
    }
}
```

本地Block的请求都会保存在localBlocks数组里。这里可以看到是调用了 blockManager.getBlockData方法获取数据。



## 远程读取 ##

bytesInFlight表示正在请求中Block的数据大小。这里的while条件。表现出了限速的功能。

```scala
private def fetchUpToMaxBytes(): Unit = {
  // Send fetch requests up to maxBytesInFlight
  while (fetchRequests.nonEmpty &&
    (bytesInFlight == 0 ||
      (reqsInFlight + 1 <= maxReqsInFlight &&
        bytesInFlight + fetchRequests.front.size <= maxBytesInFlight))) {
    sendRequest(fetchRequests.dequeue())
  }
}
```

sendRequest会创建BlockFetchingListener回调函数，这个回调函数会传递给shuffleClient.fetchBlocks。

如果Block的数据大于maxReqSizeShuffleToMem，则存储到磁盘里。

```scala
private[this] def sendRequest(req: FetchRequest) {
    
    // 定义BlockFetchingListener
    val blockFetchingListener = new BlockFetchingListener {
        override def onBlockFetchSuccess(blockId: String, buf: ManagedBuffer): Unit = {
            .......
            // 向results队列添加SuccessFetchResult消息
            results.put(new SuccessFetchResult(BlockId(blockId), address, sizeMap(blockId), buf,
              remainingBlocks.isEmpty))
        }
        
        override def onBlockFetchFailure(blockId: String, e: Throwable): Unit = {
            // 向results队列添加FailureFetchResult消息
	        results.put(new FailureFetchResult(BlockId(blockId), address, e))
      }
    }
    
    if (req.size > maxReqSizeShuffleToMem) {
      // 创建临时文件
      val shuffleFiles = blockIds.map { _ =>
        blockManager.diskBlockManager.createTempLocalBlock()._2
      }.toArray
      shuffleFilesSet ++= shuffleFiles
      // 调用fetchBlocks
      shuffleClient.fetchBlocks(address.host, address.port, address.executorId, blockIds.toArray,
        blockFetchingListener, shuffleFiles)
    } else {
      // 调用fetchBlocks
      shuffleClient.fetchBlocks(address.host, address.port, address.executorId, blockIds.toArray,
        blockFetchingListener, null)
    }
}
    
```



## 遍历Block ##

 ShuffleBlockFetcherIterator实现了Iterator[(BlockId, InputStream)]接口。

```scala
  override def hasNext: Boolean = numBlocksProcessed < numBlocksToFetch
```

遍历结束条件为是否已经将Block都已经取完