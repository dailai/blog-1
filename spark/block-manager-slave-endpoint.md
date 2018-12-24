# BlockManagerSlaveEndpoint #

BlockManagerSlaveEndpoint是作为BlockManager对外提供的服务。它是基于spark的Rpc框架。

```scala
class BlockManagerSlaveEndpoint(
    override val rpcEnv: RpcEnv,
    blockManager: BlockManager,
    mapOutputTracker: MapOutputTracker)
  extends ThreadSafeRpcEndpoint with Logging 
```

可以看到BlockManagerSlaveEndpoint是继承ThreadSafeRpcEndpoint， 表明是支持并发处理消息的。

实现Rpc服务端，继承ThreadSafeRpcEndpoint，然后只需覆写receiveAndReply就可以了。它支持下列协议：

* RemoveBlock
* RemoveRdd
* RemoveShuffle
* RemoveBroadcast
* GetBlockStatus
* GetMatchingBlockIds
* TriggerThreadDump
* ReplicateBlock

