# Kafka Group Coordinator 原理 #



## DelayOperation 原理 ##

DelayOperation有两个回调方法，子类需要实现：

* onComplete，任务完成时的函数

* onExpiration，任务过期的函数

客户在时间过期前，会调用tryComplete方法，检查是否达到完成条件，如果达到，则调用forceComplete执行完成回调。

下面举个例子，这里使用MyDelayOperation实现一个延迟任务。比如我们在某个餐厅排队吃饭，如果在半个小时可以等到，那么我们就去吃饭。否则我们改去别的餐厅吃。

```scala
class MyDelayOperation extends DelayedOperation(is_my_turn: Boolean) {
    
    val logger = Logger(LoggerFactory.getLogger(loggerName))
    
    def onComplete() = {
        logger.info("we are eating meal !")
    }
    
    def onExpiration() = {
        logger.info("we are going to other place !")
    }
    
    def tryComplete(): Boolean = {
        if (is_my_turn) {
            return forceComplete()
        }
        return false
    }
    
}
```





GroupCoordinator在进入rebalance状态后，不会立刻就返回响应。而是等待一段时间，尽可能的等待更多的consumer申请加入，这样就可以大大避免了，连续的consumer加入请求引起的多次重平衡。