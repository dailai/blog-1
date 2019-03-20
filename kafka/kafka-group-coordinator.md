# Kafka Group Coordinator 原理 #



## DelayOperation 原理 ##

DelayOperation有两个回调方法，子类需要实现：

* onComplete，任务完成时的函数

* onExpiration，任务过期的函数

客户在时间过期前，会调用tryComplete方法，检查是否达到完成条件，如果达到，则调用forceComplete执行onComplete回调。

如果等待任务过期了，则会调用forceComplete执行onComplete回调，并且还会调用onExpiration的回调。

下面举个例子，这里使用MyDelayOperation实现一个延迟任务。比如我们在某个餐厅排队吃饭，最多排队半个小时。

```scala
class MyDelayOperation extends DelayedOperation(is_my_turn: Boolean) {
    
    val logger = Logger(LoggerFactory.getLogger(loggerName))
    
    def onComplete() = {
        logger.info("we are eating meal !")
    }
    
    def onExpiration() = {
        logger.info("we hava waited for 30 minutes !")
    }
    
    def tryComplete(): Boolean = {
        if (is_my_turn) {
            logger.info("we hava waited for less than 30 minutes !")
            return forceComplete()
        }
        return false
    }
}
```





GroupCoordinator在进入rebalance状态后，不会立刻就返回响应。而是等待一段时间，尽可能的等待更多的consumer申请加入，这样就可以大大避免了，连续的consumer加入请求引起的多次重平衡。





## 处理寻找GroupCoordinator地址请求 ##

在介绍这之前，需要先了解下Kafka是如何存储consumer group的消费位置。Kafka内部保存了一个名称为__consumer_offsets的 topic，里面存储着每个consumer group对于各个topic partition的消费offset。我们知道topic是分为多个partition，一个consumer group的消费位置只存在一个partition里。而这个partition的leader副本所在的主机，就是负责该consumer group的GroupCoordinator的地址。

Kafka的所有请求都是在KafkaApis类里定义怎么处理的

```scala
class KafkaApis(...) {

  def handleFindCoordinatorRequest(request: RequestChannel.Request) {
      val findCoordinatorRequest = request.body[FindCoordinatorRequest]
      .... // 认证和校检
      val (partition, topicMetadata) = findCoordinatorRequest.coordinatorType match {
        case FindCoordinatorRequest.CoordinatorType.GROUP =>
          // 计算该consumer group被分配到哪个partition
          val partition = groupCoordinator.partitionFor(findCoordinatorRequest.coordinatorKey)
          // 创建内部topic(即保存消费位置的topic)，并且返回该topic的元数据
          val metadata = getOrCreateInternalTopic(GROUP_METADATA_TOPIC_NAME, request.context.listenerName)
          (partition, metadata)
        case FindCoordinatorRequest.CoordinatorType.TRANSACTION =>
          ... // 处理事务
        case _ =>
          throw new InvalidRequestException("Unknown coordinator type in FindCoordinator request")
      }

      def createResponse(requestThrottleMs: Int): AbstractResponse = {
        val responseBody = if (topicMetadata.error != Errors.NONE) {
          new FindCoordinatorResponse(requestThrottleMs, Errors.COORDINATOR_NOT_AVAILABLE, Node.noNode)
        } else {
          // 遍历该topic的partition，找到那个partition的leader副本 
          val coordinatorEndpoint = topicMetadata.partitionMetadata.asScala
            .find(_.partition == partition)
            .map(_.leader)
            .flatMap(p => Option(p))
          // 返回结果
          coordinatorEndpoint match {
            case Some(endpoint) if !endpoint.isEmpty =>
              new FindCoordinatorResponse(requestThrottleMs, Errors.NONE, endpoint)
            case _ =>
              new FindCoordinatorResponse(requestThrottleMs, Errors.COORDINATOR_NOT_AVAILABLE, Node.noNode)
          }
        }
        responseBody
      }
      
      // 发送响应
      sendResponseMaybeThrottle(request, createResponse)
    }
  }
    
}
```



Kafka是计算consumer group名称的哈希值，来确定它被分配到哪个partition。算法如下：

```java
class GroupMetadataManager(...) {
  // 获取该topic的分区数
  private val groupMetadataTopicPartitionCount = getGroupMetadataTopicPartitionCount
  // 调用zkClient获取分区数
  private def getGroupMetadataTopicPartitionCount: Int = {
    zkClient.getTopicPartitionCount(Topic.GROUP_METADATA_TOPIC_NAME).getOrElse(config.offsetsTopicNumPartitions)
  }
  // 计算该groupId的哈希值，取余
  def partitionFor(groupId: String): Int = Utils.abs(groupId.hashCode) % groupMetadataTopicPartitionCount
}

```



## Group 元数据 ##

GroupCoordinator为每个consumer group保存元数据，由GroupMetadata类表示。GroupMetadata类保存了组里每个成员的元数据，由MemberMetadata类表示。

MemberMetadata描述一个consumer的信息，它主要包含以下字段：

| 字段名             | 字段类型 | 字段含义                     |
| ------------------ | -------- | ---------------------------- |
| memberId           | 字符串   | 成员 id                      |
| groupId            | 字符串   | 组名称                       |
| rebalanceTimeoutMs | 整数     | 等待rebalance的最大时间      |
| sessionTimeoutMs   | 整数     |                              |
| supportedProtocols | 列表     | 该consumer支持的分配算法列表 |



GroupMetadata描述了一个consumer group的信息，它主要包含以下字段：

| 字段名       | 字段类型            | 字段含义            |
| ------------ | ------------------- | ------------------- |
| generationId | 整数                | 版本号              |
| members      | MemberMetadata 列表 | 组成员的元数据列表  |
| leaderId     | 整数                | leader角色的成员 id |



GroupMetadata还负责状态机的维护，如下图所示：









## 处理加入请求 ##

处理加入请求的过程比较复杂。首先我们先梳理一下简单的流程，沿着GroupMetadata的状态，按照Empty --> PrepareRebalance --> CompletingRebalance --> Stable的方向。

对于加入请求的处理，KafkaApis会调用GroupCoordinator的handleJoinGroup方法处理。它会首先检测请求参数和检测，然后调用doJoinGroup方法处理请求。

```scala
class GroupCoordinator(） {
    
  def handleJoinGroup(groupId: String,
                      memberId: String,
                      clientId: String,
                      clientHost: String,
                      rebalanceTimeoutMs: Int,
                      sessionTimeoutMs: Int,
                      protocolType: String,
                      protocols: List[(String, Array[Byte])],
                      responseCallback: JoinCallback): Unit = {
    // 检查group的状态
    validateGroupStatus(groupId, ApiKeys.JOIN_GROUP).foreach { error =>
      responseCallback(joinError(memberId, error))
      return
    }
    // consumer的sessionTimeoutMs时间设置，必须在group组的设定区间内
    if (sessionTimeoutMs < groupConfig.groupMinSessionTimeoutMs ||
      sessionTimeoutMs > groupConfig.groupMaxSessionTimeoutMs) {
      responseCallback(joinError(memberId, Errors.INVALID_SESSION_TIMEOUT))
    } else {
      // 查看该group是否以前存在
      groupManager.getGroup(groupId) match {
        case None =>
          // 如果是新的group，那么所有请求中的memberId必须为UNKNOWN_MEMBER_ID
          if (memberId != JoinGroupRequest.UNKNOWN_MEMBER_ID) {
            responseCallback(joinError(memberId, Errors.UNKNOWN_MEMBER_ID))
          } else {
            // 添加group，并且新建group的元数据
            val group = groupManager.addGroup(new GroupMetadata(groupId, initialState = Empty))
            // 调用doJoinGroup函数，处理请求
            doJoinGroup(group, memberId, clientId, clientHost, rebalanceTimeoutMs, sessionTimeoutMs, protocolType, protocols, responseCallback)
          }

        case Some(group) =>
          // 如果该group之前存在，那么直接调用doJoinGroup函数
          doJoinGroup(group, memberId, clientId, clientHost, rebalanceTimeoutMs, sessionTimeoutMs, protocolType, protocols, responseCallback)
      }
    }
  }  
}
```



doJoinGroup方法会依据GroupMetadata的状态，做不同的处理。

```scala
private def doJoinGroup(group: GroupMetadata,
                        memberId: String,
                        clientId: String,
                        clientHost: String,
                        rebalanceTimeoutMs: Int,
                        sessionTimeoutMs: Int,
                        protocolType: String,
                        protocols: List[(String, Array[Byte])],
                        responseCallback: JoinCallback) {
  group.inLock {
    if (!group.is(Empty) && (!group.protocolType.contains(protocolType) || !group.supportsProtocols(protocols.map(_._1).toSet))) {
      // 检查是否支持协议类型和分配算法
      responseCallback(joinError(memberId, Errors.INCONSISTENT_GROUP_PROTOCOL))
    } else if (group.is(Empty) && (protocols.isEmpty || protocolType.isEmpty)) {
      // 检查group是否新建并且还未指定协议或分配算法
      responseCallback(joinError(memberId, Errors.INCONSISTENT_GROUP_PROTOCOL))
    } else if (memberId != JoinGroupRequest.UNKNOWN_MEMBER_ID && !group.has(memberId)) {
      // 检查该成员是否在group里
      responseCallback(joinError(memberId, Errors.UNKNOWN_MEMBER_ID))
    } else {
      group.currentState match {
        case Dead =>
          // if the group is marked as dead, it means some other thread has just removed the group
          // from the coordinator metadata; this is likely that the group has migrated to some other
          // coordinator OR the group is in a transient unstable phase. Let the member retry
          // joining without the specified member id,
          responseCallback(joinError(memberId, Errors.UNKNOWN_MEMBER_ID))
        case PreparingRebalance =>
          // 如果是新成员加入，则调用addMemberAndRebalance方法处理
          if (memberId == JoinGroupRequest.UNKNOWN_MEMBER_ID) {
            addMemberAndRebalance(rebalanceTimeoutMs, sessionTimeoutMs, clientId, clientHost, protocolType,
              protocols, group, responseCallback)
          } else {
            // 如果是旧有成员加入，则调用updateMemberAndRebalance方法处理
            val member = group.get(memberId)
            updateMemberAndRebalance(group, member, protocols, responseCallback)
          }

        case CompletingRebalance =>
          // 如果是新成员加入，则调用addMemberAndRebalance方法处理
          if (memberId == JoinGroupRequest.UNKNOWN_MEMBER_ID) {
            addMemberAndRebalance(rebalanceTimeoutMs, sessionTimeoutMs, clientId, clientHost, protocolType,
              protocols, group, responseCallback)
          } else {
            val member = group.get(memberId)
            if (member.matches(protocols)) {
              // 成员之前已经发送了 JoinGroup请求，但是因为超时等原因，没有收到响应。
              // 这里直接返回响应
              responseCallback(JoinGroupResult(
                members = if (group.isLeader(memberId)) {
                  group.currentMemberMetadata
                } else {
                  Map.empty
                },
                memberId = memberId,
                generationId = group.generationId,
                subProtocol = group.protocolOrNull,
                leaderId = group.leaderOrNull,
                error = Errors.NONE))
            } else {
              // 成员的请求与上次请求不一致，说明是新的请求，需要重新平衡
              updateMemberAndRebalance(group, member, protocols, responseCallback)
            }
          }

        case Empty | Stable =>
          if (memberId == JoinGroupRequest.UNKNOWN_MEMBER_ID) {
            // if the member id is unknown, register the member to the group
            addMemberAndRebalance(rebalanceTimeoutMs, sessionTimeoutMs, clientId, clientHost, protocolType,
              protocols, group, responseCallback)
          } else {
            val member = group.get(memberId)
            if (group.isLeader(memberId) || !member.matches(protocols)) {
              // force a rebalance if a member has changed metadata or if the leader sends JoinGroup.
              // The latter allows the leader to trigger rebalances for changes affecting assignment
              // which do not affect the member metadata (such as topic metadata changes for the consumer)
              updateMemberAndRebalance(group, member, protocols, responseCallback)
            } else {
              // for followers with no actual change to their metadata, just return group information
              // for the current generation which will allow them to issue SyncGroup
              responseCallback(JoinGroupResult(
                members = Map.empty,
                memberId = memberId,
                generationId = group.generationId,
                subProtocol = group.protocolOrNull,
                leaderId = group.leaderOrNull,
                error = Errors.NONE))
            }
          }
      }

      if (group.is(PreparingRebalance))
        joinPurgatory.checkAndComplete(GroupKey(group.groupId))
    }
  }
}
```





 



## 处理获取分配结果请求 ##









