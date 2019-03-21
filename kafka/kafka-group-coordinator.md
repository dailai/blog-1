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
| assignment         | 字节数组 | 分配结果                     |



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
            // 如果是新加入的成员
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
              // 如果是旧有成员，并且是follower角色，而且与上次请求一样，
              // 那么则直接返回与上次相同的响应
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



上面的处理主要涉及到了两个方法，addMemberAndRebalance方法处理新成员加入，updateMemberAndRebalance方法处理旧有成员加入。两个方法都很简单，只是新建或修改成员的元数据，然后调用maybePrepareRebalance方法，做一些rebalance之前的准备操作。

```scala
private def addMemberAndRebalance(rebalanceTimeoutMs: Int,
                                  sessionTimeoutMs: Int,
                                  clientId: String,
                                  clientHost: String,
                                  protocolType: String,
                                  protocols: List[(String, Array[Byte])],
                                  group: GroupMetadata,
                                  callback: JoinCallback) = {
  // 这里为新成员分配 id
  val memberId = clientId + "-" + group.generateMemberIdSuffix
  // 新建成员的元数据
  val member = new MemberMetadata(memberId, group.groupId, clientId, clientHost, rebalanceTimeoutMs,
    sessionTimeoutMs, protocolType, protocols)
  // 注意到awaitingJoinCallback这个属性，当处理JoinRequest完成时，会调用这个回调
  // awaitingJoinCallback会将请求结果发送给客户端
  member.awaitingJoinCallback = callback
  // 如果group状态为PreparingRebalance，并且该group为新建的，
  // 设置newMemberAdded为true，在后面延迟rebalance有用到
  if (group.is(PreparingRebalance) && group.generationId == 0)
    group.newMemberAdded = true
  // 添加成员到组
  group.add(member)
  // 调用maybePrepareRebalance方法，执行rebalance前的准备操作
  maybePrepareRebalance(group)
  member
}

private def updateMemberAndRebalance(group: GroupMetadata,
                                     member: MemberMetadata,
                                     protocols: List[(String, Array[Byte])],
                                     callback: JoinCallback) {
  member.supportedProtocols = protocols
  // 设置回调
  member.awaitingJoinCallback = callback
  // 调用maybePrepareRebalance方法，执行rebalance前的准备操作
  maybePrepareRebalance(group)
}
```



maybePrepareRebalance方法，会判断group的状态，检查是否可以执行准备操作。

```scala
private def maybePrepareRebalance(group: GroupMetadata) {
  // 这里使用了锁，防止线程竞争
  group.inLock {
    // 判断group是否能执行Rebalance操作，它是根据GroupMetadata的状态图，判断是否能转到PrepareRebalance状态
    // 比如如果GroupMetadata现在的状态是PrepareRebalance，那么就不能执行Rebalance操作
    if (group.canRebalance)
      prepareRebalance(group)
  }
}
```

 prepareRebalance方法会有点复杂，它涉及到了Kafka的延迟操作。这里GroupCoordinator并不会立刻返回响应，而是等待一段时间，尽可能的等待更多的consumer申请加入，这样就可以大大避免了，连续的consumer加入请求引起的多次重平衡。这里简单介绍下：

```scala
private def prepareRebalance(group: GroupMetadata) {
  // if any members are awaiting sync, cancel their request and have them rejoin
  if (group.is(CompletingRebalance))
    resetAndPropagateAssignmentError(group, Errors.REBALANCE_IN_PROGRESS)
  
  val delayedRebalance = if (group.is(Empty))
    // group的状态为Empty，表示这是第一个consumer加入
    // InitialDelayedJoin表示第一个consumer加入，然后它会等待一会儿，接收别的consumer的加入请求
    new InitialDelayedJoin(this,
      joinPurgatory,
      group,
      groupConfig.groupInitialRebalanceDelayMs,
      groupConfig.groupInitialRebalanceDelayMs,
      max(group.rebalanceTimeoutMs - groupConfig.groupInitialRebalanceDelayMs, 0))
  else
    // 如果group的状态不是Empty，那么使用DelayedJoin延迟操作
    new DelayedJoin(this, group, group.rebalanceTimeoutMs)

  // 设置group的状态为PreparingRebalance
  group.transitionTo(PreparingRebalance)

  // 提交延迟操作
  val groupKey = GroupKey(group.groupId)
  joinPurgatory.tryCompleteElseWatch(delayedRebalance, Seq(groupKey))
}
```

上面涉及到两个延迟操作，InitialDelayedJoin和DelayedJoin。

DelayedJoin继承DelayedOperation类，实现了延迟操作。当任务完成时，会调用GroupCoordinator的onCompleteJoin方法。同时它也会不断调用GroupCoordinator的tryCompleteJoin，如果旧有成员都已经加入，那么就提前完成响应。

```scala
private[group] class DelayedJoin(coordinator: GroupCoordinator,
                                 group: GroupMetadata,
                                 rebalanceTimeout: Long) extends DelayedOperation(rebalanceTimeout, Some(group.lock)) {

  override def tryComplete(): Boolean = coordinator.tryCompleteJoin(group, forceComplete _)
  override def onExpiration() = coordinator.onExpireJoin()
  override def onComplete() = coordinator.onCompleteJoin(group)
}
```



InitialDelayedJoin继承DelayedJoin，它们之间主要的区别是，InitialDelayedJoin只有任务过期才会执行，它不会提前完成。InitialDelayedJoin还有可能多次延迟，只要总的延迟时间不超过指定值即可。

delayMs 表示此次操作的延迟时间

configuredRebalanceDelay 表示每次操作的最大延迟时间

remainingMs 表示剩余可以延迟的剩余空间

```scala
private[group] class InitialDelayedJoin(coordinator: GroupCoordinator,
                                        purgatory: DelayedOperationPurgatory[DelayedJoin],
                                        group: GroupMetadata,
                                        configuredRebalanceDelay: Int,
                                        delayMs: Int,
                                        remainingMs: Int) extends DelayedJoin(coordinator, group, delayMs) {
  // 永远返回false，表示不可能提前完成
  override def tryComplete(): Boolean = false

  override def onComplete(): Unit = {
    group.inLock  {
      // 如果有新增用户，并且还有剩余时间，那么会推迟
      if (group.newMemberAdded && remainingMs != 0) {
        group.newMemberAdded = false
        // 计算新的延迟时间
        val delay = min(configuredRebalanceDelay, remainingMs)
        // 计算新的剩余时间
        val remaining = max(remainingMs - delayMs, 0)
        // 添加新的延迟任务
        purgatory.tryCompleteElseWatch(new InitialDelayedJoin(coordinator,
          purgatory,
          group,
          configuredRebalanceDelay,
          delay,
          remaining
        ), Seq(GroupKey(group.groupId)))
      } else
        // 执行DelayedJoin的onComplete方法，完成响应
        super.onComplete()
    }
  }

}
```





接下来看看GroupCoordinator的tryCompleteJoin方法。tryCompleteJoin会判断旧有的成员是否全部重新加入，如果满足，那么就提前执行Rebalance操作。

```scala
def tryCompleteJoin(group: GroupMetadata, forceComplete: () => Boolean) = {
  group.inLock {
    // 检测是否还有未加入的旧有成员
    if (group.notYetRejoinedMembers.isEmpty)
      // 如果旧有成员全部已经请求重新加入，那么调用了forceComplete，执行完成函数onComplete
      forceComplete()
    else false
  }
}
```

group判断成员是否加入，是判断成员的awaitingJoinCallback是否为空。因为awaitingJoinCallback在成员发起加入请求后，group才会设置awaitingJoinCallback属性。如果awaitingJoinCallback为空，那么表示旧有的成员还未加入。



```scala
def onCompleteJoin(group: GroupMetadata) {
  group.inLock {
    // 这里有可能是因为超时，才执行的。所以不能保证所有的旧有成员都已经重新申请加入，需要将这些迟迟没有加入的成员，删除掉
    group.notYetRejoinedMembers.foreach { failedMember =>
      group.remove(failedMember.memberId)
      // TODO: cut the socket connection to the client
    }

    if (!group.is(Dead)) {
      // 更新group的版本号，并且更新其状态
      // 如果group已经没有成员，那么更新状态为Empty
      // 否则更新状态为CompletingRebalance
      group.initNextGeneration()
      if (group.is(Empty)) {
        // 更新group数据
        groupManager.storeGroup(group, Map.empty, error => {
          if (error != Errors.NONE) {
            // we failed to write the empty group metadata. If the broker fails before another rebalance,
            // the previous generation written to the log will become active again (and most likely timeout).
            // This should be safe since there are no active members in an empty generation, so we just warn.
            warn(s"Failed to write empty metadata for group ${group.groupId}: ${error.message}")
          }
        })
      } else {
        
        // 生成响应，并且调用每个成员的awaitingJoinCallback回调，发送响应
        for (member <- group.allMemberMetadata) {
          assert(member.awaitingJoinCallback != null)
          // 生成响应
          val joinResult = JoinGroupResult(
            members = if (group.isLeader(member.memberId)) {
              // 如果是leader角色，需要将组的所有成员信息发送给它
              group.currentMemberMetadata
            } else {
              Map.empty
            },
            memberId = member.memberId,              // 成员id
            generationId = group.generationId,       // group数据版本号
            subProtocol = group.protocolOrNull,      // group协议
            leaderId = group.leaderOrNull,           // 
            error = Errors.NONE)

          member.awaitingJoinCallback(joinResult)    // 调用awaitingJoinCallback
          member.awaitingJoinCallback = null         // 发送响应后，将awaitingJoinCallback设置为空
          completeAndScheduleNextHeartbeatExpiration(group, member)   // 设置心跳
        }
      }
    }
  }
}
```





## 处理获取分配结果请求 ##

GroupCoordinator的handleSyncGroup方法负责处理分配结果的请求，这里的逻辑比较简单，只是简单的接收leader角色传过来的分配结果，然后将分配结果发送给对应的组成员。

```scala
private def doSyncGroup(group: GroupMetadata,
                        generationId: Int,
                        memberId: String,
                        groupAssignment: Map[String, Array[Byte]],   // key为成员id，value为分配结果
                        responseCallback: SyncCallback) {
  group.inLock {
    if (!group.has(memberId)) {
      // 检查是否有该成员
      responseCallback(Array.empty, Errors.UNKNOWN_MEMBER_ID)
    } else if (generationId != group.generationId) {
      // 检查版本号是否一致
      responseCallback(Array.empty, Errors.ILLEGAL_GENERATION)
    } else {
      // 判断group的状态
      group.currentState match {
        case Empty | Dead =>
          responseCallback(Array.empty, Errors.UNKNOWN_MEMBER_ID)

        case PreparingRebalance =>
          responseCallback(Array.empty, Errors.REBALANCE_IN_PROGRESS)

        case CompletingRebalance =>
          // 设置该成员的awaitingSyncCallback函数，用来发送响应的
          group.get(memberId).awaitingSyncCallback = responseCallback

          // 这里只处理来自leader角色的请求。这里会保存分配结果，而且为成员发送分配结果
          if (group.isLeader(memberId)) {
            // fill any missing members with an empty assignment
            val missing = group.allMembers -- groupAssignment.keySet
            val assignment = groupAssignment ++ missing.map(_ -> Array.empty[Byte]).toMap

            groupManager.storeGroup(group, assignment, (error: Errors) => {
              group.inLock {
                // another member may have joined the group while we were awaiting this callback,
                // so we must ensure we are still in the CompletingRebalance state and the same generation
                // when it gets invoked. if we have transitioned to another state, then do nothing
                if (group.is(CompletingRebalance) && generationId == group.generationId) {
                  if (error != Errors.NONE) {
                    resetAndPropagateAssignmentError(group, error)
                    maybePrepareRebalance(group)
                  } else {
                    // 保存分配结果，并且返回响应
                    setAndPropagateAssignment(group, assignment)
                    // 更新状态为Stable
                    group.transitionTo(Stable)
                  }
                }
              }
            })
          }

        case Stable =>
          // 有些follower角色成员的请求，可能在leader角色之后，这里状态已经转为Stable了。
          // 所以只是返回该成员的分配结果
          val memberMetadata = group.get(memberId)
          responseCallback(memberMetadata.assignment, Errors.NONE)
          // 设置心跳时间
          completeAndScheduleNextHeartbeatExpiration(group, group.get(memberId))
      }
    }
  }
}
```



注意到上面的setAndPropagateAssignment方法，它会执行每个成员的awaitingSyncCallback回调，将分配结果发送给成员。

```scala
private def setAndPropagateAssignment(group: GroupMetadata, assignment: Map[String, Array[Byte]]) {
  assert(group.is(CompletingRebalance))
  // 为每个成员设置分配结果
  group.allMemberMetadata.foreach(member => member.assignment = assignment(member.memberId))
  // 为发送SyncGroup请求的成员，发送响应
  propagateAssignment(group, Errors.NONE)
}

private def propagateAssignment(group: GroupMetadata, error: Errors) {
    for (member <- group.allMemberMetadata) {
        // 只有发送SyncGroup请求的成员，它的awaitingSyncCallback才不为空
        if (member.awaitingSyncCallback != null) {
            // 执行awaitingSyncCallback函数
            member.awaitingSyncCallback(member.assignment, error)
            // 执行完设置awaitingSyncCallback为空
            member.awaitingSyncCallback = null

            // reset the session timeout for members after propagating the member's assignment.
            // This is because if any member's session expired while we were still awaiting either
            // the leader sync group or the storage callback, its expiration will be ignored and no
            // future heartbeat expectations will not be scheduled.
            completeAndScheduleNextHeartbeatExpiration(group, member)
        }
    }
}
```





