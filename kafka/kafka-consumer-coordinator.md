# KafkaConsumer  Coordinator 原理 #



```java
public void ensureActiveGroup() {
    // always ensure that the coordinator is ready because we may have been disconnected
    // when sending heartbeats and does not necessarily require us to rejoin the group.
    ensureCoordinatorReady();
    startHeartbeatThreadIfNeeded();
    joinGroupIfNeeded();
}
```

ensureCoordinatorReady负责寻找到Coordinator的位置，然后创建好连接。

发送FindCoordinatorRequest请求，处理响应的回调在FindCoordinatorResponseHandler里。



startHeartbeatThreadIfNeeded负责开启心跳线程



joinGroupIfNeeded加入Kafka的消费组，分为三个阶段

onJoinPrepare，加入之前

发送JoinGroupRequest请求，JoinGroupResponseHandler负责处理响应。

根据响应查看返回的角色，如果是leader则调用onJoinLeader回调。如果是follower则调用onJoinFollower回调。

onJoinLeader函数会执行分区的分配，然后发送SyncGroupRequest请求给coordinator

onJoinFollower函数会发送SyncGroupRequest请求给coordinator



SyncGroupRequest的响应处理回调，在SyncGroupResponseHandler类里



分别介绍下这几种请求和响应的格式

FindCoordinatorRequest有两个字段：

* coordinator_key，表示group id 或者 transaction id
* coordinator_type，表示coordinator_key是哪一种类型

FindCoordinatorResponse包含了group coordinator的地址

* node_id，coordinator服务所在主机的 id 号
* host，主机地址
* port，服务端口号



JoinGroupRequest请求

* group_id
* session_timeout
* rebalance_timeout
* protocol_type
* group_protocols,，protocol列表

group_protocol定义

* protocol_name
* protocol_metadata



JoinGroupResponse响应，Kafka为每个group的consumer都分配了一个 id 号

* throttle_time_ms
* error_code
* generation_id
* group_protocol
* leader_id，这个 consumer group 的 leader 所在的consumer id
* member_id，这consumer的 id 号
* members，member类型列表

member类型

* member_id， consumer 的 id 号
* member_metadata， consumer 的 额外数据



SyncGroupRequest请求

* group_id
* generation_id
* member_id
* group_assignment，sync_group类型列表

sync_group类型

* member_id
* member_assignment

 

SyncGroupResponse响应

* member_assignment







leader会使用PartitionAssignor来完成分区的分配

PartitionAssignor是接口，它有两个内部类，Subscription包含了topic列表和用户数据。Assignment包含了partition列表和用户数据。

```java
public interface PartitionAssignor {
    Map<String, Assignment> assign(Cluster metadata, Map<String, Subscription> subscriptions);
}
```

客户端负责实现分配，并且将结果序列化。服务端仅仅是各个消费者的沟通桥梁，它不负责解析数据。





寻找Coordinator地址

```java
public abstract class AbstractCoordinator implements Closeable {
    
    protected synchronized boolean ensureCoordinatorReady(final long timeoutMs) {
        final long startTimeMs = time.milliseconds();
        long elapsedTime = 0L;
        // 调用coordinatorUnknown方法，检测Coordinator地址是否已经获取了
        while (coordinatorUnknown()) {
            // 发送寻找Coordinator的请求
            final RequestFuture<Void> future = lookupCoordinator();
            // 等待响应，remainingTimeAtLeastZero方法计算等待时长
            client.poll(future, remainingTimeAtLeastZero(timeoutMs, elapsedTime));
            if (!future.isDone()) {
                // 如果超时，还没有收到响应
                break;
            }

            if (future.failed()) {
                // 检测是否可以重试
                if (future.isRetriable()) {
                    elapsedTime = time.milliseconds() - startTimeMs;
                    if (elapsedTime >= timeoutMs) break;
                    // 更新元数据并且等待完成
                    client.awaitMetadataUpdate(remainingTimeAtLeastZero(timeoutMs, elapsedTime));
                    elapsedTime = time.milliseconds() - startTimeMs;
                } else
                    throw future.exception();
            } else if (coordinator != null && client.isUnavailable(coordinator)) {
                // 虽然找到了Coordinator地址，但是连接失败
                markCoordinatorUnknown();
                final long sleepTime = Math.min(retryBackoffMs, remainingTimeAtLeastZero(timeoutMs, elapsedTime));
                time.sleep(sleepTime);
                elapsedTime += sleepTime;
            }
        }
        // 返回是否与Coordinator建立连接
        return !coordinatorUnknown();
    }
}
```



上面调用了lookupCoordinator方法，构建和发送请求



