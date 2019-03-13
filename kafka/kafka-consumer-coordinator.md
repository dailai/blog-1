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
* group_protocols，protocol列表

group_protocol定义

* protocol_name
* protocol_metadata



JoinGroupResponse响应，Kafka为每个group的consumer都分配了一个 id 号

* throttle_time_ms
* error_code
* generation_id
* group_protocol，分配算法
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

```java
public abstract class AbstractCoordinator implements Closeable {
    
    protected final ConsumerNetworkClient client;
    private RequestFuture<Void> findCoordinatorFuture = null;
    

    protected synchronized RequestFuture<Void> lookupCoordinator() {
        if (findCoordinatorFuture == null) {
            // 找到负载最轻的节点
            Node node = this.client.leastLoadedNode();
            if (node == null) {
                return RequestFuture.noBrokersAvailable();
            } else
                // 发送请求
                findCoordinatorFuture = sendFindCoordinatorRequest(node);
        }
        return findCoordinatorFuture;
    }
    
    private RequestFuture<Void> sendFindCoordinatorRequest(Node node) {
        // 构建请求
        FindCoordinatorRequest.Builder requestBuilder =
                new FindCoordinatorRequest.Builder(FindCoordinatorRequest.CoordinatorType.GROUP, this.groupId);
        // 这里先调用了client的send方法，返回RequestFuture<ClientResponse>类型
        // 然后调用了compose方法，转换为RequestFuture<Void>类型
        return client.send(node, requestBuilder)
                     .compose(new FindCoordinatorResponseHandler());
    } 
}
```

接下来看看FindCoordinatorResponseHandler的原理，在响应完成时会触发的它的回调函数。

```java
private class FindCoordinatorResponseHandler extends RequestFutureAdapter<ClientResponse, Void> {
    
    @Override
    public void onSuccess(ClientResponse resp, RequestFuture<Void> future) {
        clearFindCoordinatorFuture();
        // 强制转换为FindCoordinatorResponse类型
        FindCoordinatorResponse findCoordinatorResponse = (FindCoordinatorResponse) resp.responseBody();
        // 查看响应是否有错误
        Errors error = findCoordinatorResponse.error();
        if (error == Errors.NONE) {
            synchronized (AbstractCoordinator.this) {
                // use MAX_VALUE - node.id as the coordinator id to allow separate connections
                // for the coordinator in the underlying network client layer
                int coordinatorConnectionId = Integer.MAX_VALUE - findCoordinatorResponse.node().id();
                // 保存coordinator地址
                AbstractCoordinator.this.coordinator = new Node(
                        coordinatorConnectionId,
                        findCoordinatorResponse.node().host(),
                        findCoordinatorResponse.node().port());
                // 连接 Coordinator节点
                client.tryConnect(coordinator);
                // 设置心跳的超时时间
                heartbeat.resetTimeouts(time.milliseconds());
            }
            // 设置future的结果
            future.complete(null);
        } else if (error == Errors.GROUP_AUTHORIZATION_FAILED) {
            // 设置future的异常
            future.raise(new GroupAuthorizationException(groupId));
        } else {
            // 设置future的异常
            future.raise(error);
        }
    }
```



心跳线程

心跳线程有三种状态，运行，暂停和关闭。心跳线程是AbstractCoordinator的内部类

```java
public abstract class AbstractCoordinator implements Closeable {
    // 与coordinator之间的状态
    private MemberState state = MemberState.UNJOINED;
    
    private class HeartbeatThread extends KafkaThread {
        // 如果为false，表示暂停状态
        // 如果为true，表示运行状态
        private boolean enabled = false;
        // 是否为关闭状态
        private boolean closed = false;
        private AtomicReference<RuntimeException> failed = new AtomicReference<>(null);
        
        @Override
        public void run() {
            try {
                while (true) {
                    synchronized (AbstractCoordinator.this) {
                        // 如果是关闭状态，则退出
                        if (closed)
                            return;
                        // 如果是暂停状态，则等待
                        if (!enabled) {
                            AbstractCoordinator.this.wait();
                            continue;
                        }

                        if (state != MemberState.STABLE) {
                            // 如果与coordinator的连接状态有问题，则进入暂停状态
                            disable();
                            continue;
                        }

                        client.pollNoWakeup();
                        long now = time.milliseconds();

                        if (coordinatorUnknown()) {
                            if (findCoordinatorFuture != null || lookupCoordinator().failed())
                                // 检查是否找到
                                AbstractCoordinator.this.wait(retryBackoffMs);
                        } else if (heartbeat.sessionTimeoutExpired(now)) {
                            // 如果第一次心跳超时，则认为与coordinator的连接失败
                            markCoordinatorUnknown();
                        } else if (heartbeat.pollTimeoutExpired(now)) {
                            // 如果心跳超时，则认为与coordinator的连接失败，需要退出组
                            maybeLeaveGroup();
                        } else if (!heartbeat.shouldHeartbeat(now)) {
                            // 心跳发送必须保持一定的间隔，这里检查是否能发送
                            AbstractCoordinator.this.wait(retryBackoffMs);
                        } else {
                            // 设置最新发送心跳的时间
                            heartbeat.sentHeartbeat(now);
                            // 发送心跳
                            sendHeartbeatRequest().addListener(new RequestFutureListener<Void>() {
                                @Override
                                public void onSuccess(Void value) {
                                    synchronized (AbstractCoordinator.this) {
                                        // 设置最新接收心跳的时间
                                        heartbeat.receiveHeartbeat(time.milliseconds());
                                    }
                                }

                                @Override
                                public void onFailure(RuntimeException e) {
                                    synchronized (AbstractCoordinator.this) {
                                        if (e instanceof RebalanceInProgressException) {
                                            // 接收到Rebalance异常，这个coordinator正在处在reblance状态
                                            heartbeat.receiveHeartbeat(time.milliseconds());
                                        } else {
                                            heartbeat.failHeartbeat();
                                            AbstractCoordinator.this.notify();
                                        }
                                    }
                                }
                            });
                        }
                    }
                }
            } catch (...) {
                // 处理各种异常
                ....
                this.failed.set(e);
            } 
        }

    }        
}
```



心跳请求字段

group_id

generation_id

member_id



心跳响应字段

error_code



发送加入group请求

```java
private synchronized RequestFuture<ByteBuffer> initiateJoinGroup() {
    if (joinFuture == null) {
        // 暂停心跳线程
        disableHeartbeatThread();

        state = MemberState.REBALANCING;
        // 发送加入group请求
        joinFuture = sendJoinGroupRequest();
        // 添加监听器，当接收到响应后，会继续启动心跳线程
        joinFuture.addListener(new RequestFutureListener<ByteBuffer>() {
            @Override
            public void onSuccess(ByteBuffer value) {
                synchronized (AbstractCoordinator.this) {
                    //更新状态
                    state = MemberState.STABLE;
                    // 设置rejoinNeeded为false，因为join请求已经完成了
                    rejoinNeeded = false;
                    if (heartbeatThread != null)
                        // 启动心跳线程
                        heartbeatThread.enable();
                }
            }

            @Override
            public void onFailure(RuntimeException e) {
                synchronized (AbstractCoordinator.this) {
                    // 设置状态
                    state = MemberState.UNJOINED;
                }
            }
        });
    }
    return joinFuture;
}

RequestFuture<ByteBuffer> sendJoinGroupRequest() {
    if (coordinatorUnknown())
        return RequestFuture.coordinatorNotAvailable();

    // 构建请求
    JoinGroupRequest.Builder requestBuilder = new JoinGroupRequest.Builder(
        groupId,
        this.sessionTimeoutMs,
        this.generation.memberId,
        protocolType(),
        metadata()).setRebalanceTimeout(this.rebalanceTimeoutMs);

    int joinGroupTimeoutMs = Math.max(rebalanceTimeoutMs, rebalanceTimeoutMs + 5000);
    // 调用client的send发送请求，返回 RequestFuture<ClientResponse>
    // 这里调用compose方法，转换为 RequestFuture<ByteBuffer>
    return client.send(coordinator, requestBuilder, joinGroupTimeoutMs)
        .compose(new JoinGroupResponseHandler());
}
```





```java
private class JoinGroupResponseHandler extends CoordinatorResponseHandler<JoinGroupResponse, ByteBuffer> {
    
    @Override
    public void handle(JoinGroupResponse joinResponse, RequestFuture<ByteBuffer> future) {
        Errors error = joinResponse.error();
        if (error == Errors.NONE) {
            synchronized (AbstractCoordinator.this) {
                if (state != MemberState.REBALANCING) {
                    // 检查是否为REBALANCING状态，那么说明需要重新加入group，所以这次请求响应需要退出
                    future.raise(new UnjoinedGroupException());
                } else {
                    // 更新generation数据，从响应中获取generationId，memberId和groupProtocol
                    AbstractCoordinator.this.generation = new Generation(joinResponse.generationId(),
                            joinResponse.memberId(), joinResponse.groupProtocol());
                    // 如果这个consumer被认为是leader角色，那么调用onJoinLeader执行分区分配
                    if (joinResponse.isLeader()) {
                        // 注意到这里使用了chain，当onJoinLeader的结果完成后，才会调用future完成
                        onJoinLeader(joinResponse).chain(future);
                    } else {
                        // 如果这个consumer被认为是follower角色，那么调用onJoinFollower获取分配结果
                        // 注意到这里使用了chain，当onJoinFollower的结果完成后，才会调用future完成
                        onJoinFollower().chain(future);
                    }
                }
            }
        } else if (error ) {
            // 处理各种错误
            ......
            future.raise(error);
        }
    }
}
```





leader执行分配算法

```java
private RequestFuture<ByteBuffer> onJoinLeader(JoinGroupResponse joinResponse) {
    try {
        // 子类负责实现performAssignment方法，分区分配
        Map<String, ByteBuffer> groupAssignment = performAssignment(joinResponse.leaderId(), joinResponse.groupProtocol(),
                joinResponse.members());
        // 
        SyncGroupRequest.Builder requestBuilder =
                new SyncGroupRequest.Builder(groupId, generation.generationId, generation.memberId, groupAssignment);
        // 
        return sendSyncGroupRequest(requestBuilder);
    } catch (RuntimeException e) {
        return RequestFuture.failure(e);
    }
}
```

