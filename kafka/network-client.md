# Kafka NetworkClient 原理 #

无论是Kafka的生产者，还是Kakfa的消费者，都会通过NetworkClient发送消息。

使用NetworkClient的方法是

1. 调用NetworkClient的ready方法，连接服务端
2. 调用NetworkClient的poll方法，处理连接
3. 调用NetworkClient的newClientRequest方法，创建请求ClientRequest
4. 然后调用NetworkClient的send方法，发送请求
5. 最后调用NetworkClient的poll方法，处理响应



NetworkClient发送请求之前，都需要先和服务端创建连接。NetworkClient负责管理与集群的所有连接。

元数据描述了集群的节点状态，如果元数据更新

```java
public class NetworkClient implements KafkaClient {
    // Kafka的Selector，用来异步发送网络数据
    private final Selectable selector;  
    // 保存与每个节点的连接状态
    private final ClusterConnectionStates connectionStates;

    public boolean ready(Node node, long now) {
        if (node.isEmpty())
            throw new IllegalArgumentException("Cannot connect to empty node " + node);
        // 判断是否允许发送请求
        if (isReady(node, now))
            return true;

        if (connectionStates.canConnect(node.idString(), now))
            initiateConnect(node, now);

        return false;
    }

    @Override
    public boolean isReady(Node node, long now) {
        // 当发现正在更新元数据时，会禁止发送请求。因为有可能集群的节点挂了，只有获取完元数据才能知道
        // 当连接没有创建完毕或者当前发送的请求过多时，也会禁止发送请求
        return !metadataUpdater.isUpdateDue(now) && canSendRequest(node.idString());
    }    
    
    // 检测连接状态，检测发送请求是否过多
    private boolean canSendRequest(String node) {
        return connectionStates.isReady(node) && selector.isChannelReady(node) && inFlightRequests.canSendMore(node);
    }
    
    // 创建连接
    private void initiateConnect(Node node, long now) {
        String nodeConnectionId = node.idString();
        try {
            // 更新连接状态为正在连接
            this.connectionStates.connecting(nodeConnectionId, now);
            // 调用selector异步连接
            selector.connect(nodeConnectionId,
                             new InetSocketAddress(node.host(), node.port()),
                             this.socketSendBuffer,
                             this.socketReceiveBuffer);
        } catch (IOException e) {
            /* attempt failed, we'll try again after the backoff */
            connectionStates.disconnected(nodeConnectionId, now);
            /* maybe the problem is our metadata, update it */
            metadataUpdater.requestUpdate();
        }
    }
}
```



接下来看是如何处理连接的，poll方法会调用handleConnections处理连接，并且会创建版本请求。

```java
public class NetworkClient implements KafkaClient {
    // Kafka的Selector，用来异步发送网络数据
    private final Selectable selector;    
    // 是否需要与服务器的版本协调，默认都为true
    private final boolean discoverBrokerVersions;
    // 存储着要发送的版本请求，Key为主机地址，Value为构建请求的Builder
    private final Map<String, ApiVersionsRequest.Builder> nodesNeedingApiVersionsFetch = new HashMap<>();
    
    // 处理连接
    private void handleConnections() {
        // 遍历刚创建完成的连接
        for (String node : this.selector.connected()) {
            if (discoverBrokerVersions) {
                // 更新连接的状态为版本协调状态
                this.connectionStates.checkingApiVersions(node);
                // 将请求保存到nodesNeedingApiVersionsFetch集合里
                nodesNeedingApiVersionsFetch.put(node, new ApiVersionsRequest.Builder());
                log.debug("Completed connection to node {}. Fetching API versions.", node);
            } else {
                this.connectionStates.ready(node);
                log.debug("Completed connection to node {}. Ready.", node);
            }
        }
    }
}
```

创建完版本请求，接下来看看是如何发送的。poll方法会调用handleInitiateApiVersionRequests发送版本协调请求，然后调用handleApiVersionsResponse负责处理响应。

```java
// 发送版本协调请求
private void handleInitiateApiVersionRequests(long now) {
    // 遍历请求集合nodesNeedingApiVersionsFetch
    Iterator<Map.Entry<String, ApiVersionsRequest.Builder>> iter = nodesNeedingApiVersionsFetch.entrySet().iterator();
    while (iter.hasNext()) {
        Map.Entry<String, ApiVersionsRequest.Builder> entry = iter.next();
        String node = entry.getKey();
        // 判断是否允许发送请求
        if (selector.isChannelReady(node) && inFlightRequests.canSendMore(node)) {
            log.debug("Initiating API versions fetch from node {}.", node);
            ApiVersionsRequest.Builder apiVersionRequestBuilder = entry.getValue();
            // 调用newClientRequest生成请求
            ClientRequest clientRequest = newClientRequest(node, apiVersionRequestBuilder, now, true);
            // 发送请求
            doSend(clientRequest, true, now);
            iter.remove();
        }
    }
}

// 处理版本协调响应
private void handleApiVersionsResponse(List<ClientResponse> responses,
                                       InFlightRequest req, long now, ApiVersionsResponse apiVersionsResponse) {
    final String node = req.destination;
    // 判断响应和版本是否协调
    if (apiVersionsResponse.error() != Errors.NONE) {
        // 处理响应异常
        ......
    }
    // 更新连接状态为ready，表示可以发送正式请求了
    this.connectionStates.ready(node);

}
    
```



生成请求

```java
public final class ClientRequest {
    // 主机地址
    private final String destination;
    // 请求体的builder
    private final AbstractRequest.Builder<?> requestBuilder;
    // 请求头的correlation id
    private final int correlationId;
    // 请求头的client id
    private final String clientId;
    // 创建时间
    private final long createdTimeMs;
    // 是否需要响应
    private final boolean expectResponse;
    // 回调函数，用于处理响应
    private final RequestCompletionHandler callback;
}
```

NetworkClient的newClientRequest，也只是简单的实例化ClientRequest

```java
public class NetworkClient implements KafkaClient {
    
    public ClientRequest newClientRequest(String nodeId, AbstractRequest.Builder<?> requestBuilder, long createdTimeMs, boolean expectResponse, RequestCompletionHandler callback) {
        return new ClientRequest(nodeId, requestBuilder, correlation++, clientId, createdTimeMs, expectResponse, callback);
    }
}
```



发送请求

NetworkClient首先根据ClientRequest，生成请求，然后将序列化，通过Selector异步发送出去，并且将请求封装成InFlightRequest，保存到队列InFlightRequests里。

```java
public class NetworkClient implements KafkaClient {
    // 请求队列，保存正在发送但还没有收到响应的请求
    private final InFlightRequests inFlightRequests;
    
    private final List<ClientResponse> abortedSends = new LinkedList<>();
    
    // 发送请求
    public void send(ClientRequest request, long now) {
        doSend(request, false, now);
    }
    
    // 检测请求版本是否支持，如果支持则发送请求
    private void doSend(ClientRequest clientRequest, boolean isInternalRequest, long now) {
        AbstractRequest.Builder<?> builder = clientRequest.requestBuilder();
        try {
            // 检测版本
            NodeApiVersions versionInfo = apiVersions.get(nodeId);
            .....
            // 调用doSend方法
            doSend(clientRequest, isInternalRequest, now, builder.build(version));
        } catch (UnsupportedVersionException e) {
            // 请求的版本不协调，那么生成clientResponse，添加到abortedSends集合里
            ClientResponse clientResponse = new ClientResponse(clientRequest.makeHeader(builder.latestAllowedVersion()),
                    clientRequest.callback(), clientRequest.destination(), now, now,
                    false, e, null);
            abortedSends.add(clientResponse);
        }
    }
    
    // isInternalRequest表示发送前是否需要验证连接状态，如果为true则表示使用者已经确定连接是好的
    // request表示请求体
    private void doSend(ClientRequest clientRequest, boolean isInternalRequest, long now, AbstractRequest request) {
        String nodeId = clientRequest.destination();
        // 生成请求头
        RequestHeader header = clientRequest.makeHeader(request.version());
        // 结合请求头和请求体，序列化数据，保存到NetworkSend
        Send send = request.toSend(nodeId, header);
        // 生成InFlightRequest实例，它保存了发送前的所有信息
        InFlightRequest inFlightRequest = new InFlightRequest(
                header,
                clientRequest.createdTimeMs(),
                clientRequest.destination(),
                clientRequest.callback(),
                clientRequest.expectResponse(),
                isInternalRequest,
                request,
                send,
                now);
        // 添加到inFlightRequests集合里
        this.inFlightRequests.add(inFlightRequest);
        // 调用Selector异步发送数据
        selector.send(inFlightRequest.send);
    }   
}
```



InFlightRequest存储着请求所有的信息，不仅如此，它还支持

当正常收到响应时，completed 方法会根据响应内容生成ClientRespon se。

当连接突然断开，disconnected方法会生成ClientResponse。

```java
static class InFlightRequest {
    // 请求体
    final RequestHeader header;
    // 请求地址
    final String destination;
    // 回调函数
    final RequestCompletionHandler callback;
    // 是否需要服务端返回响应
    final boolean expectResponse;
    // 请求体
    final AbstractRequest request;
    // 表示发送前是否需要验证连接状态
    final boolean isInternalRequest; 
    // 请求的序列化数据
    final Send send;
    // 发送时间
    final long sendTimeMs;
    // 请求的创建时间，这个是ClientRequest的创建时间
    final long createdTimeMs;

    public ClientResponse completed(AbstractResponse response, long timeMs) {
        return new ClientResponse(header, callback, destination, createdTimeMs, timeMs, false, null, response);
    }

    public ClientResponse disconnected(long timeMs) {
        return new ClientResponse(header, callback, destination, createdTimeMs, timeMs, true, null, null);
    }
}
```



接下来看看集合InFlightRequests，它为每个连接生成一条队列，存储着还未收到响应的请求。根据队列的实时长度，它能控制发送的速度。这里简单介绍下它的几个方法

```java
final class InFlightRequests {
    // 最大的请求数目
    private final int maxInFlightRequestsPerConnection;
    // InFlightRequest集合，Key为主机地址，Value为请求队列
    private final Map<String, Deque<NetworkClient.InFlightRequest>> requests = new HashMap<>();
    
    // 将请求添加到队列头部
    public void add(NetworkClient.InFlightRequest request);
    
    // 取出该连接，最老的请求
    public NetworkClient.InFlightRequest completeNext(String node);
    
    // 取出该连接，最新的请求
    public NetworkClient.InFlightRequest completeLastSent(String node);
        
    // 判断是否该连接能发送请求
    public boolean canSendMore(String node) {
        Deque<NetworkClient.InFlightRequest> queue = requests.get(node);
        return queue == null || queue.isEmpty() ||
            // 必须等待前面的请求发送完毕，
            // 并且没有响应的请求数目小于指定数目
               (queue.peekFirst().send.completed() && queue.size() < this.maxInFlightRequestsPerConnection);
    }    
}
```



当请求发送完成后，会触发handleCompletedSends函数，处理那些不需要响应的请求。

```java
private void handleCompletedSends(List<ClientResponse> responses, long now) {
    // 遍历发送完成的请求，通过调用Selector获得自从上一次poll开始的请求
    for (Send send : this.selector.completedSends()) {
        // 从队列中取出最新的请求
        InFlightRequest request = this.inFlightRequests.lastSent(send.destination());
        if (!request.expectResponse) {
            // 如果这个请求不要求响应，则调用completed方法生成ClientResponse
            this.inFlightRequests.completeLastSent(send.destination());
            responses.add(request.completed(null, now));
        }
    }
}
```

这里可能有点疑惑，怎么能保证从Selector返回的请求，是对应到队列中最新的请求。仔细想一下，每个请求发送，都要等待前面的请求发送完成，这样就能保证同一时间只有一个请求正在发送   。因为Selector返回的请求，是从上一次poll开始的，所以这样就能够保证。



当请求收到响应后，会触发

```java
private void handleCompletedReceives(List<ClientResponse> responses, long now) {
    for (NetworkReceive receive : this.selector.completedReceives()) {
        String source = receive.source();
        InFlightRequest req = inFlightRequests.completeNext(source);
        Struct responseStruct = parseStructMaybeUpdateThrottleTimeMetrics(receive.payload(), req.header,
            throttleTimeSensor, now);
        if (log.isTraceEnabled()) {
            log.trace("Completed receive from node {} for {} with correlation id {}, received {}", req.destination,
                req.header.apiKey(), req.header.correlationId(), responseStruct);
        }
        AbstractResponse body = AbstractResponse.parseResponse(req.header.apiKey(), responseStruct);
        if (req.isInternalRequest && body instanceof MetadataResponse)
            metadataUpdater.handleCompletedMetadataResponse(req.header, now, (MetadataResponse) body);
        else if (req.isInternalRequest && body instanceof ApiVersionsResponse)
            handleApiVersionsResponse(responses, req, now, (ApiVersionsResponse) body);
        else
            responses.add(req.completed(body, now));
    }
}
```