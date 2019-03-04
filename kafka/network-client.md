# Kafka NetworkClient 原理 #

无论是Kafka的生产者，还是Kakfa的消费者，都会通过NetworkClient发送消息。

使用NetworkClient的方法是

1. 调用NetworkClient的newClientRequest方法，创建请求ClientRequest
2. 调用NetworkClient的ready方法，连接服务端
3. 然后调用NetworkClient的send方法，发送请求
4. 最后调用NetworkClient的poll方法，处理响应



NetworkClient发送请求之前，都需要先和服务端创建连接。NetworkClient负责管理与集群的所有连接。

元数据描述了集群的节点状态，如果元数据更新

```java
public class NetworkClient implements KafkaClient {

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
            this.connectionStates.connecting(nodeConnectionId, now);
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





