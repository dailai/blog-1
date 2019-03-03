# Kafka Client 架构 #





KafkaClient负责发送网络请求的接口，NetworkClient实现了KafkaClient的接口

RecordAccumulator为Producer的发送缓存，

Sender线程负责将缓存的数据，通过NetworkClient发送给服务端



KafkaProducer  --->     Sender   ------>  NetworkClient



KafkaConsumer

Fetcher负责





KafkaConsumer   --->     Fetcher  ---->  ConsumerNetworkClient   ------>  NetworkClient



 

NetworkClient内部原理



包含kafka里的Selector

NetworkClient管理着与Kafka多个节点的连接，连接状态保存在ClusterConnectionStates。

NetworkClient发送消息，不会等待前一条消息有响应后，才去发送当前消息。它为了提高吞吐量，会将请求连续的发送出去，已经发送但还没有响应的请求，就会保存到InFlightRequests里。

DefaultMetadataUpdater实现了MetadataUpdater的接口，负责更新kafka的元信息。使用调用requestUpdate请求立即更新元信息。NetworkClient会调用maybeUpdate方法更新。



NetworkClient在每次poll的时候，都会调用MetadataUpdater的maybeUpdate，尝试更新元信息。

NetworkClient的poll方法会处理响应，连接等问题。通过调用请求的回调函数



Sender线程会循环调用NetworkClient的poll方法

KafkaProducer的waitOnMetadata会设置Metadata的更新标记，等待Sender更新

KafkaConsumer会调用Fetcher的getTopicMetadata方法，更新Metadata。Fetcher会构造获取Metadata的请求，通过ConsumerNetworkClient发送，然后调用NetworkClient的poll方法更新Metadata。



ConsumerNetworkClient对于网络请求，返回RequestFuture，它类似于异步编程，还支持添加回调函数







NetworkClient的newClientRequest方法，构建请求ClientRequest，ClientRequest有包含回调函数，由RequestCompletionHandler表示。

NetworkClient发送的请求，只接受ClientRequest。

NetworkClient发送ClientRequest之后，会将请求封装成InFlightRequest，保存到InFlightRequests里面。

InFlightRequests包含每个节点的，正在发送的所有请求。

当一个不需要响应的请求发送完后，会从InFlightRequests中，将对应的节点的最新请求剔除掉

当NetworkClient接收到响应时，会从InFlightRequests中，将对应的节点的最老请求剔除掉。

因为tcp和kafka server可以保证数据传输的有序性和请求处理的有序性。

当连接失败时，会从InFlightRequests中，将对应的节点的所有请求清除。



当NetworkClient接收到响应时，会将数据解析成AbstractResponse，然后封装成ClientResponse。

最后会调用ClientResponse的成功或失败的回调函数，这样就完整的完成了这一次请求。





序列化由AbstractRequestResponse负责，它有serialize方法。

AbstractRequest继承AbstractRequestResponse，它有一个内部类Builder，用来创建AbstractRequest。

每个kafka的请求都会继承AbstractRequest，并且实现自己的Builder类。

AbstractRequest的数据通过Struct，Field，Schema来序列化。

NetworkClient会将请求序列化，转换为NetworkSend，然后交给Selector发送出去。 