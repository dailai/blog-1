spark rpc 原理

spark rpc是基于netty框架的。spark rpc的客户端是TransportClient表示。通过TransportClient的建立，可以看到它是如何封装netty。TransportClient的初始化，是由TransportClientFactory负责。

```java
  private TransportClient createClient(InetSocketAddress address)
      throws IOException, InterruptedException {
	// 初始化BootStrap
    Bootstrap bootstrap = new Bootstrap();
    // 设置eventloop
    bootstrap.group(workerGroup)
        // 指定Channel Class
      .channel(socketChannelClass)
      // Disable Nagle's Algorithm since we don't want packets to wait
      .option(ChannelOption.TCP_NODELAY, true)
      .option(ChannelOption.SO_KEEPALIVE, true)
      .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, conf.connectionTimeoutMs())
      .option(ChannelOption.ALLOCATOR, pooledAllocator);
      
     // 初始化 Channel
    bootstrap.handler(new ChannelInitializer<SocketChannel>() {
      @Override
      public void initChannel(SocketChannel ch) {
         // 调用TransportContext来初始化Channel
        TransportChannelHandler clientHandler = context.initializePipeline(ch);
      }
    });

	.......
  }
```

接着进入TransportContex的方法

```java
public class TransportContext {  
  	private static final MessageEncoder ENCODER = MessageEncoder.INSTANCE;
  	private static final MessageDecoder DECODER = MessageDecoder.INSTANCE;

	public TransportChannelHandler initializePipeline(SocketChannel channel, RpcHandler channelRpcHandler) {
    	try {
          TransportChannelHandler channelHandler = createChannelHandler(channel, channelRpcHandler);
          channel.pipeline()
            .addLast("encoder", ENCODER) // 添加MessageEncoder
             // 添加frame解码
            .addLast(TransportFrameDecoder.HANDLER_NAME, NettyUtils.createFrameDecoder())
            .addLast("decoder", DECODER) // 添加MessageDecoder
             // 添加IdleStateHandler
            .addLast("idleStateHandler", new IdleStateHandler(0, 0, conf.connectionTimeoutMs() / 1000))
             // 添加TransportChannelHandler
            .addLast("handler", channelHandler);
          return channelHandler;
        } catch (RuntimeException e) {
          logger.error("Error while initializing Netty pipeline", e);
          throw e;
        }
  	}
}
```



可以看到TransportClient添加了多个ChannelHandler，接下来按照顺序，了解这些Handler的作用

TransportFrameDecoder

数据的格式为

```shell
-----------|-------------
   size    |   data
-----------|------------
 8 byte    | size byte
-----------| -----------
```



spark 通信的基本单位是Frame，它把发送的信息分成多块Frame，然后发送。TransportFrameDecoder负责将一整个Frame完整的读取出来。

```java
public class TransportFrameDecoder extends ChannelInboundHandlerAdapter {
  // Bytebuf列表，存储了从socket中读取的数据，但是没有被解析
  private final LinkedList<ByteBuf> buffers = new LinkedList<>();
  private long totalSize = 0; // 未解析的数据大小

  public void channelRead(ChannelHandlerContext ctx, Object data) throws Exception {
    ByteBuf in = (ByteBuf) data;
    // 添加至buffers列表
    buffers.add(in);
    // 更新totalSize
    totalSize += in.readableBytes();

    while (!buffers.isEmpty()) {
      // First, feed the interceptor, and if it's still, active, try again.
      if (interceptor != null) {
        ByteBuf first = buffers.getFirst();
        int available = first.readableBytes();
        if (feedInterceptor(first)) {
          assert !first.isReadable() : "Interceptor still active but buffer has data.";
        }

        int read = available - first.readableBytes();
        if (read == available) {
          buffers.removeFirst().release();
        }
        totalSize -= read;
      } else {
        // Interceptor is not active, so try to decode one frame.
        ByteBuf frame = decodeNext();
        // decodeNext返回null， 表示没有解析完。则不向下个ChannelHandler传递数据
        if (frame == null) {
          break;
        }
        // 将解析完的frame传递给下个ChannelHandler
        ctx.fireChannelRead(frame);
      }
    }
  }
```



```java
public class TransportFrameDecoder extends ChannelInboundHandlerAdapter {
    private static final int LENGTH_SIZE = 8;
  	private long nextFrameSize = UNKNOWN_FRAME_SIZE; // 下个frame的size
    
    private ByteBuf decodeNext() throws Exception {
        // 解析frame的size内容
        long frameSize = decodeFrameSize();
        // 如果frameSize没有解析出来
        // totalSize < frameSize，表示未读取的数据长度小于frame size，不能后完整的解析frame
        if (frameSize == UNKNOWN_FRAME_SIZE || totalSize < frameSize) {
          return null;
        }

        // Reset size for next frame.
        nextFrameSize = UNKNOWN_FRAME_SIZE;

        Preconditions.checkArgument(frameSize < MAX_FRAME_SIZE, "Too large frame: %s", frameSize);
        Preconditions.checkArgument(frameSize > 0, "Frame length should be positive: %s", frameSize);
 
        // 如果buffers列表的第一个Bytebuf有足够的数据，直接解析
        int remaining = (int) frameSize;
        if (buffers.getFirst().readableBytes() >= remaining) {
          return nextBufferForFrame(remaining);
        }

        // 循环遍历buffers，直到frame的数据读取完
        CompositeByteBuf frame = buffers.getFirst().alloc().compositeBuffer(Integer.MAX_VALUE);
        while (remaining > 0) {
          ByteBuf next = nextBufferForFrame(remaining);
          remaining -= next.readableBytes();
          frame.addComponent(next).writerIndex(frame.writerIndex() + next.readableBytes());
        }
        assert remaining == 0;
        return frame;
      }
    
}
```

decodeFrameSize方法

```java
  private long decodeFrameSize() {
    //  nextFrameSize 如果不为UNKNOWN_FRAME_SIZE， 即表示已经解析出来了
    // totalSize小于8，表示未读取的数据小于需要的8位，不能够解析nextFrameSize
    if (nextFrameSize != UNKNOWN_FRAME_SIZE || totalSize < LENGTH_SIZE) {
      return nextFrameSize;
    }

    // 如果buffers列表的第一个Bytebuf，大于8个字节，则直接读取
    ByteBuf first = buffers.getFirst();
    if (first.readableBytes() >= LENGTH_SIZE) {
      nextFrameSize = first.readLong() - LENGTH_SIZE;
      totalSize -= LENGTH_SIZE;
      if (!first.isReadable()) {
        buffers.removeFirst().release();
      }
      return nextFrameSize;
    }
      
	// 循环读取buffers的列表，直到读取8个字节
    while (frameLenBuf.readableBytes() < LENGTH_SIZE) {
      ByteBuf next = buffers.getFirst();
      int toRead = Math.min(next.readableBytes(), LENGTH_SIZE - frameLenBuf.readableBytes());
      frameLenBuf.writeBytes(next, toRead);
      if (!next.isReadable()) {
        buffers.removeFirst().release();
      }
    }
	// 解析出nextFrameSize
    nextFrameSize = frameLenBuf.readLong() - LENGTH_SIZE;
    totalSize -= LENGTH_SIZE;
    frameLenBuf.clear();
    return nextFrameSize;
  }
```



MessageDecoder

MessageDecoder接收从TransportFrameDecoder解析完的Frame数据。然后将Frame解析成Message，最后给TransportChannelHandler处理。

Message的数据格式

```shell
-----------|-------------
   type    |   data
-----------|------------
 1 byte    |   
-----------| -----------
```

```java
public final class MessageDecoder extends MessageToMessageDecoder<ByteBuf> {
    @Override
    public void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) {
        // 读取message type
        Message.Type msgType = Message.Type.decode(in);
        // 根据type的种类，调用不同类型的解析
        Message decoded = decode(msgType, in);
        assert decoded.type() == msgType;
        logger.trace("Received message {}: {}", msgType, decoded);
        out.add(decoded);
      }
    
    private Message decode(Message.Type msgType, ByteBuf in) {
        switch (msgType) {
            case ChunkFetchRequest:
                return ChunkFetchRequest.decode(in);
                ......
        }
    }
}
```



