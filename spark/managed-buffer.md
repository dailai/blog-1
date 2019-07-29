# ManagedBuffer

因为 Netty 的 Buffer 实现是 ByteBuf，而且 java 则是ByteBuffer。为了更好的支持两者之间的转化，使用了ManagedBuffer 来统一接口。

并且为了支持 netty 的零拷贝技术，ManagedBuffer 还支持以文件为基础的实现。





## 接口

```java
public abstract class ManagedBuffer {
  // 返回数据的长度
  public abstract long size();
  
  // 将数据以ByteBuffer的格式返回
  public abstract ByteBuffer nioByteBuffer() throws IOException;
  
  // 以流的形式返回数据
  public abstract InputStream createInputStream() throws IOException;
  
  // 增加引用，只有NettyManagedBuffer子类才支持
  public abstract ManagedBuffer retain();
  
  // 减少引用，只有NettyManagedBuffer子类才支持
  public abstract ManagedBuffer release();

  // 当使用netty将数据发送出去时，会调用此方法完成数据格式转换。
  // 返回结果可能是 ByteBuf（netty实现的），或者 FileRegion（用于实现文件到socket的zero-copy）
  public abstract Object convertToNetty() throws IOException;
}   
```

 



## FileSegmentManagedBuffer

FileSegmentManagedBuffer 是以底层文件，来实现 ManagedBuffer 的接口。它通过下面三个属性描述了数据的存储位置

```java
public final class FileSegmentManagedBuffer extends ManagedBuffer {
  private final File file;  // 文件路径
  private final long offset;  // 数据起始位置
  private final long length;  // 数据的长度
}
```



我们来看看 nioByteBuffer 接口的实现。如果数据小于指定的值（由 spark.storage.memoryMapThreshold 配置指定，默认为2MB），那么直接将数据存到堆中的 ByteBuffer。否则，采用内存映射的方式。

```java
public ByteBuffer nioByteBuffer() throws IOException {
    FileChannel channel = null;
    channel = new RandomAccessFile(file, "r").getChannel();
    if (length < conf.memoryMapBytes()) {
        ByteBuffer buf = ByteBuffer.allocate((int) length);
        channel.position(offset);
        while (buf.remaining() != 0) {
            channel.read(buf)
        }
        buf.flip();
        return buf;
    } else {
        return channel.map(FileChannel.MapMode.READ_ONLY, offset, length);
    }
}

public Object convertToNetty() throws IOException {
    if (conf.lazyFileDescriptor()) {
        // 等待要发送出去的时候，才去打开文件
        return new DefaultFileRegion(file, offset, length);
    } else {
        // 现在就打开文件
        FileChannel fileChannel = new FileInputStream(file).getChannel();
        return new DefaultFileRegion(fileChannel, offset, length);
    }
}

```



## NettyManagedBuffer

NettyManagedBuffer 是使用了 netty 管理的 ByteBuf

```java
import io.netty.buffer.ByteBuf;

public class NettyManagedBuffer extends ManagedBuffer {
  private final ByteBuf buf;

  public ByteBuffer nioByteBuffer() throws IOException {
    return buf.nioBuffer();
  }

  public Object convertToNetty() throws IOException {
    return buf.duplicate().retain();
  }    
}
```





## NioManagedBuffer

NioManagedBuffer 是以 java 自带的ByteBuffer来实现的。

```java
public class NioManagedBuffer extends ManagedBuffer {
  private final ByteBuffer buf;
  
  public ByteBuffer nioByteBuffer() throws IOException {
    return buf.duplicate();
  }

  @Override
  public Object convertToNetty() throws IOException {
    return Unpooled.wrappedBuffer(buf);
  }    
}
```













