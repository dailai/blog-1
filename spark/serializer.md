# 序列化 #

spark计算任务，是由不同的节点上共同计算完成的，其中还有中间数据的保存。这些都涉及到了数据的序列化。

## 序列化种类 ##

这篇文章讲的是spark 2.2，目前支持Java自带的序列化，还有KryoSerializer。KryoSerializer目前只能支持简单的数据类型，2.4对KryoSerializer的支持会更好。   



## UML 类图 ##

这里spark使用了抽象工厂模式。SerializerInstance代表着抽象工厂，SerializationStream代表着序列化流，DeserializationStream代表着反序列化流。这里有两种实现方法，java和kryo。

{% plantuml %}

@startuml spark-serializer

class SerializerInstance

class SerializationStream

class DeserializationStream

class JavaSerializerInstance

class JavaSerializationStream

class JavaDeserializationStream

class KryoSerializerInstance

class KryoSerializationStream

class KryoDeserializationStream

SerializerInstance --> SerializationStream

SerializerInstance --> DeserializationStream

SerializerInstance <|-- JavaSerializerInstance

SerializationStream <|-- JavaSerializationStream

DeserializationStream <|-- JavaDeserializationStream

JavaSerializerInstance --> JavaSerializationStream

JavaSerializerInstance --> JavaDeserializationStream

SerializerInstance <|-- KryoSerializerInstance

SerializationStream <|-- KryoSerializationStream

DeserializationStream <|-- KryoDeserializationStream

KryoSerializerInstance --> KryoSerializationStream

KryoSerializerInstance --> KryoDeserializationStream

@enduml

{% endplantuml %}



## Java序列化 ##

serialize方法实现了序列化一个对象，它调用了serializeStream生成流，然后写入数据。

```scala
private[spark] class JavaSerializerInstance(
    counterReset: Int, extraDebugInfo: Boolean, defaultClassLoader: ClassLoader)
  extends SerializerInstance {

  override def serialize[T: ClassTag](t: T): ByteBuffer = {
    // 生成bytebuffer输出流
    val bos = new ByteBufferOutputStream()
    // 装饰序列化流
    val out = serializeStream(bos)
    // 写入数据
    out.writeObject(t)
    out.close()
    // 返回bytebuffer
    bos.toByteBuffer
  }
  
  // 返回JavaSerializationStream流
  override def serializeStream(s: OutputStream): SerializationStream = {
    new JavaSerializationStream(s, counterReset, extraDebugInfo)
  }
 
}
```

JavaSerializationStream作为流的装饰器，提供了序列化的功能。其实这里仅仅对ObjectOutputStream的封装，ObjectOutputStream是属于java库的类，通过它可以将数据序列化。不过ObjectOutputStream有个缺陷，当序列化的数据连续是同一个类型，ObjectOutputStream为了优化序列化的空间效率，会在内存中保存这些数据，这个有可能会导致内存溢出。所以JavaSerializationStream这里设置了定期每写入一定数目的数据，就会调用reset，避免这个问题。

```scala
private[spark] class JavaSerializationStream(
    out: OutputStream, counterReset: Int, extraDebugInfo: Boolean)
  extends SerializationStream {
      
  private val objOut = new ObjectOutputStream(out)
  // 自从上一次reset后，写入的数量
  private var counter = 0

  /**
   * Calling reset to avoid memory leak:
   * http://stackoverflow.com/questions/1281549/memory-leak-traps-in-the-java-standard-api
   * But only call it every 100th time to avoid bloated serialization streams (when
   * the stream 'resets' object class descriptions have to be re-written)
   */
  def writeObject[T: ClassTag](t: T): SerializationStream = {
    try {
      objOut.writeObject(t)
    } catch {
      case e: NotSerializableException if extraDebugInfo =>
        throw SerializationDebugger.improveException(t, e)
    }
    counter += 1
    // 每写入counterReset的数据，则调用reset
    if (counterReset > 0 && counter >= counterReset) {
      objOut.reset()
      counter = 0
    }
    this
  }

  def flush() { objOut.flush() }
  def close() { objOut.close() }
}
```



deserialize 方法提供了反序列化，反序列化涉及到了类的动态加载，这里可以指定ClassLoader。它生成JavaDeserializationStream，通过它解析数据。

```scala
private[spark] class JavaSerializerInstance(
    counterReset: Int, extraDebugInfo: Boolean, defaultClassLoader: ClassLoader)
  extends SerializerInstance {
  
  override def deserialize[T: ClassTag](bytes: ByteBuffer): T = {
    val bis = new ByteBufferInputStream(bytes)
    val in = deserializeStream(bis)
    in.readObject()
  }

  override def deserialize[T: ClassTag](bytes: ByteBuffer, loader: ClassLoader): T = {
    // 生成bytebuffer的输入流
    val bis = new ByteBufferInputStream(bytes)
   // 装饰反序列化流
    val in = deserializeStream(bis, loader)
    in.readObject()
  }

  override def serializeStream(s: OutputStream): SerializationStream = {
    new JavaSerializationStream(s, counterReset, extraDebugInfo)
  }

  override def deserializeStream(s: InputStream): DeserializationStream = {
    new JavaDeserializationStream(s, defaultClassLoader)
  }

  def deserializeStream(s: InputStream, loader: ClassLoader): DeserializationStream = {
    new JavaDeserializationStream(s, loader)
  }
  
```



JavaDeserializationStream的原理，它使用了ObjectInputStream类。ObjectInputStream类是属于java库的，它提供了反序列化的功能。这里实现了resolveClass方法，提供了指定ClassLoader来加载类。

```
private[spark] class JavaDeserializationStream(in: InputStream, loader: ClassLoader)
  extends DeserializationStream {
  
  private val objIn = new ObjectInputStream(in) {
    override def resolveClass(desc: ObjectStreamClass): Class[_] =
      try {
        // scalastyle:off classforname
        Class.forName(desc.getName, false, loader)
        // scalastyle:on classforname
      } catch {
        case e: ClassNotFoundException =>
          JavaDeserializationStream.primitiveMappings.getOrElse(desc.getName, throw e)
      }
  }

  def readObject[T: ClassTag](): T = objIn.readObject().asInstanceOf[T]
  def close() { objIn.close() }
}
```



## Kryo 序列化 ##

### kryo 初始化 ###

kryo的初始化，在KryoSerializer类里。这里主要是Kryo预先注册需要序列化的类。

```scala
// 是够需要注册类，才能序列化对应的实例
private val registrationRequired = conf.getBoolean("spark.kryo.registrationRequired", false)

def newKryo(): Kryo = {
  // 这里通过EmptyScalaKryoInstantiator工厂，实例化Kryo
  val instantiator = new EmptyScalaKryoInstantiator
  val kryo = instantiator.newKryo()
  kryo.setRegistrationRequired(registrationRequired)

  // 如果没有ClassLoader，则使用当前线程的ClassLoader
  val oldClassLoader = Thread.currentThread.getContextClassLoader
  val classLoader = defaultClassLoader.getOrElse(Thread.currentThread.getContextClassLoader)


  // 注册类
  .........
  
  kryo.setClassLoader(classLoader)
  kryo
}
```



serialize方法实现了序列化一个对象，

```scala
class KryoSerializer(conf: SparkConf) {
  // 默认分配的缓存初始大小
  private val bufferSizeKb = conf.getSizeAsKb("spark.kryoserializer.buffer", "64k")
  // 默认分配的缓存最大值
  val maxBufferSizeMb = conf.getSizeAsMb("spark.kryoserializer.buffer.max", "64m").toInt
  val maxBufferSizeMb = conf.getSizeAsMb("spark.kryoserializer.buffer.max", "64m").toInt
  private val maxBufferSize = ByteUnit.MiB.toBytes(maxBufferSizeMb).toInt
    
    
  // 实例化KryoOutput， 使用默认的配置
  def newKryoOutput(): KryoOutput =
    if (useUnsafe) {
      new KryoUnsafeOutput(bufferSize, math.max(bufferSize, maxBufferSize))
    } else {
      new KryoOutput(bufferSize, math.max(bufferSize, maxBufferSize))
    }
  }
}


private[spark] class KryoSerializerInstance(ks: KryoSerializer, useUnsafe: Boolean)
  extends SerializerInstance {
  // 缓存的Kryo
  @Nullable private[this] var cachedKryo: Kryo = borrowKryo()
  
  // 调用KryoSerializer的newKryoOutput，实例化Kryo的缓存
  private lazy val output = ks.newKryoOutput()

  override def serialize[T: ClassTag](t: T): ByteBuffer = {
    // 清除数据
    output.clear()
    // 获取Kryo， 如果有缓存，则直接返回。否则需要创建Kryo
    val kryo = borrowKryo()
    try {
      // 序列化数据
      kryo.writeClassAndObject(output, t)
    } catch {
      case e: KryoException if e.getMessage.startsWith("Buffer overflow") =>
        throw new SparkException(s"Kryo serialization failed: ${e.getMessage}. To avoid this, " +
          "increase spark.kryoserializer.buffer.max value.", e)
    } finally {
      releaseKryo(kryo)
    }
    // 返回序列化后的数据，注意output.toBytes会返回新的数组
    ByteBuffer.wrap(output.toBytes)
  }
```

 

serializeStream方法返回KryoSerializationStream

```scala
class KryoSerializationStream(
    serInstance: KryoSerializerInstance,
    outStream: OutputStream,
    useUnsafe: Boolean) extends SerializationStream {

  // 通过outStream，来实例化KryoOutput
  private[this] var output: KryoOutput =
    if (useUnsafe) new KryoUnsafeOutput(outStream) else new KryoOutput(outStream)

  private[this] var kryo: Kryo = serInstance.borrowKryo()

  override def writeObject[T: ClassTag](t: T): SerializationStream = {
    // 调用kryo的方法，序列化数据，写入outStream
    kryo.writeClassAndObject(output, t)
    this
  }
}
```



## SerializerManager ##

SerializerManager会自动选择选用哪种序列化。SerializerManager对于使用Kryo序列化的条件比较苛刻，需要数据类型为原始类型或其对应的数组。

getSerializer 会 根据条件挑选出序列化方式。首先需要支持autoPick为true，然后需要对应的类型可以支持kryo。

支持kryo的类型，有字符串类型，基本数据类型和其对应的数组类型。

```scala
def getSerializer(ct: ClassTag[_], autoPick: Boolean): Serializer = {
  // 如果允许自动挑选，并且这种类型的数据支持kryo
  if (autoPick && canUseKryo(ct)) {
    kryoSerializer
  } else {
    // 否则返回默认序列化
    defaultSerializer
  }
}

// 字符串类型
private[this] val stringClassTag: ClassTag[String] = implicitly[ClassTag[String]]

private[this] val primitiveAndPrimitiveArrayClassTags: Set[ClassTag[_]] = {
  // 基本类型
  val primitiveClassTags = Set[ClassTag[_]](
      ClassTag.Boolean,
      ClassTag.Byte,
      ClassTag.Char,
      ClassTag.Double,
      ClassTag.Float,
      ClassTag.Int,
      ClassTag.Long,
      ClassTag.Null,
      ClassTag.Short
    )
    // 基本类型对应的数组
    val arrayClassTags = primitiveClassTags.map(_.wrap)
    // 合并类型
    primitiveClassTags ++ arrayClassTags
}

def canUseKryo(ct: ClassTag[_]): Boolean = {
  primitiveAndPrimitiveArrayClassTags.contains(ct) || ct == stringClassTag
}
```



## 

