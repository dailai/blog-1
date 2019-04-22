# Kafka Schema Registry 原理

Confluent 公司为了Kafka 支持 Avro 序列化，创建了 Kafka Schema Registry 项目，项目地址为 <https://github.com/confluentinc/schema-registry> 。顾名思义 Registry作为一个注册中心，它负责管理 Kafka 所有 topic 的数据格式。



## Avro 序列化示例

Avro 序列化相比常见的序列化（比如 json）会更快，序列化的数据会更小。相比 protobuf ，它可以支持实时编译，不需要像 protobuf 那样先定义好数据格式文件，编译之后才能使用。下面简单的介绍下 如何使用 Avro 序列化：

数据格式文件：

```json
{
 "namespace": "example.avro",
 "type": "record",
 "name": "User",
 "fields": [
     {"name": "name", "type": "string"},
     {"name": "favorite_number",  "type": ["int", "null"]},
     {"name": "favorite_color", "type": ["string", "null"]}
 ]
}
```



序列化生成字节：

```java
// 解析数据格式文件  
Schema schema = new Schema.Parser().parse(new File("user.avsc"));
// 创建一个实例
GenericRecord user1 = new GenericData.Record(schema);
user1.put("name", "Alyssa");
user1.put("favorite_number", 256);

// 构建输出流，保存结果
ByteArrayOutputStream out = new ByteArrayOutputStream();
// BinaryEncoder负责向输出流，写入数据
BinaryEncoder encoder =  EncoderFactory.get().directBinaryEncoder(out, null);
// DatumWriter负责序列化
DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<GenericRecord>(schema);
// 调用DatumWriter序列化，并将结果写入到输出流
datumWriter.write(value, encoder);
// 刷新缓存
encoder.flush();

// 获取序列化的结果
byte[] result = out.toByteArray();
```



更多用法可以参见官方文档，<http://avro.apache.org/docs/current/gettingstartedjava.html>



## Kafka 客户端使用原理

Kafka 如果要使用 Avro 序列化， Kafka Schema Registry 提供了 KafkaAvroSerializer 和 KafkaAvroDeserializer 两个类，在实例化 KafkaProducer 和 KafkaConsumer 时， 指定序列化或反序列化的配置。



下面以实例 KafkaProducer 为例，运行这段代码之前，需要保证 Kafka Schema Registry 服务已经运行

```java
public class SchemaProducer {

    public static void main(String[] args) throws Exception {
        
        String kafkaHost = "xxx.xxx.xxx.xxx:9092";
        String topic = "schema-tutorial";
        String schameFilename = "user.json";
        String registryHost = "http://xxx.xxx.xxx.xxx:8081";

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaHost);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        // 指定Value的序列化类，KafkaAvroSerializer
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        // 指定 registry 服务的地址
        props.put("schema.registry.url", registryHost);
        KafkaProducer<String, GenericRecord> producer = new KafkaProducer<>(props);

        String key = "Alyssa key";
        Schema schema = new Schema.Parser().parse(new File(schameFilename));
        GenericRecord avroRecord = new GenericData.Record(schema);
        avroRecord.put("name", "Alyssa");
        avroRecord.put("favorite_number", 256);

        // 发送消息
        ProducerRecord<String, GenericRecord> record = new ProducerRecord<>(topic, key, avroRecord);
        producer.send(record);
        producer.flush();
        producer.close();
    }
}
```



上面使用到了 KafkaAvroSerializer 序列化消息，接下来看看 KafkaAvroSerializer 的 原理。我们知道 Kafka 的消息由 Key 和 Value 组成，这两部分的值可以有不同的数据格式。而这些数据格式都会保存在 Registry 服务端，客户端需要指定数据格式的名称，才能获取到。如果我们要获取当前消息 Key 这部分的数据格式，它对于的名称为 <topic>-key，如果要获取 Value 这部分的数据格式，它对应的名称为 <topic>-value（topic 为该消息所在的 topic 名称）。

 Kafka Schema Registry 还支持修改数据格式，这样对于同一个 topic ，它的消息有多个版本，前面的消息和最新的消息都可能会完全不一样，那么客户怎么区分呢。Registry 会为每种数据格式都会分配一个 id 号，然后发送的每条消息都会附带对应的数据格式 id。



KafkaProducer 在第一次序列化的时候，会自动向 Registry 服务端注册。服务端保存数据格式后，会返回一个 id 号。KafkaProducer发送消息的时候，需要附带这个 id 号。这样 KafkaConsumer 在读取消息的时候，通过这个 id 号，就可以从 Registry 服务端 获取。



Registry 客户端负责向服务端发送请求，每个请求后会将结果缓存起来，以提高性能。

```java
public class CachedSchemaRegistryClient implements SchemaRegistryClient {
  // Key 为数据格式的名称， 里面的 Value 为 Map类型，它对于的 Key 为数据格式，Value 为对应的 id 号
  private final Map<String, Map<Schema, Integer>> schemaCache;
  // Key 为数据格式的名称，里面的 Value 为 Map类型，它对于的 Key 为 id 号，Value 为对应的数据格式
  // 这个集合比较特殊，当 Key 为 null 时，表示 id 到 数据格式的缓存
  private final Map<String, Map<Integer, Schema>> idCache;
        
  @Override
  public synchronized int register(String subject, Schema schema, int version, int id)
      throws IOException, RestClientException {
    // 从schemaCache查找缓存，如果不存在则初始化空的哈希表
    final Map<Schema, Integer> schemaIdMap =
        schemaCache.computeIfAbsent(subject, k -> new HashMap<>());

    // 获取对应的 id 号
    final Integer cachedId = schemaIdMap.get(schema);
    if (cachedId != null) {
      // 检查 id 号是否有冲突
      if (id >= 0 && id != cachedId) {
        throw new IllegalStateException("Schema already registered with id "
            + cachedId + " instead of input id " + id);
      }
      // 返回缓存的 id 号
      return cachedId;
    }

    if (schemaIdMap.size() >= identityMapCapacity) {
      throw new IllegalStateException("Too many schema objects created for " + subject + "!");
    }
      
    // 如果缓存没有，则向服务端发送 http 请求 
    final int retrievedId = id >= 0
                            ? registerAndGetId(subject, schema, version, id)
                            : registerAndGetId(subject, schema);
    // 缓存结果
    schemaIdMap.put(schema, retrievedId);
    idCache.get(null).put(retrievedId, schema);
    return retrievedId;
  }
}    
```



## Registry 服务端



### 处理请求





自增 id 生成器

自增生成器目前有两种实现方式。一种是基于内存的，自己维护。另外一种是基于zookeeper的，每次获取一个 id 段，然后一个 id， 一个 id 的分配出去。









### 存储数据

Registry 服务端将数据格式存储到 Kafka 中，对应的 topic 名称为 _schemas。存储在该 topic 的消息，格式如下：

* Key 部分的值，包含数据格式名称，版本号，由 SchemaRegistryKey 类表示。 

* Value部分的值，包含数据格式名称，版本号， 数据格式 id 号，数据格式的内容，是否被删除， 由 SchemaRegistryValue 类表示。

 Registry 服务端在存储Kafka之前，还会将上述的 Key 和 Value 序列化，目前序列化由两种方式：

*  json 序列化，由 ZkStringSerializer 类负责
* 将 SchemaRegistryKey 或 SchemaRegistryValue 强制转换为 String 类型保存起来





### 高可用

如果要实现高可用，需要运行多个 Registry 服务，这些服务中必须选择出一个 leader，所有的请求都是由 leader 来负责。当 leader 挂掉之后，就会触发选举操作，来选举出新的 leader。选举的实现有两种方式，基于kafka 和 基于 zookeeper。 

基于 kafka 的原理是利用消费组，因为消费组的每个成员都需要和 kafka coordinator 服务端保持心跳，如果有成员挂了，那么就会触发组的重分配操作。重分配操作会从存活的成员中，选出 leader 角色。



基于 zookeeper 的方式，会更加简单，效率也更高。因为只有 leader 挂掉，zookeeper 才会触发重新选举。而基于 kafka 的方式，只要是有一个成员挂掉，不管它是不是 leader，都会触发重新选举。如果这个成员不是 leader，则会造成不必要的选举。

