# Hbase Bulkload 原理

当需要大批量的向Hbase导入数据时，我们可以使用Hbase Bulkload的方式，这种方式是先生成Hbase的底层存储文件 HFile，然后直接将这些 HFile 移动到Hbase的存储目录下。它相比调用Hbase 的 put 接口添加数据，处理效率更快并且对Hbase 运行影响更小。

使用 Bulkload 分为两部，生成 HFile 和 移动HFile。下面假设我们有一个 CSV 文件，是存储用户购买记录的。它一共有三列， order_id，consumer，product。我们需要将这个文件导入到Hbase里，其中 order_id 作为Hbase 的 row key。

```shell
bin/hbase org.apache.hadoop.hbase.mapreduce.ImportTsv -Dimporttsv.separator=$'\x01'
-Dimporttsv.columns=HBASE_ROW_KEY,cf:consumer,cf:product
 -Dimporttsv.bulk.output=<hdfs://storefileoutput> <hbase_table> <hdfs://datainput>
 
bin/hbase org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles <hdfs://storefileoutput> <hbase_table>
```

只需要上述两部就可以完成批量导入，下面我们来深入了解其原理。



## 底层实现原理

HFile 的生成是调用了 MapReduce 来实现的。它有两种实现方式，虽然最后生成的 HFile 是一样的，但中间过程却是不一样。现在我们先回顾下 MapReduce 的编程模型：

* InputFormat：负责读取数据源，并且将数据源切割成多个分片，分片的数目等于Map的数目

* Mapper：负责接收分片，生成中间结果<K, V>，K 为数据的 key 值类型，V为数据的 value 值类型

* Reducer：Mapper的数据会按照 key 值分组，Reducer接收的数据格式<K, List<V>>

* OutputFormat：负责将Reducer生成的数据持久化，比如存储到 hdfs。



### MapReduce 实现 一

MapReducer 程序中各个组件的实现类，如下所示：

* InputFormat 类：TextInputFormat，数据输出格式 LongWritable，Text

* Mapper 类：TsvImporterTextMapper，数据输出格式 ImmutableBytesWritable, Text

* Reduce 类：TextSortReducer，数据输出格式 ImmutableBytesWritable, KeyValue

* OutputFormat 类：HFileOutputFormat2，负责将结果持久化 HFile

执行过程如下：

1. TextInputFormat 会读取数据源文件，按照文件在 hdfs 的 Block 切割，每个Block对应着一个切片

2. Mapper会解析每行数据，然后从中解析出 row key。生成（row key， 行数据）

3. Reducer 会解析行数据，为每列生成 KeyValue。这里简单说下KeyValue，它是 Hbase 存储每列数据的格式， 详细原理后面会介绍到。如果一个 row key 对应的数据过多，它会分批处理。处理完一批数据之后，会写入（null，null）这一条特殊的数据，表示HFileOutputFormat2持久的过程中，需要创建新的HFile。

因为Mapper和Reducer的实现比较简单，这里不再详细介绍

### MapReduce 实现 二

MapReducer 程序中各个组件的实现类，如下所示：

* InputFormat 类：TextInputFormat，数据输出格式 LongWritable，Text

* Mapper 类：TsvImporterMapper，数据输出格式 ImmutableBytesWritable，Put

* Combiner 类：PutCombiner

* Reducer 类：PutSortReducer，数据输出格式 ImmutableBytesWritable, KeyValue
* OutputFormat 类：HFileOutputFormat2，负责将结果持久化 HFile

这里使用了 Combiner，它的作用是在 Map 端进行一次初始的 reduce 操作，起到聚合的作用，这样就减少了 Reduce 端与 Map 端的数据传输，提高了运行效率。

执行过程如下：

1. TextInputFormat 会读取数据源文件，原理同实现 一

1. Mapper会解析每行数据，然后从中解析出 row key，并且生成Put实例。生成（row key， Put）
2. Combine会将 row key 对应的多个Put进行合并，它也是分批合并的。
3. Reducer 会遍历Put实例，为每列生成KeyValue并且去重。

这里讲下PutSortReducer的具体实现，下面的代码经过简化，去掉了KeyValue中关于Tag的处理：

```java
public class PutSortReducer extends
    Reducer<ImmutableBytesWritable, Put, ImmutableBytesWritable, KeyValue> {
  // the cell creator
  private CellCreator kvCreator;
  
  @Override
  protected void reduce(
      ImmutableBytesWritable row,
      java.lang.Iterable<Put> puts,
      Reducer<ImmutableBytesWritable, Put,
              ImmutableBytesWritable, KeyValue>.Context context)
      throws java.io.IOException, InterruptedException
  {
    // 这里指定了一个阈值，默认为10GB过大。如果puts中不重复的数据过大，就会按照这个阈值分批处理
    long threshold = context.getConfiguration().getLong(
        "putsortreducer.row.threshold", 1L * (1<<30));
    Iterator<Put> iter = puts.iterator();
    // 开始遍历 puts列表
    while (iter.hasNext()) {
      // 这个TreeSet就是用来去重的，比如向同个qualifier添加值
      TreeSet<KeyValue> map = new TreeSet<>(CellComparator.getInstance());
      // 记录map里保存的数据长度
      long curSize = 0;
      // 遍历 puts列表，直到不重复的数据不超过阈值
      while (iter.hasNext() && curSize < threshold) {
        // 从列表中获取值
        Put p = iter.next();
        // 遍历这个Put的所有列值，一个Put包含了多列，这些列由Cell表示
        for (List<Cell> cells: p.getFamilyCellMap().values()) {
          for (Cell cell: cells) {
            KeyValue kv = null;
            kv = KeyValueUtil.ensureKeyValue(cell);
            }
            if (map.add(kv)) {
              // 如果这列值没有重复，那么添加到TreeSet中，并且更新curSize的值
              curSize += kv.heapSize();
            }
          }
        }
      }
      // 将map里的数据，调用context.write方法输出
      int index = 0;
      for (KeyValue kv : map) {
        context.write(row, kv);
        if (++index % 100 == 0)
          context.setStatus("Wrote " + index);
      }

      // 如果还有，那么说明此行数据过大，那么就会输出一条特殊的记录(null, null)
      if (iter.hasNext()) {
        // force flush because we cannot guarantee intra-row sorted order
        context.write(null, null);
      }
    }
  }
}  
```

从上面的代码可以看到，PutSortReducer会使用到TreeSet去重，TreeSet会保存数据，默认不超过 1GB。如果当Reducer的内存设置过小时，并且数据过大时，是有可能会造成内存溢出。如果遇到这种情况，可以通过减少阈值或者增大Reducer的内存。

### 选择哪种实现方式

如果用户指定 importtsv.mapper.class 的值为 org.apache.hadoop.hbase.mapreduce.TsvImporterTextMapper，那么就会采用第一种策略，否则默认采用第二种策略。



## 数据源解析

Mapper 接收到数据后，需要解析每行数据的格式。它会按照分割符来切割数据，然后根据指定的列格式，生成每列的数据。客户在使用命令时，通过 importtsv.separator 参数指定分隔符，通过 importtsv.columns 参数指定列格式。客户端可以指定列名， 有些列名会有着特殊含义，比如 HBASE_ROW_KEY 代表着该列是作为 row key，HBASE_TS_KEY 代表着该列作为数据的 timestamp，HBASE_ATTRIBUTES_KEY 代表着该列是属性列等。

ImportTsv.TsvParser 类负责解析数据源，它定义在 ImportTsv 类里。它的原理比较简单，这里不再详细介绍。



## Reducer的数目选择

我们知道MapReduce程序的一般瓶颈在于 reduce 阶段，如果我们能够适当增加 reduce 的数目，一般能够提高运行效率（如果数据倾斜不严重）。

Hbase 支持超大数据量的表，它会将表的数据自动切割，分布在不同的服务上。这些数据切片在 Hbase 里，称为Region， 每个Region只负责一段 row key 范围的数据。

Hbase Bulkload 的 Reducer 数目，等于表的 Region 数目。Hbase会自动创建表，但是创建的表的region数只有一个。所以在生成HFile之前，可以自行创建表，指定 Region 的数目和每个Reigion 的 row key 值分布范围。

HFileOutputFormat2 的 configureIncrementalLoad 方法，会读取表的 region 分布情况，然后调用 setNumReduceTasks 方法设置 reduce 数目。下面的代码经过简化：

```java
public class HFileOutputFormat2
    extends FileOutputFormat<ImmutableBytesWritable, Cell> {
    
    public static void configureIncrementalLoad(Job job, TableDescriptor tableDescriptor,
      RegionLocator regionLocator) throws IOException {
    	ArrayList<TableInfo> singleTableInfo = new ArrayList<>();
    	singleTableInfo.add(new TableInfo(tableDescriptor, regionLocator));
    	configureIncrementalLoad(job, singleTableInfo, HFileOutputFormat2.class);
  	}    
  	static void configureIncrementalLoad(Job job, List<TableInfo> multiTableInfo,
      	Class<? extends OutputFormat<?, ?>> cls) throws IOException {
        // 这里虽然支持多表，但是批量导入时只会使用单表
        List<RegionLocator> regionLocators = new ArrayList<>( multiTableInfo.size());     
      	for( TableInfo tableInfo : multiTableInfo )
      	{
            // 获取region分布情况
      		regionLocators.add(tableInfo.getRegionLocator());
     		......
      	}
        // 获取region的row key起始大小
      	List<ImmutableBytesWritable> startKeys = getRegionStartKeys(regionLocators, writeMultipleTables);
        // 设置reduce的数目
      	job.setNumReduceTasks(startKeys.size());
    }  
}
```



## Hbase 数据存储格式

Hbase的每列数据都是单独存储的，都是以 KeyValue 的形式。KeyValue 的数据格式如下图所示：

```shell
-----------------------------------------------
 keylength | valuelength | key | value | Tags
-----------------------------------------------
```

其中 key 的格式如下：

```shell
----------------------------------------------------------------------------------------------
 rowlength | row | columnfamilylength | columnfamily | columnqualifier | timestamp | keytype 
----------------------------------------------------------------------------------------------
```

Tags的格式如下：

```shell
-------------------------
 tagslength | tagsbytes 
-------------------------
```

tagsbytes 可以包含多个 tag，每个 tag 的格式如下：

```shell
----------------------------------
 taglength | tagtype | tagbytes
----------------------------------
```



Reducer 会使用 CellCreator 类，负责生成 KeyValue。CellCreator的原理很简单，也就是实例化KeyValue。



## 生成 HFile

HFileOutputFormat2 负责将Reduce的结果，持久化成 HFile 文件。持久化目录的格式如下：

```shell
.
|---- column_family_1
|    |---- uuid_1
|    `---- uuid_2
|---- column_family_2
|    |---- uuid3
|    `---- uuid4
```



HFileOutputFormat2 会创建 RecordWriter 实例，所有数据的写入都是通过 RecordWriter。 

```java
public class HFileOutputFormat2
    extends FileOutputFormat<ImmutableBytesWritable, Cell> {
    
  @Override
  public RecordWriter<ImmutableBytesWritable, Cell> getRecordWriter(
      final TaskAttemptContext context) throws IOException, InterruptedException {
    return createRecordWriter(context, this.getOutputCommitter(context));
  }
    
  static <V extends Cell> RecordWriter<ImmutableBytesWritable, V>
      createRecordWriter(final TaskAttemptContext context, final OutputCommitter committer)
          throws IOException {
      
      return new RecordWriter<ImmutableBytesWritable, V>() {
          ......
      }
  }
}
```

可以看到 createRecordWriter 方法，返回了一个匿名类。继续看看这个匿名类的定义：

```java
// 封装了StoreFileWriter，记录了写入的数据长度
static class WriterLength {
    long written = 0;
    StoreFileWriter writer = null;
}

class RecordWriter<ImmutableBytesWritable, V>() {
    // key值为表名和column family组成的字节，value为对应的writer
    private final Map<byte[], WriterLength> writers = new TreeMap<>(Bytes.BYTES_COMPARATOR);
    // 是否需要创建新的HFile
    private boolean rollRequested = false;
}
```

从上面 WriterLength 类的定义，我们可以知道 RecordWriter的底层原理是调用了StoreFileWriter的接口。对于StoreFile，我们回忆下Hbase的写操作，它接收客户端的写请求，首先写入到内存中MemoryStore，然后刷新到磁盘生成StoreFile。如果该表有两个column family，就会有两个MemoryStore和两个StoreFile，对应于不同的column family。所以 RecordWriter 类有个哈希表，每个column family 就对应一个 StoreFileWriter。（这里说的StoreFile就是HFile）

```java
class RecordWriter<ImmutableBytesWritable, V>() {
    // favoredNodes 表示创建HFile文件，希望尽可能在这些服务器节点上
    private WriterLength getNewWriter(byte[] tableName, byte[] family, Configuration conf, InetSocketAddress[] favoredNodes) throws IOException {
        // 根据表名和column family生成唯一字节
        byte[] tableAndFamily = getTableNameSuffixedWithFamily(tableName, family);
        Path familydir = new Path(outputDir, Bytes.toString(family));
        WriterLength wl = new WriterLength();
        // 获取HFile的压缩算法
        Algorithm compression = compressionMap.get(tableAndFamily);
        // 获取bloom过滤器信息
        BloomType bloomType = bloomTypeMap.get(tableAndFamily);
        // 获取HFile其他的配置
        .....
        // 生成HFile的配置信息
        HFileContextBuilder contextBuilder = new HFileContextBuilder()
                                    .withCompression(compression)
                                    .withChecksumType(HStore.getChecksumType(conf))
                                    .withBytesPerCheckSum(HStore.getBytesPerChecksum(conf))
                                    .withBlockSize(blockSize);
        HFileContext hFileContext = contextBuilder.build();
        // 实例化 StoreFileWriter
        f (null == favoredNodes) {
          wl.writer =
              new StoreFileWriter.Builder(conf, new CacheConfig(tempConf), fs)
                  .withOutputDir(familydir).withBloomType(bloomType)
              .withComparator(CellComparator.getInstance()).withFileContext(hFileContext).build();
        } else {
          wl.writer =
              new StoreFileWriter.Builder(conf, new CacheConfig(tempConf), new HFileSystem(fs))
                  .withOutputDir(familydir).withBloomType(bloomType)
                  .withComparator(CellComparator.getInstance()).withFileContext(hFileContext)
                  .withFavoredNodes(favoredNodes).build();
        }
        // 添加到 writers集合中
        this.writers.put(tableAndFamily, wl);
        return wl;
    }
}
```

上面创建StoreFileWriter时指定了文件目录，StoreFileWriter会在文件目录下，使用 uuid 生成一个唯一的文件名。

继续看看 RecordWriter 的写操作：

```java
class RecordWriter<ImmutableBytesWritable, V>() {
    @Override
    public void write(ImmutableBytesWritable row, V cell)
        Cell kv = cell;
         // 收到空数据，表示需要立即刷新到磁盘，并且创建新的HFile
    	if (row == null && kv == null) {
            // 刷新到磁盘
            rollWriters(null);
            return;
        }
        // 根据table和column family生成唯一值
    	byte[] tableAndFamily = getTableNameSuffixedWithFamily(tableNameBytes, family);
        // 获取对应的writer
    	WriterLength wl = this.writers.get(tableAndFamily);
    	if (wl == null) {
            // 如果为空，那么先创建对应的文件目录
            Path writerPath = null;
            writerPath = new Path(outputDir, Bytes.toString(family));
            fs.mkdirs(writerPath);
        }
        // 检测当前HFile的大小是否超过了最大值，默认为10GB
    	if (wl != null && wl.written + length >= maxsize) {
            this.rollRequested = true;
        }
        // 如果当前HFile过大，那么需要将它刷新到磁盘
    	if (rollRequested && Bytes.compareTo(this.previousRow, rowKey) != 0) {
            rollWriters(wl);
        }
        // 创建writer
    	if (wl == null || wl.writer == null) {
            if (conf.getBoolean(LOCALITY_SENSITIVE_CONF_KEY, DEFAULT_LOCALITY_SENSITIVE)) {
                // 如果开启了位置感知，那么就会去获取row所在的region的地址
                HRegionLocation loc = null;
                loc = locator.getRegionLocation(rowKey);
                InetSocketAddress initialIsa = new InetSocketAddress(loc.getHostname(), loc.getPort());
                // 创建writer，指定了偏向节点
                wl = getNewWriter(tableNameBytes, family, conf, new InetSocketAddress[] { initialIsa})
            } else {
                // 创建writer
                wl = getNewWriter(tableNameBytes, family, conf, null);
            }
        }
    	wl.writer.append(kv);
    	wl.written += length;
    	this.previousRow = rowKey;
	}

	private void rollWriters(WriterLength writerLength) throws IOException {
        if (writerLength != null) {
            // 关闭当前writer
            closeWriter(writerLength);
        } else {
            // 关闭所有family对应的writer
            for (WriterLength wl : this.writers.values()) {
                closeWriter(wl);
            }
        }
        this.rollRequested = false;
    }

	private void closeWriter(WriterLength wl) throws IOException {
        if (wl.writer != null) {
            close(wl.writer);
        }
        wl.writer = null;
        wl.written = 0;
    }
}   	
```

RecordWriter在写入数据时，如果遇到一条 row key 和 value 都为 null 的数据时，这条数据有着特殊的含义，表示writer应该立即 flush。在每次创建RecordWriter时，它会根据此时row key 的值，找到所属 Region 的服务器地址，然后尽量在这台服务器上，创建新的HFile文件。