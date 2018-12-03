# Task 度量 #

## merics 类 ##

所有用于记录累加的meric类，都是AccumulatorV2的子类。使用之前都要到AccumulatorContext注册，获取metadata信息。然后调用add方法更新值，最后使用value获取度量值。

```scala
abstract class AccumulatorV2[IN, OUT] extends Serializable {
    private[spark] var metadata: AccumulatorMetadata = _
    
    private[spark] def register(
      sc: SparkContext,
      name: Option[String] = None,
      countFailedValues: Boolean = false): Unit = {
    // AccumulatorContext.newId获取id
    this.metadata = AccumulatorMetadata(AccumulatorContext.newId(), name, countFailedValues)
    // 在AccumulatorContext注册
    AccumulatorContext.register(this)
  }
```

最常用的度量类是LongAccumulator和CollectionAccumulator。LongAccumulator用于计数，CollectionAccumulator用于添加数据。

## 度量数据 ##

task的运行分为三步，第一步是将字节数据解析成Task，第二步是执行Task，最后是将结果序列化。

TaskMerics有下列度量数据：

* executorDeserializeTime， 第一步解析数据的花费时间
* executorDeserializeCpuTime， 第一步花费的cpu时间
* executorRunTime， 执行Task的花费时间
* executorCpuTime， 执行Task花费的cpu时间
* resultSerializationTime， 序列化结果的花费时间
* jvmGCTime， jvm垃圾回收时间







