

Java 的 Stream 分为下面四种类型

1. IntStream
2. DoubleStream
3. LongStream
4. Stream<T>，里面的类型 T 表示各种类型，可以无缝切换。

上面的前三个是基本数值流，后面的是对象流。



四种流类型的的转化如下所示：

mapToInt 方法负责将当前流转换为 IntStream

mapToDouble 方法负责将当前流转换为 DoubleStream

mapToLong 方法负责将当前流转换为 DoubleStream

mapToObj 方法负责将当前流转换为 Stream<T>



当然对于IntStream 对应的 Stream<Interger>，DoubleStream 对应的 Stream<Double>，LongStream 对应的 Stream<Long> 的转换，有更加简单的 boxed 方法。

