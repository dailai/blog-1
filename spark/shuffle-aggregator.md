# Aggregator #

Aggregator表示根据key分组对value聚合。 数据源的格式是key，value类型。key的类型对应着K，value的类型对应着V。它有三个重要的属性，都是函数:

* createCombiner，V => C，接收V类型，返回C类型
* mergeValue，(C, V) => C， 将V类型的数据，合并到C类型
* mergeCombiners， (C, C) => C)， 合并C类型的数据

这个三个函数，都有着不同的作用。现在讲讲聚合操作的原理。聚合一般发生在shuffle的阶段。

这里演示一个例子

 (key_0, value_0), 	(key_0, value_1), 	(key_0, value_2)

(key_1, value_3),	(key_1, value_4)

首先将相同的Key分组。然后对每一个分组，做聚合

聚合第一步，以key_0分组为例：

第一步，将第一个value数据转化为Combiner类型（对应着C。比如遇到(key_0, value_0)，将value_0 生成 combine_0数据。这里调用了createCombiner函数

第二步，将后面的value，依次合并到C类型的数据。这就是mergeValue的作用。比如遇到(key_0, value_1)，讲述value_1添加到combine_0数据里

第三部，然后继续遇到(key_0, value_2),恰好这时候触发spill，会新建一个combiner_1数据, 将数据value_2添加combiner_1里。

第四部，最后将所有的combiner数据汇总，比如 合并combiner_0 和 combiner_1，这里调用了mergeCombiners函数。







