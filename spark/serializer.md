# 序列化 #

## 序列化种类 ##

spark计算任务，是由不同的节点上共同计算完成的，其中还有中间数据的保存。这些都涉及到了数据的序列化。

这篇文章讲的是spark 2.2，目前支持Java自带的序列化，还有KryoSerializer。KryoSerializer目前只能支持简单的数据类型，好像2.4对KryoSerializer的支持会更好。