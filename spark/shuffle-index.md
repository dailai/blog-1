# shuffle 文件索引 #

对于每个shuffle文件，都对应着一个索引文件，支持快速的根据partitionId读取数据。  

索引文件的格式如下：

```shell
-----------------------------------------------------
Long			      |	   Long	              |  
-----------------------------------------------------
partiton为0的offset    |  partiton为1的offset   | 
-----------------------------------------------------
```

 上面的 offset 是指 parition 在 shuffle 文件中的开始位置。shuffle 文件都是根据 partionId 排序过的，所以通过当前partition的开始位置和下一个partition的开始位置，就可以读取出来。IndexShuffleBlockResolver类负责索引文件的创建和读写。