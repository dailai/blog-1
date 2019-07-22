# HBase 基准测试



## 生成测试数据

先建表，预计生成10亿条数据，该表有10个预分区，一个

```shell
sudo -u hbase hbase shell
hbase(main):011:0> n_splits = 10
hbase(main):012:0> create 'usertable', 'info', {SPLITS => (1..n_splits).map {|i| "user#{100000000+i*100000000/n_splits}"}}
```

生成测试数据

```shell
sudo -u hbase bin/ycsb load hbase12 -P workloads/workloada -cp conf/ -p table=TestTable -p columnfamily=info -threads 10 -p recordcount=1000000000
```



