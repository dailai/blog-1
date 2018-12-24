### channel number ###

adjustChannelNumber方法， byte计算channel数目， record计算channel数目， 或者指定channel数目

doReaderSplit方法， 调用Reader.Job的split方法，返回Reader.Task的Configuration列表

doWriterSplit方法， 调用Writer.JOb的split方法，返回Writer.Task的Configuration列表

获取transformer的Configuration列表

合并reader，writer，transformer配置列表

生成整个Task的Configuration列表，格式为：

```json
{
    "taskId": "",
    "reader": {
        "name": "",
        "parameter": {
            ......
        }
    },
    "writer": {
        "name": "",
        "parameter": {
            ......
        }
    },
    "transformer": [
        ......
    ]

}
```


更新JobContainer的Configuration， job.content元素


needChannelNumber最后取上面计算的出来的，与task的个数的最小值
