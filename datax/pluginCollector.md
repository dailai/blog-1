## plugin collector ##

plugin collector用于插件的数据收集。

### 类图 ###

![plugin collector](https://github.com/zhmin/blog/blob/datax/datax/images/plugin_collector.png?raw=true)

PluginCollector接口，只是一个空接口

JobPluginCollector接口，增加了两个方法，用来获取消息的

DefaultJobPluginCollector类实现了JobPluginCollector接口。它通过从AbstractContainerCommunicator中获取消息，返回

### 原理 ###

```java
public final class DefaultJobPluginCollector implements JobPluginCollector {
    private AbstractContainerCommunicator jobCollector;

    // 初始化AbstractContainerCommunicator
    public DefaultJobPluginCollector(AbstractContainerCommunicator containerCollector) {
        this.jobCollector = containerCollector;
    }

    @Override
    public Map<String, List<String>> getMessage() {
        // 返回jobCollector的结果
        Communication totalCommunication = this.jobCollector.collect();
        return totalCommunication.getMessage();
    }

    @Override
    public List<String> getMessage(String key) {
        // 返回jobCollector的结果
        Communication totalCommunication = this.jobCollector.collect();
        return totalCommunication.getMessage(key);
    }
}
```

