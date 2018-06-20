### commnunicator ###
commnunicator封装了Collector和Reporter，提供给Job和TaskGroup使用。
使用步骤是：
  1. registerCommunication，登记注册信息
  2. collect，获取统计信息
  3. report，向上汇报信息


### 类的设计 ###
![](https://github.com/zhmin/blog/blob/datax/datax/images/communicator.png?raw=true)

StandAloneJobContainerCommunicator类，负责顶层Job与TaskGroup的通信。

StandaloneTGContainerCommunicatorl类，负责TaskGroup与Task的通信

### StandAloneJobContainerCommunicator ###

#### Job的使用 ####
Scheduler使用StandAloneJobContainerCommunicator类
```java
public abstract class AbstractScheduler {

    public void schedule(List<Configuration> configurations) {
        // 注册taskgroup的configuration
        this.containerCommunicator.registerCommunication(configurations);
        // 收集所有taskgroup的数据
        Communication nowJobContainerCommunication = this.containerCommunicator.collect();
        
        Communication reportCommunication = CommunicationTool.getReportCommunication(nowJobContainerCommunication, lastJobContainerCommunication, totalTasks);
        // 向上级报告
        this.containerCommunicator.report(reportCommunication);
    }
}
```


#### 原理 ####
```java
    public StandAloneJobContainerCommunicator(Configuration configuration) {
        super(configuration);
        // 使用 ProcessInnerCollector
        super.setCollector(new ProcessInnerCollector(configuration.getLong(
                CoreConstant.DATAX_CORE_CONTAINER_JOB_ID)));
        // 使用 ProcessInnerReporter
        super.setReporter(new ProcessInnerReporter());
    }

    @Override
    public void registerCommunication(List<Configuration> configurationList) {
        // 调用ProcessInnerCollector的registerTGCommunication方法
        super.getCollector().registerTGCommunication(configurationList);
    }


    @Override
    public void report(Communication communication) {
        super.getReporter().reportJobCommunication(super.getJobId(), communication);
        //打印出
        LOG.info(CommunicationTool.Stringify.getSnapshot(communication));
        reportVmInfo();
    }
```



### StandaloneTGContainerCommunicator ###

#### 使用 ####
TaskGroupContainer使用StandaloneTGContainerCommunicator类，
```java
        
        // 注册taskgroup的configuration
        this.containerCommunicator.registerCommunication(taskConfigs);
        
        containerCommunicator.resetCommunication(taskId);

```

#### 原理 ####

```java
public class StandaloneTGContainerCommunicator extends AbstractTGContainerCommunicator {

    public StandaloneTGContainerCommunicator(Configuration configuration) {
        super(configuration);
        super.setReporter(new ProcessInnerReporter());
    }

    @Override
    public void report(Communication communication) {
        super.getReporter().reportTGCommunication(super.taskGroupId, communication);
    }

}

    public void registerCommunication(List<Configuration> configurationList) {
        super.getCollector().registerTaskCommunication(configurationList);
    }

    @Override
    public final Communication collect() {
        return this.getCollector().collectFromTask();
    }

```
