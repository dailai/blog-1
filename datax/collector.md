## collector ##
datax需要收集两个地方的运行数据，TaskGroup的数据和Task的数据。关于collector这个模块的类设计，是比较奇怪的。按照常理是先设计一个抽象类或接口AbstractCollector,然后设计两个子类，比如TaskColletor和TaskGroupCollector。但是datax却只有一个抽象类AbstractCollector和一个子类ProcessInnerCollector。

### task的数据收集 ###
Task的数据收集的所有方法，都是由AbstractCollector负责。它使用Map<Integer, Communication> 保存Task的数据。
```java
public abstract class AbstractCollector {

    private Map<Integer, Communication> taskCommunicationMap = new ConcurrentHashMap<Integer, Communication>();

    // 返回HashMap
    public Map<Integer, Communication> getTaskCommunicationMap() {
        return taskCommunicationMap;
    }
    
    // 注册Task和Communication
    public void registerTaskCommunication(List<Configuration> taskConfigurationList) {
        for (Configuration taskConfig : taskConfigurationList) {
            int taskId = taskConfig.getInt(CoreConstant.TASK_ID);
            this.taskCommunicationMap.put(taskId, new Communication());
        }
    }

    // 收集Task的数据，并且汇总
    public Communication collectFromTask() {
        Communication communication = new Communication();
        communication.setState(State.SUCCEEDED);

        for (Communication taskCommunication :
                this.taskCommunicationMap.values()) {
            communication.mergeFrom(taskCommunication);
        }

        return communication;
    }

    // 返回task id 对应的数据
    public Communication getTaskCommunication(Integer taskId) {
        return this.taskCommunicationMap.get(taskId);
    }
}
```


### task group的数据收集 ###
TaskGroup的收集相关的方法，都是由AbstractCollector转发给LocalTGCommunicationManager负责。
#### LocalTGCommunicationManager ####
LocalTGCommunicationManager 负责所有TaskGroup的Communication的管理。Communication是保存一些数据的容器，详细介绍可以参考这篇博客。

LocalTGCommunicationManager的原理，是使用ConcurrentHashMap来保存所有的Communication，Key为task_group_id, Value为对应的Communication。
```java
public final class LocalTGCommunicationManager {
    // HashMap保存所有的Communication
    private static Map<Integer, Communication> taskGroupCommunicationMap =
            new ConcurrentHashMap<Integer, Communication>();

    // 新增 communication
    public static void registerTaskGroupCommunication(
            int taskGroupId, Communication communication) {
        taskGroupCommunicationMap.put(taskGroupId, communication);
    }

    // 获取所有的communication，汇总的数据
    public static Communication getJobCommunication() {
        Communication communication = new Communication();
        communication.setState(State.SUCCEEDED);

        for (Communication taskGroupCommunication :
                taskGroupCommunicationMap.values()) {
            // 遍历Communication， 将数据相加汇总
            communication.mergeFrom(taskGroupCommunication);
        }

        return communication;
    }
}
```


#### AbstractCollector的原理 ####

```java
public abstract class AbstractCollector {
    // 注册taskgroup
    public void registerTGCommunication(List<Configuration> taskGroupConfigurationList) {
        for (Configuration config : taskGroupConfigurationList) {
            int taskGroupId = config.getInt(
                    CoreConstant.DATAX_CORE_CONTAINER_TASKGROUP_ID);
            LocalTGCommunicationManager.registerTaskGroupCommunication(taskGroupId, new Communication());
        }
    }

    // 收集和汇总所有TaskGroup的数据
    public abstract Communication collectFromTaskGroup();

    // 获取TaskGroup和Communication的HashMap
    public Map<Integer, Communication> getTGCommunicationMap() {
        return LocalTGCommunicationManager.getTaskGroupCommunicationMap();
    }

    // 获取对应taskGroupId的Communication
    public Communication getTGCommunication(Integer taskGroupId) {
        return LocalTGCommunicationManager.getTaskGroupCommunication(taskGroupId);
    }
}

public class ProcessInnerCollector extends AbstractCollector {

    public ProcessInnerCollector(Long jobId) {
        super.setJobId(jobId);
    }

    @Override
    public Communication collectFromTaskGroup() {
        return LocalTGCommunicationManager.getJobCommunication();
    }

}
```




