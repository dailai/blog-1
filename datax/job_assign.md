## 任务分配 ##

### 分配算法 ###

1. 首先根据指定的channel数目和每个Taskgroup的拥有channel数目，计算出Taskgroup的数目
2. 将任务根据reader.parameter.loadBalanceResourceMark和writer.parameter.loadBalanceResourceMark来讲任务分组
3. 轮询任务组，依次轮询的向各个TaskGroup添加一个，直到所有任务都被分配完

### 代码解释 ###
任务的分配是由JobAssignUt类负责。使用者调用assignFairly方法，传入参数，返回TaskGroup配置列表

```java

public final class JobAssignUtil {
    /**
    * configuration 配置
    * channelNumber， channel总数
    * channelsPerTaskGroup， 每个TaskGroup拥有的channel数目
    */
    public static List<Configuration> assignFairly(Configuration configuration, int channelNumber, int channelsPerTaskGroup) {
        
        List<Configuration> contentConfig = configuration.getListConfiguration(CoreConstant.DATAX_JOB_CONTENT);
        // 计算TaskGroup的数目
        int taskGroupNumber = (int) Math.ceil(1.0 * channelNumber / channelsPerTaskGroup);

        ......
        // 任务分组
        LinkedHashMap<String, List<Integer>> resourceMarkAndTaskIdMap = parseAndGetResourceMarkAndTaskIdMap(contentConfig);
        // 调用doAssign方法，分配任务
        List<Configuration> taskGroupConfig = doAssign(resourceMarkAndTaskIdMap, configuration, taskGroupNumber);

        // 调整 每个 taskGroup 对应的 Channel 个数（属于优化范畴）
        adjustChannelNumPerTaskGroup(taskGroupConfig, channelNumber);
        return taskGroupConfig;
    }
}
```
#### 任务分组 ####

```java
    /**
    * contentConfig参数，task的配置列表
    */
    private static LinkedHashMap<String, List<Integer>> parseAndGetResourceMarkAndTaskIdMap(List<Configuration> contentConfig) {
        // reader的任务分组，key为分组的名称，value是taskId的列表
        LinkedHashMap<String, List<Integer>> readerResourceMarkAndTaskIdMap = new LinkedHashMap<String, List<Integer>>();
        // writer的任务分组，key为分组的名称，value是taskId的列表
        LinkedHashMap<String, List<Integer>> 
        writerResourceMarkAndTaskIdMap = new LinkedHashMap<String, List<Integer>>();

        for (Configuration aTaskConfig : contentConfig) {
            int taskId = aTaskConfig.getInt(CoreConstant.TASK_ID);
            
            // 取出reader.parameter.loadBalanceResourceMar的值，作为分组名
            String readerResourceMark = aTaskConfig.getString(CoreConstant.JOB_READER_PARAMETER + "." + CommonConstant.LOAD_BALANCE_RESOURCE_MARK);
            if (readerResourceMarkAndTaskIdMap.get(readerResourceMark) == null) {
                readerResourceMarkAndTaskIdMap.put(readerResourceMark, new LinkedList<Integer>());
            }
            // 把 readerResourceMark 加到 readerResourceMarkAndTaskIdMap 中
            readerResourceMarkAndTaskIdMap.get(readerResourceMark).add(taskId);

            // 取出writer.parameter.loadBalanceResourceMar的值，作为分组名
            String writerResourceMark = aTaskConfig.getString(CoreConstant.JOB_WRITER_PARAMETER + "." + CommonConstant.LOAD_BALANCE_RESOURCE_MARK);
            if (writerResourceMarkAndTaskIdMap.get(writerResourceMark) == null) {
                writerResourceMarkAndTaskIdMap.put(writerResourceMark, new LinkedList<Integer>());
            }
            // 把 writerResourceMark 加到 writerResourceMarkAndTaskIdMap 中
            writerResourceMarkAndTaskIdMap.get(writerResourceMark).add(taskId);
        }

        // 选出reader和writer其中最大的
        if (readerResourceMarkAndTaskIdMap.size() >= writerResourceMarkAndTaskIdMap.size()) {
            // 采用 reader 对资源做的标记进行 shuffle
            return readerResourceMarkAndTaskIdMap;
        } else {
            // 采用 writer 对资源做的标记进行 shuffle
            return writerResourceMarkAndTaskIdMap;
        }
    }
```

#### 任务分配 ####

这里举个实例：
taskGroupNumber为4

resourceMarkAndTaskIdMap的数据

```json
{
    "database_a" : [task_id_1, task_id_2],
    "database_b" : [task_id_3, task_id_4, task_id_5],
    "database_c" : [task_id_6, task_id_7]
}
```
执行过程是，轮询database_a, database_b, database_c，取出第一个。循环上一步

1. 取出task_id_1 放入 taskGroup_1
2. 取出task_id_3 放入 taskGroup_2
3. 取出task_id_6 放入 taskGroup_3
4. 取出task_id_2 放入 taskGroup_4
5. .........

最后返回的结果为
```json
{
    "taskGroup_1": [task_id_1, task_id_4],
    "taskGroup_2": [task_id_3, task_id_7],
    "taskGroup_3": [task_id_6, task_id_5],
    "taskGroup_4": [task_id_2]
}
```


```java
    private static List<Configuration> doAssign(LinkedHashMap<String, List<Integer>> resourceMarkAndTaskIdMap, Configuration jobConfiguration, int taskGroupNumber) {
        List<Configuration> contentConfig = jobConfiguration.getListConfiguration(CoreConstant.DATAX_JOB_CONTENT);

        Configuration taskGroupTemplate = jobConfiguration.clone();
        taskGroupTemplate.remove(CoreConstant.DATAX_JOB_CONTENT);

        List<Configuration> result = new LinkedList<Configuration>();

        // 初始化taskGroupConfigList
        List<List<Configuration>> taskGroupConfigList = new ArrayList<List<Configuration>>(taskGroupNumber);
        for (int i = 0; i < taskGroupNumber; i++) {
            taskGroupConfigList.add(new LinkedList<Configuration>());
        }

        // 取得resourceMarkAndTaskIdMap的值的最大个数
        int mapValueMaxLength = -1;

        List<String> resourceMarks = new ArrayList<String>();
        for (Map.Entry<String, List<Integer>> entry : resourceMarkAndTaskIdMap.entrySet()) {
            resourceMarks.add(entry.getKey());
            if (entry.getValue().size() > mapValueMaxLength) {
                mapValueMaxLength = entry.getValue().size();
            }
        }

        
        int taskGroupIndex = 0;
        // 执行mapValueMaxLength次数，每一次轮询一遍resourceMarkAndTaskIdMap
        for (int i = 0; i < mapValueMaxLength; i++) {
            // 轮询resourceMarkAndTaskIdMap
            for (String resourceMark : resourceMarks) {
                if (resourceMarkAndTaskIdMap.get(resourceMark).size() > 0) {
                    // 取出第一个
                    int taskId = resourceMarkAndTaskIdMap.get(resourceMark).get(0);
                    // 轮询的向taskGroupConfigList插入值
                    taskGroupConfigList.get(taskGroupIndex % taskGroupNumber).add(contentConfig.get(taskId));
                    // taskGroupIndex自增
                    taskGroupIndex++;
                    // 删除第一个
                    resourceMarkAndTaskIdMap.get(resourceMark).remove(0);
                }
            }
        }

        Configuration tempTaskGroupConfig;
        for (int i = 0; i < taskGroupNumber; i++) {
            tempTaskGroupConfig = taskGroupTemplate.clone();
            // 设置TaskGroup的配置
            tempTaskGroupConfig.set(CoreConstant.DATAX_JOB_CONTENT, taskGroupConfigList.get(i));
            tempTaskGroupConfig.set(CoreConstant.DATAX_CORE_CONTAINER_TASKGROUP_ID, i);

            result.add(tempTaskGroupConfig);
        }
        // 返回结果
        return result;
    }
```

#### 分配channel ####
分配能够保证尽量平均的分配Channel。算法原理是，当channel总的数目，不能整除TaskGroup的数目时。多的余数个channel，从中挑选出余数个TaskGroup，每个多分配一个。

```java
    private static void adjustChannelNumPerTaskGroup(List<Configuration> taskGroupConfig, int channelNumber) {
        int taskGroupNumber = taskGroupConfig.size();
        int avgChannelsPerTaskGroup = channelNumber / taskGroupNumber;
        int remainderChannelCount = channelNumber % taskGroupNumber;
        // 表示有 remainderChannelCount 个 taskGroup,其对应 Channel 个数应该为：avgChannelsPerTaskGroup + 1；
        // （taskGroupNumber - remainderChannelCount）个 taskGroup,其对应 Channel 个数应该为：avgChannelsPerTaskGroup

        int i = 0;
        for (; i < remainderChannelCount; i++) {
            taskGroupConfig.get(i).set(CoreConstant.DATAX_CORE_CONTAINER_TASKGROUP_CHANNEL, avgChannelsPerTaskGroup + 1);
        }

        for (int j = 0; j < taskGroupNumber - remainderChannelCount; j++) {
            taskGroupConfig.get(i + j).set(CoreConstant.DATAX_CORE_CONTAINER_TASKGROUP_CHANNEL, avgChannelsPerTaskGroup);
        }
    }
```