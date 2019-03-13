# Kafka Consumer 分区分配算法 #



分配算法由PartitionAssignor接口表示

```java
public interface PartitionAssignor {
    // 执行分区分配
    // metadata为元数据，会使用它获取每个topic的分区分布情况
    // subscriptions包含了每个consumer的订阅信息， key为 member_id， value为该consumer的Subscription数据
    Map<String, Assignment> assign(Cluster metadata, Map<String, Subscription> subscriptions);
    
    // 返回该分配算法的名称
    String name();
    
}
```



上面的输入参数涉及到Subscription类，Subscription包含了订阅的topic列表，和用户上传的自定义数据。

返回结果涉及到Assignment类，Assignment包含了分配的topic partition 列表，和用户上传的自定义数据。



目前consumer有三种分配算法。这三个算法都继承了AbstractPartitionAssignor，AbstractPartitionAssignor提供了公共的部分，根据元数据生成每个topic的分区数目表。

```java
public abstract class AbstractPartitionAssignor implements PartitionAssignor {

    @Override
    public Map<String, Assignment> assign(Cluster metadata, Map<String, Subscription> subscriptions) {
        // 获取涉及到订阅的所有 topic
        Set<String> allSubscribedTopics = new HashSet<>();
        // 遍历每个consumer的订阅信息
        for (Map.Entry<String, Subscription> subscriptionEntry : subscriptions.entrySet())
            // 将订阅的topoc列表，添加到set集合里
            allSubscribedTopics.addAll(subscriptionEntry.getValue().topics());
        // 遍历订阅的topic，获取其分区数目，保存到HashMap里
        Map<String, Integer> partitionsPerTopic = new HashMap<>();
        for (String topic : allSubscribedTopics) {
            Integer numPartitions = metadata.partitionCountForTopic(topic);
            if (numPartitions != null && numPartitions > 0)
                partitionsPerTopic.put(topic, numPartitions);
            else
                log.debug("Skipping assignment for topic {} since no metadata is available", topic);
        }
        // 子类需要实现assign方法，执行分配
        // 结果保存在Map，key为consumer的 id， value为分配的分区列表
        Map<String, List<TopicPartition>> rawAssignments = assign(partitionsPerTopic, subscriptions);
        
        // 将分区列表转换为Assignment
        Map<String, Assignment> assignments = new HashMap<>();
        for (Map.Entry<String, List<TopicPartition>> assignmentEntry : rawAssignments.entrySet())
            assignments.put(assignmentEntry.getKey(), new Assignment(assignmentEntry.getValue()));
        
        return assignments;
    }
    
    // 子类需要实现次方法，执行分配
    public abstract Map<String, List<TopicPartition>> assign(Map<String, Integer> partitionsPerTopic, Map<String, Subscription> subscriptions);    
    
}
```





RangeAssignor算法

首先找出每个topic被哪些consumer订阅，然后根据该topic的分区数，平均分配

以下列为例，topic_a有5个分区，有3个consumer订阅了topic_a。策略执行如下：

1. 首先确定每个consumer至少分配多少个分区， 这里为 5 / 3 = 1 个
2. 计算剩余的分区数目，这里为 5 % 3 = 2

排在前面的consumer分配的分区数为 平均数加1，比如consumer_0和consumer_1的分区数为2

排在后面的consumer分配的分区数为 平均数，比如consumer_2的分区数为1



```java
public class RangeAssignor extends AbstractPartitionAssignor {
    
    // 返回分配算法的名称
    public String name() {
        return "range";
    }
    
    // 返回的结果，key为 consumer id， value为分配的分区列表
    public Map<String, List<TopicPartition>> assign(Map<String, Integer> partitionsPerTopic,
                                                    Map<String, Subscription> subscriptions) {
        // 计算每个topic，被哪些consumer订阅
        // key为 topic名称， value为 consumer列表
        Map<String, List<String>> consumersPerTopic = consumersPerTopic(subscriptions);
        
        // 初始化结果值assignment， key为 consumer id， value为分配的分区列表
        Map<String, List<TopicPartition>> assignment = new HashMap<>();
        for (String memberId : subscriptions.keySet())
            assignment.put(memberId, new ArrayList<TopicPartition>());

        for (Map.Entry<String, List<String>> topicEntry : consumersPerTopic.entrySet()) {
            // 获取要分配的topic
            String topic = topicEntry.getKey();
            // 获取consumer列表
            List<String> consumersForTopic = topicEntry.getValue();
            // 获取该topic的分区数
            Integer numPartitionsForTopic = partitionsPerTopic.get(topic);
            if (numPartitionsForTopic == null)
                continue;
            // 排序consumer列表
            Collections.sort(consumersForTopic);
            // 计算每个consumer分配的最小分区数
            int numPartitionsPerConsumer = numPartitionsForTopic / consumersForTopic.size();
            // 计算多余的分区数，这些分区数都会添加到前面的consumer
            int consumersWithExtraPartition = numPartitionsForTopic % consumersForTopic.size();
            // 生成TopicPartition列表
            List<TopicPartition> partitions = AbstractPartitionAssignor.partitions(topic, numPartitionsForTopic);
            for (int i = 0, n = consumersForTopic.size(); i < n; i++) {
                // 计算在TopicPartition列表中分配的起始位置
                // 当consumer的位置小于剩余分区数，这里的起始位置为 2 * i * numPartitionsPerConsumer
                // 否则，起始位置为 numPartitionsPerConsumer * i + consumersWithExtraPartition
                int start = numPartitionsPerConsumer * i + Math.min(i, consumersWithExtraPartition);
                // 排在前面的consumer，分配的分区数为 numPartitionsPerConsumer + 1
                // 排在后面的consumer，分配的分区数为 numPartitionsPerConsumer
                int length = numPartitionsPerConsumer + (i + 1 > consumersWithExtraPartition ? 0 : 1);
                // 将结果保存到assignment集合
                assignment.get(consumersForTopic.get(i)).addAll(partitions.subList(start, start + length));
            }
        }
        return assignment;
    }

}    
```



 







