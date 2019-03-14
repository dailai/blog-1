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
    
    // 根据订阅的topic列表，实例化Subscription，可以添加自定义的数据
    Subscription subscription(Set<String> topics);
    
}
```



上面的输入参数涉及到Subscription类，Subscription包含了订阅的topic列表，和用户上传的自定义数据。

返回结果涉及到Assignment类，Assignment包含了的分配结果。分配结果包含了该consumer的 topic partition 列表，和用户上传的自定义数据。



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



 



RoundRobinAssignor算法

首先依次遍历订阅的 topic，将每个 topic 的 partition 列表合成一个大的列表。

然后依次遍历 partition 列表，轮询分配给consumer。

```java
public class RoundRobinAssignor extends AbstractPartitionAssignor {
    
    // 返回分配算法的名称
    public String name() {
        return "roundrobin";
    }

    @Override
    public Map<String, List<TopicPartition>> assign(Map<String, Integer> partitionsPerTopic,
                                                    Map<String, Subscription> subscriptions) {
        // 初始化结果集
        Map<String, List<TopicPartition>> assignment = new HashMap<>();
        for (String memberId : subscriptions.keySet())
            assignment.put(memberId, new ArrayList<TopicPartition>());
        // 这里使用了CircularIterator，它的作用是循环遍历 consumer列表
        CircularIterator<String> assigner = new CircularIterator<>(Utils.sorted(subscriptions.keySet()));
        // 遍历 TopicPartition 列表
        for (TopicPartition partition : allPartitionsSorted(partitionsPerTopic, subscriptions)) {
            final String topic = partition.topic();
            // 查看当前consumer是否订阅了这个topic
            // 一直循环consumer列表，直到找到订阅这个topic的consumer
            while (!subscriptions.get(assigner.peek()).topics().contains(topic))
                assigner.next();
            // 将分区分配给这个consumer
            assignment.get(assigner.next()).add(partition);
        }
        return assignment;
    }

    // 生成 partition 列表
    public List<TopicPartition> allPartitionsSorted(Map<String, Integer> partitionsPerTopic,
                                                    Map<String, Subscription> subscriptions) {
        // 获取所有的订阅topic
        SortedSet<String> topics = new TreeSet<>();
        for (Subscription subscription : subscriptions.values())
            topics.addAll(subscription.topics());
        
        List<TopicPartition> allPartitions = new ArrayList<>();
        for (String topic : topics) {
            // 遍历topic，获取它的分区数
            Integer numPartitionsForTopic = partitionsPerTopic.get(topic);
            if (numPartitionsForTopic != null)
                // 生成 TopicPartition 列表，添加到结果列表中 allPartitions
                allPartitions.addAll(AbstractPartitionAssignor.partitions(topic, numPartitionsForTopic));
        }
        return allPartitions;
    }
}
```



StickyAssignor

StickyAssignor算法涉及到自定义数据，consumer会将上一次的分区分配结果，作为自定义数据，上传到Coordinator。





1. 首先根据每个consumer上传的自定义数据，得到这次分配前的结果，保存了每个consumer订阅的分区列表。
2. 为每个consumer，都生成一个 分区列表。这些分区只属于了它订阅的topic。
3. 为每个分区，都生成一个 consumer 列表。这些consumer只订阅了 该分区所属的 topic。
4. 根据步骤1的结果，计算出每个topic partition 和 consumer 的哈希表
5.  分为两种情况
   1. 第一次分配的情况，将步骤3的结果的分区按照PartitionComparator排序，生成topic partition 列表
   2. 再次分配，并且步骤2和步骤3的结果，是相同的。
6. 步骤3的结果中，去除那些退出的consumer的分配的partition
7. 删除掉没有consumer订阅的partition



输入数据：

Map<String, Integer> partitionsPerTopic， topic 拥有的 partition 数目

Map<String, Subscription> subscriptions， consumer 的订阅 信息

Map<String, List<TopicPartition>> currentAssignment， 上次分配的结果。每个consumer分配的 partition列表

isFreshAssignment， 是否为第一次分配



中间重要字段：

Map<TopicPartition, List<String>> partition2AllPotentialConsumers，根据订阅信息，生成的partition 和 consumer 的对应关系，两种之间有topic的订阅关系

Map<String, List<TopicPartition>>  consumer2AllPotentialPartitions， 根据订阅信息，生成consumer和partition的对应关系，两种之间有topic的订阅关系

Map<TopicPartition, String> currentPartitionConsumer，上次分配的结果，partition 和 consumer的关系



判断partition2AllPotentialConsumers中，每个partition对应的consumer列表，是否相同。

判断consumer2AllPotentialPartitions中，每个consumer对应的partition列表，是否相同。



复制currentAssignment集合，删除那么没有consumer订阅的partition，然后将consumer按照SubscriptionComparator排序。然后一次按照consumer的排序规则遍历，将其对应的partition添加到 sortedPartitions列表。



按照订阅信息，如果一些consumer退出了，那么需要从currentPartitionConsumer，删除掉对应的partition

如果topic没有被consumer订阅，那么currentPartitionConsumer删除掉对应的partition。还需要从currentAssignment删除。

如果topic不在被之前的consumer订阅，那么需要从currentAssignment删除。

否则从unassignedPartitions删除掉 partition





SubscriptionComparator 排序规则，先按照consumer订阅的partition的数目拍戏，然后按照consumer的字符串排序。

PartitionComparator 排序 规则， 先按照 所属 topic 被 consumer订阅的数目排序，之后按照 topic 的字符串排序，最后按照分数索引排序。



```java
private void balance(Map<String, List<TopicPartition>> currentAssignment,
                     List<TopicPartition> sortedPartitions,
                     List<TopicPartition> unassignedPartitions,
                     TreeSet<String> sortedCurrentSubscriptions,
                     Map<String, List<TopicPartition>> consumer2AllPotentialPartitions,
                     Map<TopicPartition, List<String>> partition2AllPotentialConsumers,
                     Map<TopicPartition, String> currentPartitionConsumer)
```



currentAssignment： 在上次分配结果之上，根据这次的订阅信息，减少那些不在使用的consumer和partition

unassignedPartitions： 在上次分配结果之上，保留那些需要重新分配的partition

sortedCurrentSubscriptions：consumer列表

consumer2AllPotentialPartitions：

partition2AllPotentialConsumers：

currentPartitionConsumer：partition到consumer的对应表



如果某个consumer加入，



生成currentAssignment集合，如果该consumer之前分配过，会保存之前的订阅的partition。如果之前没有分配过，则创建空的列表。

根据新的订阅信息，生成partition 到 consumer的对应表，和consumer到 partition的对应表。

根据currentAssignment集合，生成partition 到 consumer的对应表。



生成排序后的partition列表：

 如果不是第一次分配，并且每个consumer订阅的topic都是一样。

1. 复制currentAssignment集合，保存到临时集合assignments里。 如果以前的partition，现在没有被consumer订阅，那么将其从assignments中删掉。
2. 将所有订阅的consumer 排序，按照之前分配的分区数，排序
3. 然后按照从大到小的顺寻，遍历consumer。依次将consumer之前分配的分区，添加到列表sortedPartitions中
4. 因为sortedPartitions只包含了之前的分区，如果有consumer订阅了新的topic，那么需要将新的topic的partition添加到sortedPartitions列表中

否则

   将订阅的partition，按照被订阅的consumer数目排序，保存到sortedPartitions



sortedPartitions只包含了即将要分配的partition。如果一个partition在上次分配中存在，但这次没有，则不会保存到sortedPartitions集合里。



目前currentAssignment集合，既包含了以前的consumer，也包含了新的consumer。





复制sortedPartitions列表，保存到临时列表unassignedPartitions。

如果一些consumer退出了，那么需要从currentPartitionConsumer，删除掉对应的partition。还需要从currentAssignment删除掉旧的consumer。

如果该consumer不再订阅该topic，那么currentPartitionConsumer删除掉topic对应的partition。还需要从currentAssignment删除。

如果topic不在被之前的consumer订阅，那么需要从currentAssignment删除。

否则从unassignedPartitions删除掉 partition。





目前currentAssignment集合，只包含了订阅的consumer，而且只包含了consumer订阅的有效partition分区。

目前unassignedPartitions集合，包含了需要分配的分区

目前currentPartitionConsumer

