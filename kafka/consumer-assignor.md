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

目前currentPartitionConsumer包含了currentAssignment的partition到consumer的对应表



将unassignedPartitions的partition分配到consumer，结果保存在currentAssignment集合里



找到那些有多个consumer可以订阅的partition列表，然后将这些 partition 从 sortedPartitions 集合中去除掉

找到那些不能参与 partition balance 的 consumer。需要满足以下条件：如果该consumer的分配的partition数目 小于 可以订阅的partition数目 。或者它所分配的partition 有多个consumer 订阅

这些不能参与 balance 的 partition 保存在 fixedAssignments 集合里。而且还需要把sortedCurrentSubscriptions的这些consumer删除掉





  isBalanced函数判断当前分区的分配情况是否平衡

```java
/*
  currentAssignment：当前consumer的分配partition情况
  sortedCurrentSubscriptions：排序的consumer列表，按照分配的partition数目规则排序
  allSubscriptions：consumer可以订阅的 partition 对应表
*/

private boolean isBalanced(Map<String, List<TopicPartition>> currentAssignment,
                           TreeSet<String> sortedCurrentSubscriptions,
                           Map<String, List<TopicPartition>> allSubscriptions) {
    int min = currentAssignment.get(sortedCurrentSubscriptions.first()).size();
    int max = currentAssignment.get(sortedCurrentSubscriptions.last()).size();
    if (min >= max - 1)
        // 当consumer分配的分区数，最大数和最小数相差不大于 1，则认为已经平衡
        return true;

    // 根据当前的分区分配情况，创建 partition 到 consumer 的对应表
    final Map<TopicPartition, String> allPartitions = new HashMap<>();
    // 遍历当前分布情况的 currentAssignment 集合 
    Set<Entry<String, List<TopicPartition>>> assignments = currentAssignment.entrySet();
    for (Map.Entry<String, List<TopicPartition>> entry: assignments) {
        List<TopicPartition> topicPartitions = entry.getValue();
        for (TopicPartition topicPartition: topicPartitions) {
            if (allPartitions.containsKey(topicPartition))
                log.error(topicPartition + " is assigned to more than one consumer.");
            // 将此partition 和 consumer 的记录，添加到 allPartitions 集合
            allPartitions.put(topicPartition, entry.getKey());
        }
    }

    // 遍历 consumer，按照从小到大的顺序遍历
    for (String consumer: sortedCurrentSubscriptions) {
        List<TopicPartition> consumerPartitions = currentAssignment.get(consumer);
        int consumerPartitionCount = consumerPartitions.size();

        // 如果该 consumer 已经分配的分区数，等于 可以分配分区的最大数，那么认为该consumer不能接收多的partition。因为这是按照从小到大的顺序遍历consumer，后面的consumer已经分配的分区数肯定会大于当前consumer。为了平衡，所以也不可能减少该consumer的partition数，否则这样更加破坏了平衡
        if (consumerPartitionCount == allSubscriptions.get(consumer).size())
            continue;

        // 获取当前consumer可以分配的最大分区数
        List<TopicPartition> potentialTopicPartitions = allSubscriptions.get(consumer);
        // 遍历可以分配的所有分区，尝试将心分区分配给当前consumer
        for (TopicPartition topicPartition: potentialTopicPartitions) {
            // 如果当前consumer还没有分配到此分区，那么尝试将此分区移动到当前consumer
            if (!currentAssignment.get(consumer).contains(topicPartition)) {
                // 找到该partition 现在分配给哪个consumer
                String otherConsumer = allPartitions.get(topicPartition);
                // 这里只有当别的consumer分配的分区多，才会移动partition
                int otherConsumerPartitionCount = currentAssignment.get(otherConsumer).size();
                if (consumerPartitionCount < otherConsumerPartitionCount) {
                    // 返回false，表示还可以进一步平衡
                    return false;
                }
            }
        }
    }
    // 返回true，表示无法进一步改善平衡了
    return true;
}
```





performReassignments方法实现重新分配

```java
private boolean performReassignments(List<TopicPartition> reassignablePartitions,
                                     Map<String, List<TopicPartition>> currentAssignment,
                                     TreeSet<String> sortedCurrentSubscriptions,
                                     Map<String, List<TopicPartition>> consumer2AllPotentialPartitions,
                                     Map<TopicPartition, List<String>> partition2AllPotentialConsumers,
                                     Map<TopicPartition, String> currentPartitionConsumer) {
    boolean reassignmentPerformed = false;
    boolean modified;

    // repeat reassignment until no partition can be moved to improve the balance
    do {
        modified = false;
        // 遍历可以重新分配的partition
        Iterator<TopicPartition> partitionIterator = reassignablePartitions.iterator();
        
        // 如果遍历完partition或者分配情况达到平衡
        while (partitionIterator.hasNext() && !isBalanced(currentAssignment, sortedCurrentSubscriptions, consumer2AllPotentialPartitions)) {
            
            TopicPartition partition = partitionIterator.next();

            // the partition must have at least two consumers
            if (partition2AllPotentialConsumers.get(partition).size() <= 1)
                log.error("Expected more than one potential consumer for partition '" + partition + "'");

            // 找到该partition现在分配给哪个consumer
            String consumer = currentPartitionConsumer.get(partition);
            if (consumer == null)
                log.error("Expected partition '" + partition + "' to be assigned to a consumer");

            // 遍历可以该 partition 可以分配的consumer
            for (String otherConsumer: partition2AllPotentialConsumers.get(partition)) {
                // 如果其他consumer分配的分区数 小于 该 consumer分配的分区数，并且插值至少为2
                // 那么将此partition重新分配
                if (currentAssignment.get(consumer).size() > currentAssignment.get(otherConsumer).size() + 1) {
                    // 重新分配该partition
                    reassignPartition(partition, currentAssignment, sortedCurrentSubscriptions, currentPartitionConsumer, consumer2AllPotentialPartitions);
                    reassignmentPerformed = true;
                    modified = true;
                    break;
                }
            }
        }
    } while (modified);

    return reassignmentPerformed;
}
```



reassignPartition方法负责重新分配单个partition，它尽量将partition分配给，分区数目最小的那个consumer。

```java
private void reassignPartition(TopicPartition partition,
                               Map<String, List<TopicPartition>> currentAssignment,
                               TreeSet<String> sortedCurrentSubscriptions,
                               Map<TopicPartition, String> currentPartitionConsumer,
                               Map<String, List<TopicPartition>> consumer2AllPotentialPartitions) {
    String consumer = currentPartitionConsumer.get(partition);
    String newConsumer = null;
    // 按照分区数从小到大的遍历consumer，如果遇到consumer可以订阅该partition，那么就移动partition
    for (String anotherConsumer: sortedCurrentSubscriptions) {
        if (consumer2AllPotentialPartitions.get(anotherConsumer).contains(partition)) {
            newConsumer = anotherConsumer;
            break;
        }
    }

    assert newConsumer != null;

    // find the correct partition movement considering the stickiness requirement
    TopicPartition partitionToBeMoved = partitionMovements.getTheActualPartitionToBeMoved(partition, consumer, newConsumer);
    processPartitionMovement(partitionToBeMoved, newConsumer, currentAssignment, sortedCurrentSubscriptions, currentPartitionConsumer);

    return;
}
```





balance平衡



```java
private void balance(Map<String, List<TopicPartition>> currentAssignment,
                     List<TopicPartition> sortedPartitions,
                     List<TopicPartition> unassignedPartitions,
                     TreeSet<String> sortedCurrentSubscriptions,
                     Map<String, List<TopicPartition>> consumer2AllPotentialPartitions,
                     Map<TopicPartition, List<String>> partition2AllPotentialConsumers,
                     Map<TopicPartition, String> currentPartitionConsumer) {
    boolean initializing = currentAssignment.get(sortedCurrentSubscriptions.last()).isEmpty();
    boolean reassignmentPerformed = false;

    // 遍历未分配的partition
    for (TopicPartition partition: unassignedPartitions) {
        if (partition2AllPotentialConsumers.get(partition).isEmpty())
            continue;
        // 将partition尽量分配给分区数目小的consumer
        assignPartition(partition, sortedCurrentSubscriptions, currentAssignment,
                        consumer2AllPotentialPartitions, currentPartitionConsumer);
    }
    
    // 目前所有的partition都已经分配完了，结果保存在currentAssignment集合里

    // 找到那些只能被一个consumer订阅的partition，这样的partition是不能重新分配的
    Set<TopicPartition> fixedPartitions = new HashSet<>();
    for (TopicPartition partition: partition2AllPotentialConsumers.keySet())
        // canParticipateInReassignment方法，判断partition是否被多个consumer订阅
        if (!canParticipateInReassignment(partition, partition2AllPotentialConsumers))
            fixedPartitions.add(partition);
    // 从sortedPartitions删除掉不能重新分配的 partition
    sortedPartitions.removeAll(fixedPartitions);
    
    // 目前sortedPartitions列表只包含了可以重新分配的partition
    
    Map<String, List<TopicPartition>> fixedAssignments = new HashMap<>();
    // 遍历consumer
    for (String consumer: consumer2AllPotentialPartitions.keySet())
        // 找到那些不能参与重新分配的consumer，不能参与重新分配的条件是该consumer已经分配的分区数达到可以分配的最大值，并且所有的分区只有这么一个consumer可以订阅
        if (!canParticipateInReassignment(consumer, currentAssignment,
                                          consumer2AllPotentialPartitions, partition2AllPotentialConsumers)) {
            // 从sortedCurrentSubscriptions中删除掉这些consumer
            sortedCurrentSubscriptions.remove(consumer);
            // 从currentAssignment删除掉这些consumer的分配信息，这样避免影响到重新分区
            fixedAssignments.put(consumer, currentAssignment.remove(consumer));
        }

    // 目前sortedCurrentSubscriptions列表只包含了可以参与重新分配的partition
   
    Map<String, List<TopicPartition>> preBalanceAssignment = deepCopy(currentAssignment);
    Map<TopicPartition, String> preBalancePartitionConsumers = new HashMap<>(currentPartitionConsumer);
    // 执行重新分配，尽量保证分区的平衡
    reassignmentPerformed = performReassignments(sortedPartitions, currentAssignment, sortedCurrentSubscriptions,
            consumer2AllPotentialPartitions, partition2AllPotentialConsumers, currentPartitionConsumer);


    if (!initializing && reassignmentPerformed && getBalanceScore(currentAssignment) >= getBalanceScore(preBalanceAssignment)) {
        deepCopy(preBalanceAssignment, currentAssignment);
        currentPartitionConsumer.clear();
        currentPartitionConsumer.putAll(preBalancePartitionConsumers);
    }

    // 将没有参与重新分配的consumer，添加到sortedCurrentSubscriptions列表里
    // 将这些consumer的分区分配情况，添加currentAssignment集合里
    for (Entry<String, List<TopicPartition>> entry: fixedAssignments.entrySet()) {
        String consumer = entry.getKey();
        currentAssignment.put(consumer, entry.getValue());
        sortedCurrentSubscriptions.add(consumer);
    }

    fixedAssignments.clear();
}
```