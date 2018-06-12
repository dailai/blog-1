### TaskMonitor ###
TaskMonitor负责监控task的状态，及时的发现task运行是否出现问题。

TaskMonitor使用了单例模式
```java
public class TaskMonitor {
    private static final TaskMonitor instance = new TaskMonitor();
    
    private TaskMonitor() {
    }

    public static TaskMonitor getInstance() {
        return instance;
    }
```

### TaskCommunication ###
TaskCommunication是实际负责Task的监控，TaskMonitor是管理TaskCommunication的。TaskCommunication的监控原理，是通过监测Reader读取的记录数，如果一直没有增加，则认为task失败。
```java
public static class TaskCommunication {
    // 过期时间， 48小时
    private static long EXPIRED_TIME = 172800 * 1000;

    //记录最后更新的记录数
    private long lastAllReadRecords = -1;
    //只有第一次，或者统计变更时才会更新TS
    private long lastUpdateComunicationTS;

    public void report(Communication communication) {

        ttl = System.currentTimeMillis();
        
        // 判断读取的记录数是有有增长
        if (CommunicationTool.getTotalReadRecords(communication) > lastAllReadRecords) {
            // 如果有变化，则更新字记录数
            lastAllReadRecords = CommunicationTool.getTotalReadRecords(communication);
            lastUpdateComunicationTS = ttl;
        } else if (isExpired(lastUpdateComunicationTS)) {
            // 如果超过了 EXPIRED_TIME 时间， 记录数一直没增长，则认为任务失败。
            communication.setState(State.FAILED);
            communication.setTimestamp(ttl);
            communication.setThrowable(DataXException.asDataXException(CommonErrorCode.TASK_HUNG_EXPIRED,
                    String.format("task(%s) hung expired [allReadRecord(%s), elased(%s)]", taskid, lastAllReadRecords, (ttl - lastUpdateComunicationTS))));
        }


    }

    private boolean isExpired(long lastUpdateComunicationTS) {
        return System.currentTimeMillis() - lastUpdateComunicationTS > EXPIRED_TIME;
    }
```

### TaskMonitor原理 ###
TaskMonitor使用ConcurrentHashMap保存task信息

```java
public class TaskMonitor {
    // Key为task id, value为TaskCommunication
    private ConcurrentHashMap<Integer, TaskCommunication> tasks = new ConcurrentHashMap<Integer, TaskCommunication>();

    /**
    * 登记task
    */
    public void registerTask(Integer taskid, Communication communication) {
        //如果task已经finish，直接返回
        if (communication.isFinished()) {
            return;
        }
        tasks.putIfAbsent(taskid, new TaskCommunication(taskid, communication));
    }

    /**
    * 调用report方法，检查task
    */
    public void report(Integer taskid, Communication communication) {
        //如果task已经finish，直接返回
        if (communication.isFinished()) {
            return;
        }
        if (!tasks.containsKey(taskid)) {
            LOG.warn("unexpected: taskid({}) missed.", taskid);
            tasks.putIfAbsent(taskid, new TaskCommunication(taskid, communication));
        } else {
            tasks.get(taskid).report(communication);
        }
    }
```