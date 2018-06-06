# Datax 信息传递

datax，communication

---

### Communication ###

DataX所有的状态及统计信息交互类，job、taskGroup、task等的消息汇报都会使用Communication类。
它的本质是提供了有锁的访问Map数据。下面是它主要的属性
```java
public class Communication extends BaseObject implements Cloneable {
    /**
     * 数字的Map *
     */
    private Map<String, Number> counter;

    /**
     * 运行状态 *
     */
    private State state;

    /**
     * 异常记录 *
     */
    private Throwable throwable;

    /**
     * 记录的timestamp *
     */
    private long timestamp;

    /**
     * 信息的Map *
     */
    Map<String, List<String>> message;
```

