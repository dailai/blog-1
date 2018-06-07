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

Communication的方法很简单，使用synchronized同步访问。比如下面的访问counter和message
```java
    public synchronized Long getLongCounter(final String key) {
        Number value = this.counter.get(key);

        return value == null ? 0 : value.longValue();
    }

    public synchronized void setLongCounter(final String key, final long value) {
        Validate.isTrue(StringUtils.isNotBlank(key), "设置counter的key不能为空");
        this.counter.put(key, value);
    }

    public synchronized void increaseCounter(final String key, final long deltaValue) {
        Validate.isTrue(StringUtils.isNotBlank(key), "增加counter的key不能为空");

        long value = this.getLongCounter(key);

        this.counter.put(key, value + deltaValue);
    }



    public List<String> getMessage(final String key) {
        return message.get(key);
    }

    public synchronized void addMessage(final String key, final String value) {
        Validate.isTrue(StringUtils.isNotBlank(key), "增加message的key不能为空");
        List valueList = this.message.get(key);
        if (null == valueList) {
            valueList = new ArrayList<String>();
            this.message.put(key, valueList);
        }

        valueList.add(value);
    }
```

Communication的结构很简单，只是提供了同步的方法，来访问一些数据。这些数据用Map来存储。它用来统计信息，提供不同组件的交流。