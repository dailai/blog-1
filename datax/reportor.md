 ### reporter ###
 reporter用来向上汇报数(保存在Communication里)。datax的类设计是由一个抽象类AbstractReporter和一个子类ProcessInnerReporter实现。目前只有汇报TaskGroup数据的接口，可以使用。

 ```java
 public abstract class AbstractReporter {

    public abstract void reportJobCommunication(Long jobId, Communication communication);

    public abstract void reportTGCommunication(Integer taskGroupId, Communication communication);

}
 ```

 ```java
 public class ProcessInnerReporter extends AbstractReporter {

    @Override
    public void reportJobCommunication(Long jobId, Communication communication) {
        // do nothing
    }

    @Override
    public void reportTGCommunication(Integer taskGroupId, Communication communication) {
        LocalTGCommunicationManager.updateTaskGroupCommunication(taskGroupId, communication);
    }
}
 ```
 reportTGCommunication接口，是调用了LocalTGCommunicationManager的方法，更新数据。