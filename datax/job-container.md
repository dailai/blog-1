# Datax 任务执行流程 #



## 加载配置 ##

Datax启动是从Engine类开始的。Engine会读取配置文件，并且初始化JobContainer。

```java
public class Engine {
	public static void entry(final String[] args) throws Throwable {
    // .....
	Configuration configuration = ConfigParser.parse(jobPath);
	Engine engine = new Engine();
	engine.start(configuration);
	}
    
    public void start(Configuration allConf) {
        // ......
        container = new JobContainer(allConf);
        container.start();
    }
}
```



## 运行JobContainer ##

JobContaier的源码涉及到插件的加载，可以参考此篇博客。

JobContainer的start方法如下

```java
public class JobContainer extends AbstractContainer {
    public void start() {
        LOG.debug("jobContainer starts to do preHandle ...");
        this.preHandle();
        LOG.debug("jobContainer starts to do init ...");
        this.init();
        LOG.info("jobContainer starts to do prepare ...");
        this.prepare();
        LOG.info("jobContainer starts to do split ...");
        this.totalStage = this.split();
        LOG.info("jobContainer starts to do schedule ...");
        this.schedule();
        LOG.debug("jobContainer starts to do post ...");
        this.post();
        
        LOG.debug("jobContainer starts to do postHandle ...");
        this.postHandle();
        LOG.info("DataX jobId [{}] completed successfully.", this.jobId);
    }
}
        
```



### 加载 和运行 Handler的初始化函数 ###

这里会根据配置，加载指定的Handler类，并且执行它的preHandler回调函数。相关的配置如下：

* job.preHandler.pluginType， 插件类型
* job.preHandler.pluginName， 插件名称

```java
private void preHandle() {
    // 获取preHandler的类型，由job.preHandler.pluginType指定
    String handlerPluginTypeStr = this.configuration.getString(
            CoreConstant.DATAX_JOB_PREHANDLER_PLUGINTYPE);
    // 实例化PluginType
    PluginType handlerPluginType = PluginType.valueOf(handlerPluginTypeStr.toUpperCase());
	// 获取preHandler的名称
    String handlerPluginName = this.configuration.getString(
            CoreConstant.DATAX_JOB_PREHANDLER_PLUGINNAME);
    // 切换类加载器
    classLoaderSwapper.setCurrentThreadClassLoader(LoadUtil.getJarLoader(
            handlerPluginType, handlerPluginName));
	// 加载 Handler
    AbstractJobPlugin handler = LoadUtil.loadJobPlugin(
            handlerPluginType, handlerPluginName);
	// 初始化handler的jobPluginCollector， 通过它可以知道任务的详情
    JobPluginCollector jobPluginCollector = new DefaultJobPluginCollector(
            this.getContainerCommunicator());
    handler.setJobPluginCollector(jobPluginCollector);

    //调用handler的preHandler
    handler.preHandler(configuration);
    // 切回类加载器
    classLoaderSwapper.restoreCurrentThreadClassLoader();
}
```



### 加载Reader和Writer

init方法会将加载配置中指定的Reader和Writer，来完成数据的读取和写入。Reader的初始化和Writer相同，这里以Reader为例：

```java
private void init() {
    JobPluginCollector jobPluginCollector = new DefaultJobPluginCollector(this.getContainerCommunicator());
    //必须先Reader ，后Writer
    this.jobReader = this.initJobReader(jobPluginCollector);
    this.jobWriter = this.initJobWriter(jobPluginCollector);
}

// 加载Reader
private Reader.Job initJobReader(JobPluginCollector jobPluginCollector) {
    // 读取 Reader的名称
    this.readerPluginName = this.configuration.getString(
        CoreConstant.DATAX_JOB_CONTENT_READER_NAME);
    // 切换类加载器
    classLoaderSwapper.setCurrentThreadClassLoader(LoadUtil.getJarLoader(
        PluginType.READER, this.readerPluginName));
    // 加载Reader插件
    Reader.Job jobReader = (Reader.Job) LoadUtil.loadJobPlugin(
        PluginType.READER, this.readerPluginName);
    // 更新Reader的配置
    jobReader.setPluginJobConf(this.configuration.getConfiguration(
        CoreConstant.DATAX_JOB_CONTENT_READER_PARAMETER));
    jobReader.setPeerPluginJobConf(this.configuration.getConfiguration(
        CoreConstant.DATAX_JOB_CONTENT_WRITER_PARAMETER));
    // 初始化Reader的jobPluginCollector
    jobReader.setJobPluginCollector(jobPluginCollector);
    // 调用Reader的init回调函数
    jobReader.init();
    // 切回类加载器
    classLoaderSwapper.restoreCurrentThreadClassLoader();
    return jobReader;
}
    
```



### Reader和Writer的初始化回调函数 ###

prepare方法会执行Reader和Writer的prepare回调函数。Reader和Writer相同，下面以Reader为例：

```java
private void prepare() {
	this.prepareJobReader();
    this.prepareJobWriter();
}
    
private void prepareJobReader() {
    // 切换类加载器
    classLoaderSwapper.setCurrentThreadClassLoader(LoadUtil.getJarLoader(
            PluginType.READER, this.readerPluginName));
    LOG.info(String.format("DataX Reader.Job [%s] do prepare work .",
            this.readerPluginName));
    // 调用jobReader的prepare方法
    this.jobReader.prepare();
    // 切回类加载器
    classLoaderSwapper.restoreCurrentThreadClassLoader();
}
```



### 切分任务 ###

split方法会根据channel的数目，将整个job任务，划分成多个小的task。具体原理参见这篇博客。



### 分配和执行任务 ###

schedule方法会将task发送给各个Channel执行。具体原理参见这篇博客。



### 调用Reader和Writer的完成回调函数

post会执行Reader和Writer的post回调函数。Reader和Writer相同，下面以Reader为例：

```java
private void post() {
    this.postJobWriter();
    this.postJobReader();
}

private void postJobReader() {
	// 切换类加载器
	classLoaderSwapper.setCurrentThreadClassLoader(LoadUtil.getJarLoader(
		PluginType.READER, this.readerPluginName));
    LOG.info("DataX Reader.Job [{}] do post work.", this.readerPluginName);
    // 调用jobReader的post方法
    this.jobReader.post();
    // 切回类加载器
    classLoaderSwapper.restoreCurrentThreadClassLoader();
}
```



### 加载 和运行 Handler的完成函数 ###

这里会根据配置，加载指定的Handler类，并且执行它的postHandler回调函数。相关的配置如下：

- job.postHandler.pluginType， 插件类型
- job.postHandler.pluginName， 插件名称

```java
private void postHandle() {
    // 获取插件类型
	String handlerPluginTypeStr = this.configuration.getString(
		CoreConstant.DATAX_JOB_POSTHANDLER_PLUGINTYPE);
    PluginType handlerPluginType = PluginType.valueOf(handlerPluginTypeStr.toUpperCase());
    
    // 获取插件名称
    String handlerPluginName = this.configuration.getString(
        CoreConstant.DATAX_JOB_POSTHANDLER_PLUGINNAME);
    // 切换类加载器
    classLoaderSwapper.setCurrentThreadClassLoader(LoadUtil.getJarLoader(
        handlerPluginType, handlerPluginName));
    // 加载插件
    AbstractJobPlugin handler = LoadUtil.loadJobPlugin(
        handlerPluginType, handlerPluginName);
    
    // 配置jobPluginCollector， 通过它可以得到任务执行的详情
    JobPluginCollector jobPluginCollector = new DefaultJobPluginCollector(
        this.getContainerCommunicator());
    handler.setJobPluginCollector(jobPluginCollector);
    
    // 调用handler的postHandler函数
    handler.postHandler(configuration);
    // 切回类加载器
    classLoaderSwapper.restoreCurrentThreadClassLoader();
```



## 扩展插件 ##

如果有个需求，需要将任务的完成情况，记录下来。这个时候需要自定义handler。