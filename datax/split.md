## job 切分任务 ##

### JobContainer ###

JobContainer 负责任务划分，生成任务信息配置的列表

```java
    private int split() {
        // 计算所需的channel数目
        this.adjustChannelNumber();

        if (this.needChannelNumber <= 0) {
            this.needChannelNumber = 1;
        }
        // 生成任务的reader配置
        List<Configuration> readerTaskConfigs = this
                .doReaderSplit(this.needChannelNumber);
        int taskNumber = readerTaskConfigs.size();
        // 生成任务的writer配置
        List<Configuration> writerTaskConfigs = this
                .doWriterSplit(taskNumber);

        // 生成任务的transformer配置
        List<Configuration> transformerList = this.configuration.getListConfiguration(CoreConstant.DATAX_JOB_CONTENT_TRANSFORMER);

        LOG.debug("transformer configuration: "+ JSON.toJSONString(transformerList));
        
        // 合并任务的reader，writer，transformer配置
        List<Configuration> contentConfig = mergeReaderAndWriterTaskConfigs(
                readerTaskConfigs, writerTaskConfigs, transformerList);
        LOG.debug("contentConfig configuration: "+ JSON.toJSONString(contentConfig));
        
        // 将配置结果保存在job.content路径下
        this.configuration.set(CoreConstant.DATAX_JOB_CONTENT, contentConfig);

        return contentConfig.size();
    }
```

### Reader ###

```java
    /**
    * adviceNumber, 建议的数目
    */
    private List<Configuration> doReaderSplit(int adviceNumber) {
        // 切换ClassLoader
        classLoaderSwapper.setCurrentThreadClassLoader(LoadUtil.getJarLoader(
                PluginType.READER, this.readerPluginName));
        // 调用Job.Reader的split切分
        List<Configuration> readerSlicesConfigs =
                this.jobReader.split(adviceNumber);
        if (readerSlicesConfigs == null || readerSlicesConfigs.size() <= 0) {
            throw DataXException.asDataXException(
                    FrameworkErrorCode.PLUGIN_SPLIT_ERROR,
                    "reader切分的task数目不能小于等于0");
        }
        LOG.info("DataX Reader.Job [{}] splits to [{}] tasks.",
                this.readerPluginName, readerSlicesConfigs.size());
        classLoaderSwapper.restoreCurrentThreadClassLoader();
        return readerSlicesConfigs;
    }
```

### Writer ###

```java
    private List<Configuration> doWriterSplit(int readerTaskNumber) {
        // 切换ClassLoader
        classLoaderSwapper.setCurrentThreadClassLoader(LoadUtil.getJarLoader(
                PluginType.WRITER, this.writerPluginName));
        // 调用Job.Reader的split切分
        List<Configuration> writerSlicesConfigs = this.jobWriter
                .split(readerTaskNumber);
        if (writerSlicesConfigs == null || writerSlicesConfigs.size() <= 0) {
            throw DataXException.asDataXException(
                    FrameworkErrorCode.PLUGIN_SPLIT_ERROR,
                    "writer切分的task不能小于等于0");
        }
        LOG.info("DataX Writer.Job [{}] splits to [{}] tasks.",
                this.writerPluginName, writerSlicesConfigs.size());
        classLoaderSwapper.restoreCurrentThreadClassLoader();

        return writerSlicesConfigs;
    }
```

### 合并配置 ###

```java
    private List<Configuration> mergeReaderAndWriterTaskConfigs(
            List<Configuration> readerTasksConfigs,
            List<Configuration> writerTasksConfigs,
            List<Configuration> transformerConfigs) {
        // reader切分的任务数目必须等于writer切分的任务数目
        if (readerTasksConfigs.size() != writerTasksConfigs.size()) {
            throw DataXException.asDataXException(
                    FrameworkErrorCode.PLUGIN_SPLIT_ERROR,
                    String.format("reader切分的task数目[%d]不等于writer切分的task数目[%d].",
                            readerTasksConfigs.size(), writerTasksConfigs.size())
            );
        }

        List<Configuration> contentConfigs = new ArrayList<Configuration>();
        for (int i = 0; i < readerTasksConfigs.size(); i++) {
            Configuration taskConfig = Configuration.newDefault();
            // 保存reader相关配置
            taskConfig.set(CoreConstant.JOB_READER_NAME,
                    this.readerPluginName);
            taskConfig.set(CoreConstant.JOB_READER_PARAMETER,
                    readerTasksConfigs.get(i));
            // 保存writer相关配置
            taskConfig.set(CoreConstant.JOB_WRITER_NAME,
                    this.writerPluginName);
            taskConfig.set(CoreConstant.JOB_WRITER_PARAMETER,
                    writerTasksConfigs.get(i));
            // 保存transformer相关配置
            if(transformerConfigs!=null && transformerConfigs.size()>0){
                taskConfig.set(CoreConstant.JOB_TRANSFORMER, transformerConfigs);
            }

            taskConfig.set(CoreConstant.TASK_ID, i);
            contentConfigs.add(taskConfig);
        }

        return contentConfigs;
    }
```