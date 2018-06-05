# Datax 插件载入

datax，插件， 加载

---

### JarLoader ###
JarLoader继承URLClassLoader，负责从指定的目录下，把传入的路径、及其子路径、以及路径中的jar文件加入到class path。

``` java
public class JarLoader extends URLClassLoader {
    public JarLoader(String[] paths) {
        this(paths, JarLoader.class.getClassLoader());
    }

    public JarLoader(String[] paths, ClassLoader parent) {
        // 调用getURLS，获取所有的jar包路径
        super(getURLs(paths), parent);
    }

    /**
    获取所有的jar包
    */
    private static URL[] getURLs(String[] paths) {
        // 获取包括子目录的所有目录路径
        List<String> dirs = new ArrayList<String>();
        for (String path : paths) {
            dirs.add(path);
            // 获取path目录和其子目录的所有目录路径
            JarLoader.collectDirs(path, dirs);
        }
        // 遍历目录，获取jar包的路径
        List<URL> urls = new ArrayList<URL>();
        for (String path : dirs) {
            urls.addAll(doGetURLs(path));
        }

        return urls.toArray(new URL[0]);
    }

    /**
    递归的方式，获取所有目录
    */
    private static void collectDirs(String path, List<String> collector) {
        // path为空，终止
        if (null == path || StringUtils.isBlank(path)) {
            return;
        }

        // path不为目录，终止
        File current = new File(path);
        if (!current.exists() || !current.isDirectory()) {
            return;
        }

        // 遍历完子文件，终止
        for (File child : current.listFiles()) {
            if (!child.isDirectory()) {
                continue;
            }

            collector.add(child.getAbsolutePath());
            collectDirs(child.getAbsolutePath(), collector);
        }
    }    

    private static List<URL> doGetURLs(final String path) {
        
        File jarPath = new File(path);
	// 只寻找文件以.jar结尾的文件
        FileFilter jarFilter = new FileFilter() {
            @Override
            public boolean accept(File pathname) {
                return pathname.getName().endsWith(".jar");
            }
        };

		
        File[] allJars = new File(path).listFiles(jarFilter);
        List<URL> jarURLs = new ArrayList<URL>(allJars.length);

        for (int i = 0; i < allJars.length; i++) {
            try {
                jarURLs.add(allJars[i].toURI().toURL());
            } catch (Exception e) {
                throw DataXException.asDataXException(
                        FrameworkErrorCode.PLUGIN_INIT_ERROR,
                        "系统加载jar包出错", e);
            }
        }

        return jarURLs;
    }

```

### 加载器原理 ###
每个插件的加载，都是都是由独立的加载器负责，也就是上述的JarLoader,这样起到了隔离的作用。加载器的管理是LoadUtil类负责。
```java
public class LoadUtil {
    // 加载器的HashMap, Key由插件类型和名称决定, 格式为plugin.{pulginType}.{pluginName}
    private static Map<String, JarLoader> jarLoaderCenter = new HashMap<String, JarLoader>();



public static synchronized JarLoader getJarLoader(PluginType pluginType, String pluginName) {
        Configuration pluginConf = getPluginConf(pluginType, pluginName);

        JarLoader jarLoader = jarLoaderCenter.get(generatePluginKey(pluginType,
                pluginName));
        if (null == jarLoader) {
            // 构建加载器JarLoader
            // 获取jar所在的目录
            String pluginPath = pluginConf.getString("path");
            jarLoader = new JarLoader(new String[]{pluginPath});
            //添加到HashMap中
            jarLoaderCenter.put(generatePluginKey(pluginType, pluginName),
                    jarLoader);
        }

        return jarLoader;
    }

    private static final String pluginTypeNameFormat = "plugin.%s.%s";
// 生成HashMpa的key值
    private static String generatePluginKey(PluginType pluginType,
                                            String pluginName) {
        return String.format(pluginTypeNameFormat, pluginType.toString(),
                pluginName);
    }
```


### LoadUtil接口 ###
datax都是使用LoadUtil的方法，来加载各种插件。插件暂时分为Job和Task两种。 先来看看plugin的类图

![](https://github.com/zhmin/blog/blob/datax/datax/images/plugin-uml.png?raw=true)

LoadUtil提供了接口，来加载不同类型的插件。
```java

/**
加载Job类型的Plugin
*/
public static AbstractJobPlugin loadJobPlugin(PluginType pluginType, String pluginName) {
        // 调用loadPluginClass方法，加载插件对应的class
        Class<? extends AbstractPlugin> clazz = LoadUtil.loadPluginClass(pluginType, pluginName, ContainerType.Job);

        // 实例化Plugin，转换为AbstractJobPlugin
        AbstractJobPlugin jobPlugin = (AbstractJobPlugin) clazz.newInstance();
        // 设置Job的配置,路径为plugin.{pluginType}.{pluginName}
        jobPlugin.setPluginConf(getPluginConf(pluginType, pluginName));
        return jobPlugin;

    }

/**
加载Task类型的Plugin
    */
public static AbstractTaskPlugin loadTaskPlugin(PluginType pluginType, String pluginName) {
        // 调用loadPluginClass方法，加载插件对应的class
        Class<? extends AbstractPlugin> clazz = LoadUtil.loadPluginClass(pluginType, pluginName, ContainerType.Task);
        // 实例化Plugin，转换为AbstractTaskPlugin
        AbstractTaskPlugin taskPlugin = (AbstracTasktTaskPlugin) clazz.newInstance();
        // 设置Task的配置,路径为plugin.{pluginType}.{pluginName}
        taskPlugin.setPluginConf(getPluginConf(pluginType, pluginName));
    }

```

上述方法都调用了loadPluginClass方法
```java
    private static synchronized Class<? extends AbstractPlugin> loadPluginClass(
            PluginType pluginType, String pluginName,
            ContainerType pluginRunType) {
        // 获取插件配置
        Configuration pluginConf = getPluginConf(pluginType, pluginName);
        // 获取插件对应的ClassLoader
        JarLoader jarLoader = LoadUtil.getJarLoader(pluginType, pluginName);
        try {
            // 加载插件的class
            return (Class<? extends AbstractPlugin>) jarLoader
                    .loadClass(pluginConf.getString("class") + "$"
                            + pluginRunType.value());
        } catch (Exception e) {
            throw DataXException.asDataXException(FrameworkErrorCode.RUNTIME_ERROR, e);
        }
    }
```


### preHandler和postHandler的加载 ###
preHandler在导入数据之前，会被调用。postHandler在数据导入结束后，会被调用。这两个插件比较简单，可以来看看它的加载过程。它们的加载是由JobContainer负责

```java
public class JobContainer extends AbstractContainer {
    private void preHandle() {
        // 获取handler的类型
        String handlerPluginTypeStr = this.configuration.getString(
                CoreConstant.DATAX_JOB_PREHANDLER_PLUGINTYPE);
        if(!StringUtils.isNotEmpty(handlerPluginTypeStr)){
            return;
        }
        PluginType handlerPluginType;
        try {
            handlerPluginType = PluginType.valueOf(handlerPluginTypeStr.toUpperCase());
        } catch (IllegalArgumentException e) {
            throw DataXException.asDataXException(
                    FrameworkErrorCode.CONFIG_ERROR,
                    String.format("Job preHandler's pluginType(%s) set error, reason(%s)", handlerPluginTypeStr.toUpperCase(), e.getMessage()));
        }

        // 获取handler的名称
        String handlerPluginName = this.configuration.getString(
                CoreConstant.DATAX_JOB_PREHANDLER_PLUGINNAME);
        // 设置当前线程的thread context class loader为，plugin对应的JarLoader
        classLoaderSwapper.setCurrentThreadClassLoader(LoadUtil.getJarLoader(
                handlerPluginType, handlerPluginName));
        // 加载Plugin
        AbstractJobPlugin handler = LoadUtil.loadJobPlugin(
                handlerPluginType, handlerPluginName);
        // 设置JobPluginCollector
        JobPluginCollector jobPluginCollector = new DefaultJobPluginCollector(
                this.getContainerCommunicator());
        handler.setJobPluginCollector(jobPluginCollector);

        //调用preHandler方法
        handler.preHandler(configuration);
        // 恢复thread context class loader
        classLoaderSwapper.restoreCurrentThreadClassLoader();
    }
```

postHandler和preHandler同理。这里设置到了ClassLoaderSwapper类，
```java
public final class ClassLoaderSwapper {
    private ClassLoader storeClassLoader = null;

    private ClassLoaderSwapper() {
    }

    public static ClassLoaderSwapper newCurrentThreadClassLoaderSwapper() {
        return new ClassLoaderSwapper();
    }

    /**
     * 保存当前classLoader，并将当前线程的classLoader设置为所给classLoader
     */
    public ClassLoader setCurrentThreadClassLoader(ClassLoader classLoader) {
        this.storeClassLoader = Thread.currentThread().getContextClassLoader();
        Thread.currentThread().setContextClassLoader(classLoader);
        return this.storeClassLoader;
    }

    /**
     * 将当前线程的类加载器设置为保存的类加载
     */
    public ClassLoader restoreCurrentThreadClassLoader() {
        ClassLoader classLoader = Thread.currentThread()
                .getContextClassLoader();
        Thread.currentThread().setContextClassLoader(this.storeClassLoader);
        return classLoader;
    }
}

```

整个preHandler的加载过程就是，通过改变ThreadContextClassloader为JarLoader，然后loadClass加载类，实例化，调用prehandlerde的方法，最后恢复当前线程的ThreadContextClassloader。
