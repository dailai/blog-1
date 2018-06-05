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