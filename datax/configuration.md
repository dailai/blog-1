# Datax 配置加载

datax，配置

---

## **Configuration类** ##
Datax会读取json格式的配置文件，载入内存，使用Configuration类表示。
### **加载配置文件** ###

Configuration有三种加载方法, 分别支持从文件，字符串，数据流加载。从文件和数据流的方法，本质还是读取内容，通过字符串的方法加载。这三种方法都是只支持json格式。

``` java
public class Configuration {

    public static Configuration from(String json) {
        // 替换环境变量
        json = StrUtil.replaceVariable(json);
        // 检查json字符串的格式
    	checkJSON(json);

    	try {
    	    // 实例化Configuration
    		return new Configuration(json);
    	} catch (Exception e) {
    		throw DataXException.asDataXException(CommonErrorCode.CONFIG_ERROR,
    				e);
    	}
    }
}
```

将${variable}格式的变量，如果对应环境变量的值存在并为非空，则替换成对应的值。
``` java
public class StrUtil {
    // 格式为以$开头，变量名不为空,由字母数字下划线组成，并且被{}包围
    private static final Pattern VARIABLE_PATTERN = Pattern
            .compile("(\\$)\\{?(\\w+)\\}?");

    public static String replaceVariable(final String param) {
        Map<String, String> mapping = new HashMap<String, String>();

        Matcher matcher = VARIABLE_PATTERN.matcher(param);
        while (matcher.find()) {
            // 提取变量
            String variable = matcher.group(2);
            // 取出环境变量的值
            String value = System.getProperty(variable);
            if (StringUtils.isBlank(value)) {
                value = matcher.group();
            }
            mapping.put(matcher.group(), value);
        }

        String retString = param;
        for (final String key : mapping.keySet()) {
            // 替换环境变量的值
            retString = retString.replace(key, mapping.get(key));
        }

        return retString;
    }
```

Configuration接收到json字符串后，调用fastJson解析
``` java
import com.alibaba.fastjson.JSON;

private Configuration(final String json) {
		try {
			this.root = JSON.parse(json);
		} catch (Exception e) {
			throw DataXException.asDataXException(CommonErrorCode.CONFIG_ERROR,
					String.format("配置信息错误. 您提供的配置信息不是合法的JSON格式: %s . 请按照标准json格式提供配置信息. ", e.getMessage()));
		}
	}
```

### **查询规则** ###
path 不能有空字符串

* 取root元素（即最顶层），path设置为空字符串
* 查询列表中的某一项，必须以中括号[ ]包含索引值，索引值从0开始。

例如：

```
{
    "job": {
        "content": [
            {
                "reader": {
                    ......
                },
                ......
            }
        ]
    }
}

```

如果取content列表的第一个元素的reader，则path为job.content[0].reader

### 查询原理 ###
Configuration 类是主要通过get方法获取值，返回Object对象。Object可以是Json下的数据类型，比如int， string， map，list
 
``` java
public Object get(final String path) {
        // 检查path合法性，不能包含空字符
		this.checkPath(path);
		try {
			／／ 调用findObject方法查询
			return this.findObject(path);
		} catch (Exception e) {
			return null;
		}
	}
```

继续看看findObject方法。首先它会将path拆分成list，拆分规则是，先把含有列表项[ ]提取出来，然后根据分隔符 . 切割。比如：job.content[0].reader， 会被拆分为["job", "content", "[0]", "reader"]
``` java
private Object findObject(final String path) {
  // 如果为空字符串，则返回root元素
  boolean isRootQuery = StringUtils.isBlank(path);
  if (isRootQuery) {
    return this.root;
  }

  Object target = this.root;
  // 先将path拆分为列表， 循环遍历
  for (final String each : split2List(path)) {

    if (isPathMap(each)) {
      // 判断如果为map，则认为target元素为Map类型，直接取对应key的value值
      target = findObjectInMap(target, each);
      continue;
    } else {
      // 判断如果list，则认为target元素为list类型，直接取对应索引的值
      target = findObjectInList(target, each);
      continue;
    }
  }

  return target;
}


// 字符包含中括号[ ]，则认为是是属于List类型的路径
private boolean isPathList(final String path) {
  return path.contains("[") && path.contains("]");
}

// 字符非空并且不是List类型的路径，则认为是Map类型
private boolean isPathMap(final String path) {
  return StringUtils.isNotBlank(path) && !isPathList(path);
}

private Object findObjectInMap(final Object target, final String index) {
  .......
  将target强制转换为Map类型，取key对应的value值
  Object result = ((Map<String, Object>) target).get(index);
  .......

  return result;
}

private Object findObjectInList(final Object target, final String each) {
  ......
  // 去除中括号，返回里面的数字
  String index = each.replace("[", "").replace("]", "");
  ......
  // 将target强制转换为List类型，取列表中对应索引的元素
  return ((List<Object>) target).get(Integer.valueOf(index));
}
```

### 设置原理 ###
可以将json看成数结构，递归的改变子节点的值

``` java
private void setObject(final String path, final Object object) {
        // 递归调用setObjectRecursive方法，设置值
		Object newRoot = setObjectRecursive(this.root, split2List(path), 0,
				object);

		if (isSuitForRoot(newRoot)) {
			this.root = newRoot;
			return;
		}

		......
	}

 
    Object setObjectRecursive(Object current, final List<String> paths,
			int index, final Object value) {

		// 如果是已经超出path，我们就返回value即可，作为最底层叶子节点
		boolean isLastIndex = index == paths.size();
		if (isLastIndex) {
			return value;
		}
        // 取出对应的path
		String path = paths.get(index).trim();
        // 判断path是否对应的元素是Map
		boolean isNeedMap = isPathMap(path);
		if (isNeedMap) {
			Map<String, Object> mapping;

			// 当前不是map，因此全部替换为map，并返回新建的map对象
			boolean isCurrentMap = current instanceof Map;
			if (!isCurrentMap) {
				mapping = new HashMap<String, Object>();
				mapping.put(
						path,
						buildObject(paths.subList(index + 1, paths.size()),
								value));
				return mapping;
			}

			// 当前是map，但是没有对应的key，也就是我们需要新建对象插入该map，并返回该map
			mapping = ((Map<String, Object>) current);
			boolean hasSameKey = mapping.containsKey(path);
			if (!hasSameKey) {
				mapping.put(
						path,
						buildObject(paths.subList(index + 1, paths.size()),
								value));
				return mapping;
			}

			// 当前是map，并且存在这个值，递归遍历
            // 取出当前元素， index自增1， 递归调用setObjectRecursive
			current = mapping.get(path);
			mapping.put(path,
					setObjectRecursive(current, paths, index + 1, value));
			return mapping;
		}

        // 判断path对应的元素是否是List
		boolean isNeedList = isPathList(path);
		if (isNeedList) {
			List<Object> lists;
			int listIndexer = getIndex(path);

			// 当前是list，直接新建并返回即可
			boolean isCurrentList = current instanceof List;
			if (!isCurrentList) {
				lists = expand(new ArrayList<Object>(), listIndexer + 1);
				lists.set(
						listIndexer,
						buildObject(paths.subList(index + 1, paths.size()),
								value));
				return lists;
			}

			// 当前是list，但是对应的indexer是没有具体的值，也就是我们新建对象然后插入到该list，并返回该List
			lists = (List<Object>) current;
			lists = expand(lists, listIndexer + 1);

			boolean hasSameIndex = lists.get(listIndexer) != null;
			if (!hasSameIndex) {
				lists.set(
						listIndexer,
						buildObject(paths.subList(index + 1, paths.size()),
								value));
				return lists;
			}

			// 当前是list，并且存在对应的index，继续递归遍历
			current = lists.get(listIndexer);
			lists.set(listIndexer,
					setObjectRecursive(current, paths, index + 1, value));
			return lists;
		}

	}
```

### 加载方式 ###
Configuration加载支持http get请求，和本地文件，由ConfigParser类负责。

``` java
    public static Configuration parseJobConfig(final String path) {
        // 获取配置内容
        String jobContent = getJobContent(path);
        // 实例化Configuration
        Configuration config = Configuration.from(jobContent);
        // 将一些加密的值，反解密出来
        return SecretUtil.decryptSecretKey(config);
    }

    /**
    根据路径，获取配置内容
    */
    private static String getJobContent(String jobResource) {
        String jobContent;
        // 判断是否是http协议
        boolean isJobResourceFromHttp = jobResource.trim().toLowerCase().startsWith("http");


        if (isJobResourceFromHttp) {
            ......
            // http 请求获取配置内容
            HttpClientUtil httpClientUtil = new HttpClientUtil();
            try {
                URL url = new URL(jobResource);
                // 只支持Get请求
                HttpGet httpGet = HttpClientUtil.getGetRequest();
                httpGet.setURI(url.toURI());

                jobContent = httpClientUtil.executeAndGetWithFailedRetry(httpGet, 1, 1000l);
            } catch (Exception e) {
               ......
            }
        } else {
            // jobResource 是本地文件绝对路径
            try {
                // 读取本地文件的内容
                jobContent = FileUtils.readFileToString(new File(jobResource));
            } catch (IOException e) {
                ......
            }
        }
        return jobContent;
    }
```

加密的方式支持RSA和DESede两种方法。加密的路径，比如以两个*开始，比如：
```
例如：mysql的用户名需要加密，下面的job.content[0].reader.parameter.**username, 和job.content[0].reader.parameter.**password

``` json
{
    "job": {
        "content": [
            {
                "reader": {
                    "name": "mysqlreader",
                    "parameter": {
                        "**username": "decrypted_username",
                        "**password": "decrypted_password",
                    }
                },
                ......
            }
        ]
    }
}

```

### 加载过程 ###
ConfigParser首先会读取配置文件，然后合并内置的配置。然后按需，加载要用到的reader，writer，handler。

``` java

    public static Configuration parse(final String jobPath) {
        Configuration configuration = ConfigParser.parseJobConfig(jobPath);
        // 合并 conf/core.json文件的配置, false 表示不覆盖原有的配置
        configuration.merge(
                //CoreConstant.DATAX_CONF_PATH的值为conf/core.json
                ConfigParser.parseCoreConfig
                (CoreConstant.DATAX_CONF_PATH),
                false);
        // 获取job.content列表的第一个reader
        String readerPluginName = configuration.getString(
                //CoreConstant.DATAX_JOB_CONTENT_READER_NAME的值为job.content[0].reader.name
                CoreConstant.DATAX_JOB_CONTENT_READER_NAME);
        // 获取job.content列表的第一个writer
        String writerPluginName = configuration.getString(
                //CoreConstant.DATAX_JOB_CONTENT_WRITER_NAME的值为job.content[0].writer.name
                CoreConstant.DATAX_JOB_CONTENT_WRITER_NAME);
        // 读取job.preHandler.pluginName
        String preHandlerName = configuration.getString(
                //CoreConstant.DATAX_JOB_PREHANDLER_PLUGINNAME的值为job.preHandler.pluginName
                CoreConstant.DATAX_JOB_PREHANDLER_PLUGINNAME);
        // 读取job.postHandler.pluginName
        String postHandlerName = configuration.getString(
                //CoreConstant.DATAX_JOB_POSTHANDLER_PLUGINNAME的值为job.postHandler.pluginName
                CoreConstant.DATAX_JOB_POSTHANDLER_PLUGINNAME);

        Set<String> pluginList = new HashSet<String>();
        pluginList.add(readerPluginName);
        pluginList.add(writerPluginName);
        ......
        // 调用parsePluginConfig生成plugin的配置，然后合并
        configuration.merge(parsePluginConfig(new ArrayList<String>(pluginList)), false);
        ......
        return configuration;
    }
```

plugin配置加载
``` java
    public static Configuration parsePluginConfig(List<String> wantPluginNames) {
        Configuration configuration = Configuration.newDefault();
        ......
        // 遍历plugin.reader目录下的文件夹
        for (final String each : ConfigParser
                .getDirAsList(CoreConstant.DATAX_PLUGIN_READER_HOME)) {
            // 调用 parseOnePluginConfig解析单个plugin配置
            Configuration eachReaderConfig = ConfigParser.parseOnePluginConfig(each, "reader", replicaCheckPluginSet, wantPluginNames);
            if(eachReaderConfig!=null) {
                configuration.merge(eachReaderConfig, true);
                complete += 1;
            }
        }

        // 遍历plugin.writer目录下的文件夹
        for (final String each : ConfigParser
                .getDirAsList(CoreConstant.DATAX_PLUGIN_WRITER_HOME)) {
            // 调用 parseOnePluginConfig解析单个plugin配置
            Configuration eachWriterConfig = ConfigParser.parseOnePluginConfig(each, "writer", replicaCheckPluginSet, wantPluginNames);
            if(eachWriterConfig!=null) {
                configuration.merge(eachWriterConfig, true);
                complete += 1;
            }
        }

        ......

        return configuration;
    }

public static Configuration parseOnePluginConfig(final String path, final String type, Set<String> pluginSet, List<String> wantPluginNames) {
        String filePath = path + File.separator + "plugin.json";
        Configuration configuration = Configuration.from(new File(filePath));

        String pluginPath = configuration.getString("path");
        String pluginName = configuration.getString("name");
        if(!pluginSet.contains(pluginName)) {
            pluginSet.add(pluginName);
        } else {
            ......
        }

        //不是想要的插件，就不生成配置，直接返回
        if (wantPluginNames != null && wantPluginNames.size() > 0 && !wantPluginNames.contains(pluginName)) {
            return null;
        }

        // plugin.json的path路径，是指插件的jar包。如果没有指定，则默认为和plugin.json文件在同一个目录下
        boolean isDefaultPath = StringUtils.isBlank(pluginPath);
        if (isDefaultPath) {
            configuration.set("path", path);
        }

        Configuration result = Configuration.newDefault();
        // 最后保存在puligin.{type}.{pluginName}路径下
        result.set(
                String.format("plugin.%s.%s", type, pluginName),
                configuration.getInternal());

        return result;
    }

```

### 总结 ###
Datax里面最基础的组件Configuration已经介绍完了。它的本质就是使用fastJson解析出来的对象，支持常用的get，set方法。比较特殊的就是涉及到list的操作。

Configuration的加载过程，涉及到环境变量替代，加密解密，插件加载。