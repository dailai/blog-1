# Datax 配置加载

datax，配置

---

## **Configuration类** ##
Datax会读取json格式的配置文件，载入内存，使用Configuration类表示。
### **加载配置文件** ###

Configuration有三种加载方法, 分别支持从文件，字符串，数据流加载。从文件和数据流的方法，本质还是读取内容，通过字符串的方法加载。这三种方法都是只支持json格式。

```
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
```
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
```
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

* 去root元素（即最顶层），path设置为空字符串
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

```
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
```
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
