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



