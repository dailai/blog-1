

Unicode 定义了每个字符和数值的对应表

utf-8是实现了Unicode存储的一种方式。utf-8是变长的，占用1~4个字节

字节顺序是指多个字节存储的之间的顺序。单个字节类的位是不变的。以下列为例，一个 int 占用4个字节，它的十六进制为0x01020304。大端模式表示为 

```shell
--------------------------------
   01  |   02   |   03  |  04
--------------------------------
```

小端模式表示为

```shell
--------------------------------
   04  |   03   |   02  |  01
--------------------------------
```

虽然大小端存储的字节顺序不一样，但是单个字节还是一样的。



Java的String的length，返回的是有多少个字符，而不是占用的字节数（一个字符可能占用多个字节）。

Java的break支持标签，适用于跳出多层循环

Java的数组分配需要new申请，如果是对象的数组，那么初始值为null，并不占用内存。需要单独实例化对象，添加到数组中。

Java建议使用LocalDate来处理日期

Java方法参数有两种类型

* 基本数据类型，采用值传递
* 对象引用，传递引用的值，可以通过引用操作对象

Java中嵌套的包名没有关系，比如java.utils包与java.utils.jar毫无关系

Java包名中的类，需要与目录对应

Java的jar包就属于一个类路径

Java判断实例是否为该类的，instance instanceof class  (如果class是该实例的父类，也会返回true)

Java获取实例的类，getClass

Java接收可变参数，使用 类名 + 三个点号，比如 Object... args

Java的枚举都是继承Enum类，定义枚举需要全部定义好它的实例。

比如下面定义了枚举Size，并且定义了三个实例

```java
public enum Size {
    SMALL("S"), MEDIUM("M"),LARGE("L");
    
    private String value;
    
    private Size(String value) {this.value = value}
}
```

Enum提供了valueOf的方法，例如Size s = Enum.valueOf(Size.class, "SMALL");

Class.forName方法，可以通过字符串获取对应的类。注意类名是包括包名的。例如：Class cl = CLass.forName("java.utils.Random")

Class的newInstance方法，创建实例

 



对于只有一个抽象方法的接口，需要这种接口的对象时，就可以提供一个 lambda 表达式，这种接口称为函数式接口。

lambda表达式中捕获的变量必须实际上是最终变量。

lambda表达式与嵌套块有相同的作用域

引进内部类的类名为 OutClass$InnerClass，外部类和内部类用$隔开



守护线程永远不应该去访问资源，如文件，数据库，因为它会在任何时候甚至在一个操作的中间发生中断。



Jvm的堆分为年轻代和老年代。

Jstack -l pid， 查看Jvm栈的情况，常常用于查看死锁的问题

jmap 用于生产堆转存快照，支持生成hprof文件。然后调用jhat 分析，它会启动一个浏览器。





Thread实例可以通过setUncaughtExceptionHandler方法，设置异常处理器

通过RunTime.getRunTime().addShutdownHook方法，传入Thread，可以在Jvm退出时回调。支持添加多个钩子线程。一般用于资源释放



线程上下文类加载器

当高层提供了统一接口让低层去实现，同时又要是在高层加载（或实例化）低层的类时，必须通过线程上下文类加载器来帮助高层的ClassLoader找到并加载该类。

当使用本类托管类加载，然而加载本类的ClassLoader未知时，为了隔离不同的调用者，可以取调用者各自的线程上下文类加载器代为托管。





ByteBuffer提取某一段字节，转换为utf-8字符串。ByteArray的数据存储在byte[ ] 数组里，但是数据的起始位置不一定是数组的起始位置，需要调用ByteBuffer的arrayOffset方法返回。ByteBuffer的position方法，返回相对于数据起始位置的偏移量。比如我们需要提取ByteBuffer当前位置，长度为3的字符串，那么该代码为

```
 new String(buffer.array(), buffer.arrayOffset() + buffer.position(), 3, StandardCharsets.UTF_8);
```

