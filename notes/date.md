

java8 的日期处理

```java
public static void main(String[] args) {
    LocalDateTime dateTime = LocalDateTime.now();
    DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    System.out.println(dateTime.format(formatter));
    
    LocalDateTime parsed = LocalDateTime.parse("2019-07-10 11:09:09", formatter);
    System.out.println(parsed.format(formatter));
    
    LocalDateTime next1day = dateTime.plusDays(1);
    System.out.println(next1day.format(formatter));
}
```

