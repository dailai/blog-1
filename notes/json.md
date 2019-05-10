



Jackson 写 json

```java
ObjectMapper mapper = new ObjectMapper();
ObjectNode treeRootNode = mapper.createObjectNode();
treeRootNode.put("car", "Alfa Romio");
treeRootNode.put("price", "54000");
treeRootNode.put("model", "2013");
ArrayNode arrayNode = treeRootNode.putArray("colors");
arrayNode.add("GRAY");
arrayNode.add("BLACK");
arrayNode.add("WHITE");

String json = treeRootNode.toString()
System.out.println(json);
```





Jackson 读 json

```java
ObjectMapper mapper = new ObjectMapper();
 
JsonNode rootNode = mapper.readTree("....."));

JsonNode carNode = rootNode.path("car");
System.out.println(carNode.asText());

JsonNode priceNode = rootNode.path("price");
System.out.println(priceNode.asText());

JsonNode modelNode = rootNode.path("model");
System.out.println(modelNode.asText());

JsonNode colorsNode = rootNode.path("colors");
Iterator<JsonNode> colors = colorsNode.elements();

while(colors.hasNext()){
    System.out.println(colors.next().asText());
}
```

