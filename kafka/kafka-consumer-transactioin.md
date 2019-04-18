

## 事务索引

事务索引保存了所有的Aborted 事务消息的位置。对于每个LogSegment文件，都对应着一个事务索引文件

事务索引的原理图



当Consumer请求消息时，不仅会返回消息，还会返回对应范围内的所有Aborted 事务消息。举例如下：





查找事务的代码如下：











## Consumer 过滤事务消息

当Consumer收到响应后，会结合Aborted 事务消息，过滤掉因为事务没有成功的消息。过滤原理如下图所示：

下面表示服务端返回的响应，有多个消息batch，其中涉及到两个事务，A 和 B。 还有每个事务的起始位置。

当Consumer遍历到batch 0时，发现它属于事务A的消息，并且这个batch的起始位置，通过比较事务A的起始和结束位置（Aborted消息的位置），可以判断出该batch是在终止事务中的，所以会跳过。

同理当Consumer遍历到batch 1时，发现它属于事务B的消息，并且这个消息是在终止的事务中，所以会跳过。

当Consumer遍历到 batch 2，它是事务A的终止消息batch。这个batch很特殊，它不包含任何数据，只是表示事务的终止。

当Consumer遍历到 batch 3，发现它并不在任何终止事务中，所以认为这个batch是合法的，会返回。

解析来的遍历原理同上，最后的返回结果，只包含了batch 3 和 batch 5。







过滤的代码如下：

首先创建一个优先队列，存储Aborted 事务消息，排序依照事务消息的起始offset。

然后遍历消息batch，根据消息batch的 last offset，找到所有可能与它相关的Aborted 事务消息，将这些涉及到的produce id 保存起来。根据这些produce id，就可以判断出此条消息是否为废弃的事务消息。

如果此条消息是Aborted 事务消息，那么说明对应的produce id的事务已经确定了，就将 produce id 从集合中删除掉。

```java
private class PartitionRecords {
    // 消息batch列表
    private final Iterator<? extends RecordBatch> batches;
    // 保存了那些producer，发送的消息为事务终止
    private final Set<Long> abortedProducerIds;
    // record结果列表，从batch中生成
    private CloseableIterator<Record> records;
    // 保存了所有了终止事务的信息
    private final PriorityQueue<FetchResponse.AbortedTransaction> abortedTransactions;
    
    private Record nextFetchedRecord() {
        while (true) {
            if (records == null || !records.hasNext()) {
                ....
                // 如果records遍历完了，需要从下个batch生成
                currentBatch = batches.next();
                // 注意到isolationLevel，它可以设置只读取事务成功的消息，这样就可以过滤掉由于事务终止的废弃消息
                if (isolationLevel == IsolationLevel.READ_COMMITTED && currentBatch.hasProducerId()) {
                    // 更新abortedProducerIds列表
                    consumeAbortedTransactionsUpTo(currentBatch.lastOffset());

                    long producerId = currentBatch.producerId();
                    if (containsAbortMarker(currentBatch)) {
                        // 如果是终止事务batch，那么就从abortedProducerIds列表中，将对应的produce id删除，
                        // 因为该消息表示该事务的终止，表示该producer之后发送的消息，已经不再属于上次终止的事务了。
                        abortedProducerIds.remove(producerId);
                    } else if (isBatchAborted(currentBatch)) {
                        // 如果确定该batch因为事务终止而废弃的，那么跳过
                        nextFetchOffset = currentBatch.nextOffset();
                        continue;
                    }
                }
                // 从batch中生成record列表
                records = currentBatch.streamingIterator(decompressionBufferSupplier);
            } else {
                // 遍历batch里的record
                Record record = records.next();
                    if (record.offset() >= nextFetchOffset) {
                        maybeEnsureValid(record);
                        // 这里如果遇到事务确认成功的消息batch，则需要跳过
                        if (!currentBatch.isControlBatch()) {
                            return record;
                        } else {
                            // 通过设置nextFetchOffset，跳过这个batch（因为这个batch是只包含一个事务成功的取人消息，）
                            nextFetchOffset = record.offset() + 1;
                        }
                    }
                }
            }
        }
    }
     
    private boolean isBatchAborted(RecordBatch batch) {
        // 如果该batch是事务类型，并且它的produce id 在abortedProducerIds集合里
        return batch.isTransactional() && abortedProducerIds.contains(batch.producerId());
    }
    
    private void consumeAbortedTransactionsUpTo(long offset) {
        if (abortedTransactions == null)
            return;
        // 找到那些事务，它的跨度包含了当前batch
        while (!abortedTransactions.isEmpty() && abortedTransactions.peek().firstOffset <= offset) {           // 这里会从abortedTransactions提取事务信息
            FetchResponse.AbortedTransaction abortedTransaction = abortedTransactions.poll();
            // 并且将它的produce id 添加到abortedProducerIds集合里，表示现在produce id发送的消息处于终止事务里
            abortedProducerIds.add(abortedTransaction.producerId);
        }
    }   
}
```









参考资料： <https://www.confluent.io/blog/transactions-apache-kafka/>

<https://docs.google.com/document/d/11Jqy_GjUGtdXJK94XGsEIK7CP1SnQGdp2eF0wSw9ra8/edit#>

<https://docs.google.com/document/d/1Rlqizmk7QCDe8qAnVW5e5X8rGvn6m2DCR3JR2yqwVjc/edit>

<https://toutiao.io/posts/9nzg8m/preview>