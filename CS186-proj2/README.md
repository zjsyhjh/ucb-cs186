### CS186 Project2: SimpleDB Operator总结



Project2主要目标是实现数据库表的修改（主要指insert和delete），选择以及joins和aggregates，需要实现的部分主要包括：

- 实现Filter以及Join操作


- 实现IntegerAggregator以及StringAggregator聚合功能
- 实现Insert以及Delete操作



1. **Filter and Join**

   - Filter操作返回的是那些满足`Predicate`的tuples
   - Join操作Join两个tuple满足`JoinPredicate`
   - Predicate的本质是实现两个tuple字段值的比较， 通过调用Field的compare方法来进行比较
   - 具体可以查看代码
     - src/simpledb/Predicate.java
     - src/simpledb/JoinPredicate.java
     - src/simpledb/Filter.java
     - src/simpledb/Join.java

2. **Aggregate**

   - 需要实现5个SQL aggregates(COUNT, SUM, AVG, MIN, MAX)以及支持grouping
   - IntegerAggregator和StringAggregator均实现了Aggregator接口，Aggregator.Op决定了需要进行的操作，然后对于每个tuple(tuple的读取通过child  DbIterator)调用Aggregator.mergeTupleIntoGroup()方法，当所有的tuple都被merge，返回一个DbIterator作为Aggregation的结果
   - 具体可以查看代码
     - src/simpledb/Aggregate.java
     - src/simpledb/IntegerAggregator.java
     - src/simpledb/StringAggregator.java

3. **HeapFile Mutability**

   - HeapFile主要涉及到add tuples以及remove tuples操作
   - 对于add tuples操作，需要调用HeapFile类的insertTuple方法，当添加一条tuple，首先需要找到一个有空slot的page，如果没有，则需要创建一个新的page
   - 对于remove tuples操作，需要调用HeapFile类的deleteTuple方法，该方法通过BufferPool.getPage()方法取得需要删除的tuple所在的page，然后调用HeapPage的deleteTuple方法删除tuple
   - 需要注意的是insertTuple以及deleteTuple都是通过BufferPool类的getPage方法来确定插入或者删除的tuple所在的page，然后再调用HeapPage的插入或者删除方法来操作
   - 具体代码可以查看
     - src/simpledb/HeapPage.java
     - src/simpledb/HeapFile.java
     - src/simpledb/BufferPool.java

4. **Insertion and deletion**

   - Insert以及Delete操作是最上层的操作，返回的是实际操作的tuple的数量，具体都是通过调用BufferPool.insertTuple以及BufferPool.deleteTuple方法实现的
   - 具体代码可以查看
     - src/simpledb/Insert.java
     - src/simpledb/Delete.java
     - src/simpledb/BufferPool.java

5. **Page eviction**

   - 由于内存有限，BufferPool不可能缓存所有的page，因此当page数量超过某个设定值之后，需要剔除一些脏页（如果没有，则提出普通的页）
   - 脏页的剔除需要将页写回磁盘，并且标记它不再是脏页(调用HeapPage类的markDirty()方法），通过HeapFile的writePage操作来实现
   - 具体代码可以查看
     - src/simpledb/BufferPool.java

   ​