### CS186 Project1: SimpleDB总结

1. Fields and Tuples
    - Tuple可以说是SimpleDB中最基本的概念了，由一个个Field组成；Field是接口，本项目提供了Integer以及String的实现，具体可以查看IntField类以及StringField类的实现；TupleDesc类用来描述Tuple，每一个Tuple都对应着一个TupleDesc，每个TupleDesc由一个个Type组成，用于描述Tuple中每个Field的相关信息（类型，名称）
    - 相关代码
        - src/java/simpledb/Type.java
        - src/java/simpledb/Field.java
        - src/java/simpledb/IntField.java
        - src/java/simpledb/StringField.java
        - src/java/simpledb/Tuple.java
        - src/java/simpledb/TupleDesc.java
2. Catalog
    - Catalog是table的集合，每个table默认对应着一个file，可以使用HashMap来实现，并且每个table也对应着一个TupleDesc对象来描述table中保存的Tuple的类型以及每个Tuple的Field；
    - 相关代码
        - src/java/simpledb/DbFile.java
        - src/java/simpledb/Catalog.java
3. BufferPool, HeapPage and HeapFile
    - BufferPool用于缓存最近从磁盘读取的pages，所有读写磁盘文件的操作都要通过BufferPool, BufferPool默认指定了可以缓存page的数目，不过也可以通过numPages参数指定（如果超过了指定的缓存数目，需要剔除某些page，后面的project需要实现）
    - HeapFile由一系列的HeapPage组成，HeapPage的大小由BufferPool.PAGE_SIZE决定，之前提到过，在SimpleDB中，默认是一个HeapFile对应着一个table；每个HeapPage被分割成一个个slot，每个slot对应着一个tuple（如何判断当前slot有没有存储tuple？每个HeapPage的开头会留一部分空间用于存储slot的状态，给每个slot设置一个状态位，1代表着当前slot已经存储了tuple，0代表没有，因此可以通过这个状态位来判断当前slot是否空）
    - 如何定位某个Tuple的位置
        - 每个tuple不仅仅关联了用于描述tuple信息的TupleDesc，同时也关联了一个RecordId，这个RecordId记录了tuple所在的PageId以及在Page中的偏移量，而PageId又纪录了page所在的tableId以及在table中的偏移量，这样就能够确定某个tuple的准确位置了
    - 相关代码
        - src/java/simpledb/BufferPool.java
        - src/java/simpledb/HeapPageId.java
        - src/java/simpledb/HeapPage.java
        - src/java/simpledb/RecordId.java
        - src/java/simpledb/HeapFile.java
4. Operators
    - 在SimpleDB中，每个operater都需要实现DbIterator接口
    - 相关代码
        - src/java/simpledb/DbIterator.java
        - src/java/simpledb/DbFileIterator.java
        - src/java/simpledb/SeqScan.java
