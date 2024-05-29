## LSM-KV Project：键值分离的LSM Tree实现

该repo为上海交通大学课程“高级数据结构“课程项目，实现一个键值分离的LSM Tree。

LSM Tree (Log-structured Merge Tree) 是一种可以高性能执行大量写操作的数据结构。它于 1996 年，在Patrick O'Neil等人的一篇论文中被提出。现在，这种数据结构已经广泛应用于数据存储中。Google的LevelDB和Facebook的RocksDB 都以LSM Tree为核心数据结构。2016 年，Lanyue Lu等人发表的论文Wisckey针对LSM Tree长期被人诟病的写放大（Write Amplification）问题提出了相应优化：键值分离。在过去几年中，工业界中也陆续涌现出键值分离的LSM Tree存储引擎，例如TerarkDBTitan等。

## 程序编译

使用如下命令编译程序：

```
# 编译链接测试文件
make all
# 清理编译结果
make clean
```

## 正确性测试

正确性测试实现在correctness.cc文件中，使用如下命令运行正确性测试：

使用如下命令编译程序并运行正确性测试：

```
# 运行正确性测试
make correctness
```

## 持久性测试

持久性测试实现在persistence.cc文件中，使用如下命令运行持久性测试：

使用如下命令编译程序并运行持久性测试：

```
# 运行持久性测试
make persistence
```

## 性能测试

性能测试实现在performance.cc文件中，包含以下四种测试

+ 常规测试：测试不同数据量情况下，Get、Put、Delete和Scan四种操作的平均时延和吞吐量
+ 索引缓存与Bloom Filter的效果测试：对比下面三种情况Get操作的平均时延：
  + 内存中没有缓存SSTable的任何信息，从磁盘中访问SSTable的索引，在找到offset之后读取数据
  + 内存中只缓存了SSTable的索引信息，通过二分查找从SSTable的索引中找到offset，并在磁盘中读取对应的值
  + 内存中缓存SSTable的Bloom Filter和索引，先通过Bloom Filter判断一个键值是否可能在一个SSTable中，如果存在再利用二分查找，否则直接查看下一个SSTable的索引
+ Compaction的影响：在不断插入数据的情况下，统计键值对应Put操作的时延，并将结果写入文件compaction_latency.csv
+ Bloom Filter大小配置的影响：在SSTable大小固定为16Kb的情况下，测试不同Bloom Filter大小下Get、Put操作的性能，过程中需要手动修改config.h中bloom_filter_size的值。

使用如下命令运行性能测试：

```
# 运行性能测试
make performance
```

