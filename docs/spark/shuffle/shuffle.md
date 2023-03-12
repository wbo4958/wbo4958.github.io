---
layout: page
title: Shuffle 实现
nav_order: 9 
parent: Spark 
---

# Spark Shuffle
{: .no_toc}

本文通过 repartition 学习 Spark 中 shuffle 的实现. 本文基于 Spark 3.2.0

``` scala
val df = Seq(1, 2, 3, 4, 5, 6).toDS()
val df1 = df.repartition(2)
df1.show()
```

## 目录
{: .no_toc .text-delta}

1. TOC
{:toc}

## Shuffle 过程

![shuffle](/docs/spark/shuffle/shuffle-shuffle.svg)

DAG 划分 Stages 的判断条件是 ShuffleDependency. 将 ShuffeleDependency 自带的 rdd chain 划分为 ShuffleStage.

Shuffle整个过程分为 Shuffle write 和 Shuffle read 过程, 分别在不同的 Stage 里完成.

那 Spark 是如何将 Shuffle write 和 Shuffle read 关联起来呢? 答案是 ShuffleDependency, ShuffleDependency 是一个 Serializeable 的类, 由 driver 端生成一个对应的实例, 该实例会同时存在于 Shuffle write 和 Shuffle read 中.

对于 ShuffleWrite, ShuffleMapTask 首先反序列化获得 ShuffleDependency. 然后调用 `ShuffleDependency.shuffleWriterProcessor.write(rdd, dep, ...)` 将rdd的计算结果进行 shuffle.

对于 ShuffleRead, ShuffledRowRDD 借助于 ShuffleDependency 进行 shuffle read.

ShuffleDependency 类中保存了很多在 shuffle write/read 过程中需要用到的一些变量, 如

- shuffleId - 每个 Shuffle 过程都有一个唯一 ID
- partitioner - 如何对数据进行分区
- ShuffleHandle -  决定 shuffle write 由哪个 ShuffleWriter 来写入
- Serializer - 对shuffle数据进行序列化/反序列化
- aggregator/keyOrdering

除了 ShuffleDependency 关联 Shuffle write/read 外, 还有 MapOutputTracker. MapOutputTracker 分为 MapOutputTrackerMaster/MapOutputTrackerWorker, ShuffleMapTask 将 Shuffle Write 后的 MapStatus 保存到 MapOutputTrackerMaster 中. Shuffle read 首先从 MapOutputTrackerMaster 获得相关的 BlockId/mapId/BlockManagerId 用于定位Shuffle数据是保存在哪个 BlockManager 中的哪个文件中.

## Shuffle write

对于 Spark SQL, ShuffleExchangeExec 是一个 Shuffle 的 operator.

在 shuffle write 之前, ShuffleExchangeExec 内部可能会添加 MapPartitionsRDD - 比如为 repartition 添加 sort.

![shuffle-write-rdd](/docs/spark/shuffle/shuffle-rdd-write.svg)

Shuffle write 按照 partitioning 将数据进行分区.

![partitioning](/docs/spark/shuffle/shuffle-Partitioning.svg)

并且在最后会添加一个 MapPartitionsRDD 根据 Partitioning 将数据变成 `Iterator(partitionId, internalRow)` 格式.

根据 ShuffleHandle 的不同, ShuffleWriter 不同, 对数据 shuffle的方式不同, 可以分为下面三种.

- UnsafeShuffleWriter

  ![UnsafeShuffleWriter](/docs/spark/shuffle/shuffle-UnsafeShuffleWriter.svg)

  UnsafeShuffleWriter 利用 Unsafe 的接口将数据存储到 MemoryBlock (具体是 ON_HEAP 还是 OFF_HEAP 由 `spark.memory.offHeap.enabled` 控制). 另外将数据的保存地址与reducer的partitionID pack 成一个 long 型数据保存到 ShuffleInMemorySorter.
当有 spill 发生时, ShuffleInMemorySorter 先将保存的地址按照 partitionID 进行排序, 此目的是将相同 reducer partitionID的数据放在一起, 最后 spill 时根据排序好的地址获得最终的数据并写入到 shuffle 文件中 (数据落盘), 并返回 Spill 信息(shuffle 文件名, 每个 reducer partition 的数据长度及其它)

- SortShuffleWriter

  ![SortShuffleWriter](/docs/spark/shuffle/shuffle-SortShuffleWriter.svg)

  SortShuffleWriter 先对相同的 (partition, key) 进行 merge 和 sort, 这样可以保证同一个 partition 中的数据放在一起, 然后对数据进行磁盘落地.

- BypassMergeSortShuffleWriter (default)

  ![BypassMergeSortShuffleWriter](/docs/spark/shuffle/shuffle-BypassMergeSortShuffleWriter.svg)

  BypassMergeSortShuffleWriter 在 shuffle write的时候 Bypass sort merge 相关操作, BypassMergeSortShuffleWriter 创建多个 (reducer partition个数) shuffle 临时文件, 然后将数据分别写入到这些shuffle 临时文件中, 最后合并成一个大的 shuffle data 文件, 以及 shuffle index.

尽管分为3种不同的 ShuffleWriter, 但最终都会生成两种文件, 一个 shuffle.data 保存真正的 (key, value) 数据, 另一个是 shuffle.index 保存 partition 的偏移. 只不过可能这3种的 performance 会不同, 默认的 BypassMergeSortShuffleWriter 可能会优于其它两种, 因为它没有 sort 的过程.

那如何选择 shuffle writer 呢?

1. 如果 Reducer partition number <= `spark.shuffle.sort.bypassMergeThreshold`(200) 时选择 **BypassMergeSortShuffleWriter**. 如果 partition 数太多, 对于 BypassMergeSortShuffleWriter 方式来说， 每个 task 会产生太多的临时文件，导致 performance 可以下降.

2. Serializer 要支持 `supportsRelocationOfSerializedObjects` UnsafeRowSerializer 默认是支持的. 且 `Reducer partition number <= 16777215`, 选择 **UnsafeShuffleWriter**

3. 1,2 条件不满足，则选择 **SortShuffleWriter**

## shuffle read

Shuffle write已经将数据 partition 好了, Shuffle read基本上就是反过程, 将对应的 partition 从文件中读取出来就行了, 先从 Driver 端获得 shuffle 数据在哪个 BlockManager, 然后从该 BlockManager 找到 shuffle.index 文件找到 partition 的偏移，再从 shuffle.data 数据中读取出来. 首先 ShuffleBlockFetcherIterator 读取相关的数据到 InputStream, 然后再将数据转换为 (key, value), 后面再根据是否需要 agg/sort, 再对从不同 map task 里读取的数据进行 agg/sort ...
