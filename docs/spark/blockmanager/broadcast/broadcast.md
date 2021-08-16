---
layout: page
title: Broadcast
nav_order: 10
parent: BlockManager
grand_parent: Spark
---

# Broadcast
{: .no_toc}

本文通过在代码中使用 Broadcast 来学习 Broadcast 原理. 本文基于 Spark 3.1.1.

## 目录
{: .no_toc .text-delta}

1. TOC
{:toc}

## 示例代码

``` java
case class Bean(x: Int) //自定义一个 Bean class

val conf = new SparkConf()
val sc = SparkContext.getOrCreate(conf)
val list = List[Int](1, 2, 3, 4)
val broadcast = sc.broadcast(list) // 广播一个 List
val results = sc.parallelize(1 to 2).map { x =>
    (x, broadcast.value.sum) // 在 Executor 端获得 broadcast 的值，并求和
}
assert(results.collect().toSet === Set((1, 10), (2, 10)))
sc.stop()
```

## Broadcast 类

`sc.broadcast` 最终生成一个 TorrentBroadcast 对象. TorrentBroadcast 继承于 Serializable.

|TorrentBroadcast||
|----|----|
|@transient_value: SoftReference[T]|广播的值在本地的缓存，声明为 SoftReference, GC时_value被回收|
|@transient compressionCodec |对广播的值进行压缩使用的  codec|
|@transient blockSize: Int|每个Block的大小,默认为4M|
|broadcastId BroadcastBlockId(id)|Broadcast 的 BlockID|
|numBlocks: Int|广播的值分为多少个 block|
|id: Long| 生成Broadcast时，一个全局递增的值，用于区分不同的 Broadcast|

TorrentBroadcast 最终会被序列化, 并在 Executor 端被反序列化出来. TorrentBroadcast在序列化过程中,
成员变量`id/numBlocks/broadcastId` 等被保存下来.

## Broadcast 的流程图

![flowchart](/docs/spark/blockmanager/broadcast/BroadcastManager-flowchart.svg)

### Driver 端流程

1. 生成 TorrentBroadcast 实例
2. 在 TorrentBroadcast 类中创建 BroadcastID(id)
3. 先将待广播的对象通过 BlockManager 的 putSingle 函数，保存到driver端 BlockManager 中， 此时不会告知 BlockManagerMaster.
4. 将待广播的对象按 `blockSize` 分为不同的 chunk, 然后依次 putBytes, 保存到 driver 端的 BlockManager 中，此时会通知
   BlockManagerMaster 该 chunk 状态.
   1. 此时保存的 chunk 是待广播的值的一部分， 与之对应的是 `BroadcastBlockId(id, "piece" + i)`

### Executor 端流程

1. 反序列获得 broadcast 实例，包括类中的 `id/numBlocks/broadcastId` 变量.
2. 首先从TorrentBroadcast中的`_value`缓存中查询
3. 尝试从 BroadcastManager 的 cachedValues 中查询
4. 如果缓存没有找到, 根据 broadcastId 从本地尝试获得整个广播的 Block
5. 如果本地没有，则先根据 `BroadcastBlockId(id, "piece" + i)` 从本地查询 Block 的 chunk,
   如果本地也没有，则从远程获得 Block chunk, 最后将所有的 chunk 拼接成广播的值.
6. 最后将广播的值通过 putSingle 保存到 Executor 的 BlockManager 中，且不会通知 BlockManagerMaster.
