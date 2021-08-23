---
layout: page
title: RDD cache
nav_order: 10
parent: BlockManager
grand_parent: Spark
---

# RDD/Dataset cache 
{: .no_toc}

本文通过代码学习 RDD cache的工作机制. 本文基于 Spark 3.1.1.

## 目录
{: .no_toc .text-delta}

1. TOC
{:toc}

## RDD cache

### 示例代码

``` scala
val conf = new SparkConf()
val sc = SparkContext.getOrCreate(conf)
val rdd0 = sc.makeRDD(Array(1, 2, 3, 4), 1)
val rdd1 = rdd0.map{ v => v + 10 }
val rdd2 = rdd1.map{ v => v + 100 }.cache() //标识 rdd2 为需要cache
val rdd3 = rdd2.map{ v => v + 1000}

// 触发 rdd 执行, 并缓存 rdd2 的计算结果 (111,112,113,114)
assert(rdd3.collect().toList === List(1111, 1112, 1113, 1114))

// 从 rdd1 缓存中获得 value (111,112,113,114), 再经过计算得 (1111, 1112, 1113, 1114)
assert(rdd3.collect().toList === List(1111, 1112, 1113, 1114)) 
```

RDD 是计算和数据的载体, 所有的 SQL 最终都会转换成 RDD chain. 每个 Operator 都会触发 RDD 执行计算. 如示例代码所示,
rdd3触发了两次 `collect`,

- rdd2 没有 cache
  
  两次 collect 的计算相同, 分别为

  1. rdd0 获得数据(1,2,3,4)
  2. rdd1 对数据加10操作, 得到的结果为 (11,12,13,14)
  3. rdd2 对数据加100操作, 得到的结果为 (111,112,113,114)
  4. rdd3 对数据加1000操作, 得到的结果为 (1111,1112,1113,1114)

- rdd2.cache

  第一次 collect

  1. rdd0 获得数据(1,2,3,4)
  2. rdd1 对数据加10操作, 得到的结果为 (11,12,13,14)
  3. rdd2 对数据加100操作, 得到的结果为 (111,112,113,114), 并将结果缓存到 BlockManager 中
  4. rdd3 对数据加1000操作, 得到的结果为 (1111,1112,1113,1114)

  第二次 collect,

  1. 从 BlockManager 中获得 rdd2 缓存后的数据结果 (111,112,113,114)
  2. rdd3 对数据加1000操作, 得到的结果为 (1111,1112,1113,1114)

从这两次计算结果可以看出, RDD 缓存是将 rdd计算结果缓存到 BlockManager 中, 当后面有对该RDD操作的, 直接返回缓存结果.

### RDD cache 原理

![](/docs/spark/blockmanager/cache/rdd-cache-rdd-iterator.svg)

RDD.cache 并不会触发 cache 动作, 它只是对 RDD 的 storageLevel 赋值, 当有 RDD operator 发生时, 才会触发真正的缓存.

缓存的原理也很简单, 它将RDD的计算结果通过 BlockManager.putSingle 将整个计算结果的 iterator 保存到 BlockManager 中.

RDD 通过 `case class RDDBlockId(rddId: Int, splitIndex: Int) extends BlockId` 去正确的索引到 Block, 因为
RDD 的执行是多 task 同时执行的, 每个 task 都会缓存自己的计算结果, 因此需要 splitIndex 也是 PartitionId 来进一步标识该
RDD 缓存是由哪个 task 生成的.
