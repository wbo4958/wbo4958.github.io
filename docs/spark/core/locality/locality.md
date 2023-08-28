---
layout: page
title: Locality
nav_order: 12
parent: core 
grand_parent: spark
---

# Spark Locality
{: .no_toc}

本文基于 spark 3.4 学习 spark locality.

分布式计算的目的是移动计算, 因为相对于移动`动则几百G`的数据, 移动几行计算代码是非常高效的.
如果计算函数与数据在同一个节点, 甚至是在同一个进程中, 那这时的计算将会非常高效. 而反之如果
计算与数据不在同一个节点, 那可能会出现跨网络读取大量的数据进行处理, 这会大大的影响性能.

下面就来学习 spark 的 data locality.

## 目录
{: .no_toc .text-delta}

1. TOC
{:toc}

## 测试代码

``` bash
Thread.sleep(6000) // wait 6s for all executors connecting with driver

// The hosts of worker nodes are 192.168.23.10 and 192.168.23.22
var rdd = sc.makeRDD(
  List(
    (1000, List("192.168.23.10")),
    (1001, List("192.168.23.10")),
    (1002, List("192.168.23.10")),
    (1003, List("192.168.23.10")),
    (1004, List("192.168.23.10")),
    (1005, List("192.168.23.10")),
    (1006, List("192.168.23.10", "192.168.23.22")),
    (1007, List("192.168.23.10", "192.168.23.22")),
  )
)

rdd.repartition(2).collect()
```

上面 Spark Job 有 2 个 stage, 第一个 shuffle stage 有8个partition, 其中 partition 6, 7 在 192.168.23.10, 192.168.23.22 节点上都有的数据.
最后一个 ResultStage 总共有 2 个 partition.

## 流程国

![](/docs/spark/core/locality/locality.drawio.svg)


### ShuffleStage

makeRDD 创建一个 ParallelCollectionRDD 且带有每个 partition location 信息. Spark 首先执行 ShuffleStage, 



