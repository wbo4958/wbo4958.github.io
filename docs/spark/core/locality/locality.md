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

## 流程图

![](/docs/spark/core/locality/locality.drawio.svg)


### ShuffleStage

makeRDD 创建一个带有 partition location 的 ParallelCollectionRDD. Spark 首先提交 ShuffleStage.

1. 根据待计算的 partition ID, 获得该 partition 对应的 Location. 最终会从 ParallelCollectionRDD.preferredLocations 中获取.
2. 创建 Task, 并获得其 TaskLocation 信息. 对于本例, 每个 Partition 会有一个 HostTaskLocation 指明其数据在哪个 Host.
3. 创建 TaskSetManager, 并通过 addPendingTasks 将 tasks 分配到不同等级的 list 中, 比如
  - forExecutor: 数据在 process level 的 partition
  - forHost: 数据在host level 的 partition
  - forRack: 数据在相同 Rack 的 partition
  - noPrefs: 表示没有 locality 信息的 partition
  - all: 所有的 partitions
4. 随后计算出该 TaskSetManager 的 locality levels. 比如本例中, 所有的数据都在 forHost 中, 因此最后的 Locality Levels = (NODE_LOCAL, ANY). 这里有 *ANY* 是因为所有的 partition 都在 *all* 里.

### offer resource

当 Tasks 与 WorkerOffer 准备好后, Spark 开始 offer resources 给 tasks.

1. 从 Pool 里获得一个 TaskSetManager
2. 依次遍历 TaskSetManager 的 locality levels, 本例为(NODE_LOCAL, ANY)
3. 然后依次遍历 worker offers 进行 resource offer.
4. 获得当前 allowed locality level
  - a. 依次遍历 TaskSetManager 的 myLocalityLevels (除去最后一个)
  - b. 从 a 遍历的 Level 从对应的 forExecutor (PROCESS_LOCAL)/forHost (NODE_LOCAL)/noPrefs (NO_PREFS)/forRack (RACK_ONLYs) 中查找是否还有 task 需要 offer.
  - c. 如果没有 task了, 则返回 a 继续处理下一个 level.
  - d. 如果对应的 list 还有 task, 但是已经超过了 locality 等待时间 (3s), 则依然返回 a, 处理下一个 level
  - e. 如果等待时间没有 time out, 直接返回当前的 locality level.
5. 首先尝试从 forExecutor 里查找哪些 task 的数据在同一个 host 上, 如果找到直接返回 
6. 根据 max allowed locality level, 依次从 forHost/noPrefs/forRack 查找, 如果找到直接返回, 如果没有找到, 则从 *all* 里返回.
7. 重复3, 2, 直到所有 task 都被 offer 了resource, 或者所有 worker resources 已经被 assign 完了
8. send task 到 executor 执行.

### ResultStage

当计算完 shuffle stage 后, 最后计算 ResultStage, 而该 ResultStage 的输入也就是 ShuffleMapStage 的输出. 其过程大部分与 ShuffleMapStage 相同, 但是在获得 partition location 时不同, 具体如下,

1. 调用 ShuffledRDD.getPreferredLocations 准备获得每个 partition 的 location
2. 如果 `spark.shuffle.reduceLocality.enabled=false`, 则直接返回 empty list, 表示 shuffle read 不需要 locality 信息.
3. 如果 `shuffle write`/`shuffle read` 的partition数量超过 1000, 也直接返回 empty list, 表示 shuffle read 不需要 locality 信息.
4. 然后根据 reducer task mapId 获得 MapStatus, 并获得该 reducerId block size. 最后计算 `reducer block size / total block size`, 如果大于等于 0.2 则返回该 host 作为 locality 信息.
