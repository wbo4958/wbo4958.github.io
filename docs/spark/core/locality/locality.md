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


注意: spark 分别为 PROCESS_LOCAL/NODE_LOCAL/RACK_LOCAL 设置为默认的 timeout 时间, 默认 `spark.locality.wait=3s` (表示 3s 超时后, 开始选择下一个 locality level). 但是也可以分别设置不同的 timeout 时间, 分别由 `spark.locality.wait.process`, `spark.locality.wait.node`, `spark.locality.wait.rack` 指定.

### ResultStage

当计算完 shuffle stage 后, 最后计算 ResultStage, 而该 ResultStage 的输入也就是 ShuffleMapStage 的输出. 其过程大部分与 ShuffleMapStage 相同, 但是在获得 partition location 时不同, 具体如下,

1. 调用 ShuffledRDD.getPreferredLocations 准备获得每个 partition 的 location
2. 如果 `spark.shuffle.reduceLocality.enabled=false`, 则直接返回 empty list, 表示 shuffle read 不需要 locality 信息.
3. 如果 `shuffle write`/`shuffle read` 的partition数量超过 1000, 也直接返回 empty list, 表示 shuffle read 不需要 locality 信息.
4. 然后根据 reducer task mapId 获得 MapStatus, 并获得该 reducerId block size. 最后计算 `reducer block size / total block size`, 如果大于等于 0.2 则返回该 host 作为 locality 信息.


### 调度log

- shuffle stage

``` console
23/09/05 09:06:46 INFO TaskSetManager: Starting task 0.0 in stage 0.0 (TID 0) (192.168.23.10, executor 1, partition 0, NODE_LOCAL, 7309 bytes)
23/09/05 09:06:46 INFO TaskSetManager: Starting task 6.0 in stage 0.0 (TID 1) (192.168.23.22, executor 0, partition 6, NODE_LOCAL, 7309 bytes)
23/09/05 09:06:46 INFO TaskSetManager: Starting task 1.0 in stage 0.0 (TID 2) (192.168.23.10, executor 1, partition 1, NODE_LOCAL, 7309 bytes)
23/09/05 09:06:46 INFO TaskSetManager: Starting task 7.0 in stage 0.0 (TID 3) (192.168.23.22, executor 0, partition 7, NODE_LOCAL, 7309 bytes)
23/09/05 09:06:46 INFO TaskSetManager: Starting task 2.0 in stage 0.0 (TID 4) (192.168.23.10, executor 1, partition 2, NODE_LOCAL, 7309 bytes)
23/09/05 09:06:46 INFO TaskSetManager: Starting task 3.0 in stage 0.0 (TID 5) (192.168.23.10, executor 1, partition 3, NODE_LOCAL, 7309 bytes)
23/09/05 09:06:46 INFO TaskSetManager: Starting task 4.0 in stage 0.0 (TID 6) (192.168.23.10, executor 1, partition 4, NODE_LOCAL, 7309 bytes)
23/09/05 09:06:46 INFO TaskSetManager: Starting task 5.0 in stage 0.0 (TID 7) (192.168.23.10, executor 1, partition 5, NODE_LOCAL, 7309 bytes)
```

对于 shuffle stage, 有两个 worker offer, 分别为 (192.168.23.10, 192.168.23.22), 由于 offer resource 是 round robin way,

1. 遍历 offers, 得到 192.168.23.10, 从 TaskSetManager 中 forHost.get("192.168.23.10") 获得需要调度的 partition 0
2. 遍历 offers, 得到 192.168.23.22, 从 TaskSetManager 中 forHost.get("192.168.23.22") 获得需要调度的 partition 6
3. 重复 1, 2, 依次调度 partition 1, 7.
4. 在 step 3 后, 192.168.23.22 上的 partition 已经没有待计算的 partition 了
5. 遍历 offers, 得到 192.168.23.10, 从 TaskSetManager 中 forHost.get("192.168.23.10") 获得需要调度的 partition 2
6. 遍历 offers, 得到 192.168.23.12, TaskSetManager 中 forHost.get("192.168.23.12") 中已经没有partition, pass
7. 遍历 offers, 得到 192.168.23.10, 从 TaskSetManager 中 forHost.get("192.168.23.10") 获得需要调度的 partition 3
8. 遍历 offers, 得到 192.168.23.12, TaskSetManager 中 forHost.get("192.168.23.12") 中已经没有partition, pass
5. 遍历 offers, 得到 192.168.23.10, 从 TaskSetManager 中 forHost.get("192.168.23.10") 获得需要调度的 partition 4
6. 遍历 offers, 得到 192.168.23.12, TaskSetManager 中 forHost.get("192.168.23.12") 中已经没有partition, pass
7. 遍历 offers, 得到 192.168.23.10, 从 TaskSetManager 中 forHost.get("192.168.23.10") 获得需要调度的 partition 5
8. 遍历 offers, 得到 192.168.23.12, TaskSetManager 中 forHost.get("192.168.23.12") 中已经没有partition, pass

- result stage

``` console
23/09/05 09:06:47 INFO TaskSetManager: Starting task 0.0 in stage 1.0 (TID 8) (192.168.23.10, executor 1, partition 0, NODE_LOCAL, 7461 bytes)
23/09/05 09:06:47 INFO TaskSetManager: Starting task 1.0 in stage 1.0 (TID 9) (192.168.23.22, executor 0, partition 1, NODE_LOCAL, 7461 bytes)
```

result tasks 的 数据 在两个 worker 上都有, 所以它们的 location 信息都是 (192.168.23.10, 192.168.23.22), 最后的调度信息如上所示.

### PROCESS LOCAL

上面的例子是数据在 host 节点, 但是这并不是最优的情况, 因为即使数据在 host, 对于 task 也有可能会从 host 的 disk 直接读取数据,此时会涉及 磁盘 IO, 而 磁盘 IO 本身是比较耗时的操作. 那有没有一种更快的 locality. 如果数据已经在 executor 进程中也就是 PROCEE_LOCAL, 此时 task 直接从内存中获得数据, 这时是最快的选项.

那什么样的数据会已经存在于 executor 中了呢?

从 TaskSetManager.addPendingTask 代码中可以看出

``` scala
    for (loc <- tasks(index).preferredLocations) {
      loc match {
        case e: ExecutorCacheTaskLocation =>
          pendingTaskSetToAddTo.forExecutor.getOrElseUpdate(e.executorId, new ArrayBuffer) += index
        case e: HDFSCacheTaskLocation =>
          val exe = sched.getExecutorsAliveOnHost(loc.host)
          exe match {
            case Some(set) =>
              for (e <- set) {
                pendingTaskSetToAddTo.forExecutor.getOrElseUpdate(e, new ArrayBuffer) += index
              }
              logInfo(s"Pending task $index has a cached location at ${e.host} " +
                ", where there are executors " + set.mkString(","))
            case None => logDebug(s"Pending task $index has a cached location at ${e.host} " +
              ", but there are no executors alive there.")
          }
        case _ =>
      }
      ...
```

当 TaskLocation 为 ExecutorCacheTaskLocation 或 HDFSCacheTaskLocation 时, 此时表示数据已经与 executor 在同一个进程中了.

- ExecutorCacheTaskLocation

比如 kafka 过来的数据, 或都是 streaming 相关, 又或者是 BlockManager 里面的数据.

``` console
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
rdd.cache().repartition(2).collect()
rdd.collect()
```

上面的例子在 rdd 后加了一个 cache, 那么在第一次 collect 时, 会将 rdd 的计算结果 cache 到  BlockManager 中. 第二次 collect, 那直接就从 BlockManager 里去取, 并不会再重新计算 RDD.

```
23/09/07 06:41:05 INFO TaskSetManager: Starting task 0.0 in stage 2.0 (TID 10) (192.168.23.10, executor 1, partition 0, PROCESS_LOCAL, 7320 bytes)
23/09/07 06:41:05 INFO TaskSetManager: Starting task 6.0 in stage 2.0 (TID 11) (192.168.23.22, executor 0, partition 6, PROCESS_LOCAL, 7320 bytes)
23/09/07 06:41:05 INFO TaskSetManager: Starting task 1.0 in stage 2.0 (TID 12) (192.168.23.10, executor 1, partition 1, PROCESS_LOCAL, 7320 bytes)
23/09/07 06:41:05 INFO TaskSetManager: Starting task 7.0 in stage 2.0 (TID 13) (192.168.23.22, executor 0, partition 7, PROCESS_LOCAL, 7320 bytes)
23/09/07 06:41:05 INFO TaskSetManager: Starting task 2.0 in stage 2.0 (TID 14) (192.168.23.10, executor 1, partition 2, PROCESS_LOCAL, 7320 bytes)
23/09/07 06:41:05 INFO TaskSetManager: Starting task 3.0 in stage 2.0 (TID 15) (192.168.23.10, executor 1, partition 3, PROCESS_LOCAL, 7320 bytes)
23/09/07 06:41:05 INFO TaskSetManager: Starting task 4.0 in stage 2.0 (TID 16) (192.168.23.10, executor 1, partition 4, PROCESS_LOCAL, 7320 bytes)
23/09/07 06:41:05 INFO TaskSetManager: Starting task 5.0 in stage 2.0 (TID 17) (192.168.23.10, executor 1, partition 5, PROCESS_LOCAL, 7320 bytes)
```

上面可以看出, LOCALITY LEVEL 已经变成 PROCESS_LEVEL 了. 其实 Spark 做法很简单

在第一次 collect 过程中, DAGscheduler.getCacheLocs 检查到 ParallelRDD 的 storage level 不为 None, 同时 rdd 还没被计算, 也就是 BlockManager 中还没有其数据,所有这里直接返回为空, 直接从 ParallelRDD 中获得 partition locality 信息.

在第二次 collect 过程中. rdd 的计算结果已经缓存到了 BlockManager 中. DAGscheduler.getCacheLocs 会生成 ExecutorCacheTaskLocation 并将其保存到 cacheLocs 中, 并返回. 所以第二次的数据将直接从 executor 所在的进程 memory 直接读取.

- HDFSCacheTaskLocation

HDFSCacheTaskLocation 表示 HDFS 数据已经读取到 executor memory 中了. 比如 HadoopRDD 里获得的 Location.
