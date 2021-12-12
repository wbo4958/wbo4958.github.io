---
layout: page
title: Spark Task调度
nav_order: 6
parent: SparkCore 
grand_parent: Spark
---

# Spark 调度
{: .no_toc}

> 本文基于 Spark 3.2.0 学习 Spark 对于 Task 的调度过程.

## 目录
{: .no_toc .text-delta}

1. TOC
{:toc}

## OverView

SparkContext 通过调用 `DAGScheduler.submitJob` 来向 Spark Cluster 提交 Spark Job,

``` scala
def submitJob[T, U](
    rdd: RDD[T], // 在 RDD 上运行Spark tasks
    func: (TaskContext, Iterator[T]) => U, // RDD里每个 Partition 的计算函数
    partitions: Seq[Int], // RDD 里需要计算的 partition 集合
    callSite: CallSite,
    resultHandler: (Int, U) => Unit, // 更新对应 partition 的计算结果
    properties: Properties): JobWaiter[U] = {
```

从 submitJob 的参数来看, Spark Scheduler 对用户是不可见的. 用户只关心将 job 提交给 Spark 后能得到正确的结果,且 performance 要好.

什么是 RDD? 分布式计算的本质是 `移动数据`与`计算函数` 到计算节点进行计算并返回计算结果. 在 Spark 中, RDD 即是数据与计算函数的载体. 即 Spark scheduler 会将 RDD 与 其计算函数打包发送到 Executor 计算. Spark 中有很多种不同类型的 RDD, 有些 RDD 只提供数据, 如 ParallelCollectionRDD, 而有些 RDD 只提供计算函数, 如 MapPartitionsRDD.s

下面这张图基本上描述了 Spark 怎么样调度一个 Task.

![spark schedule](/docs/spark/spark-core/schedule/dag-schedule.drawio.svg)

上图涉及到很多类,

- DAGScheduler
  
  根据 RDD chain 划分不同的 Stages 有 (ResultStage, ShuffleMapStage), 创建 Job. 并为 Stage 中每个待计算的 Partition 创建对应的 Task 有 (ResultTask, ShuffleMapTask). 最后将这些 Task 生成 TaskSet 提交给 TaskSchedulerImpl.

- TaskSchedulerImpl

  为每一个 TaskSet 生成一个 TaskSetManager, 并将该 TaskSetManager 提交给 Pool (有 FIFO 与 FailSchduler 之分), 并为 Task 分配计算资源. 生成 TaskDescription

- StandaloneSchedulerBackend
  
  与 Executor 进行通信, 管理 Executor 可用计算资源, 将已经分配好计算资源的 Task 发送到 Executor 端执行.

- TaskSetManager

  管理一个 TaskSet 中所有的 Task, 根据 Executor 信息在 Task 中找到一个对应的 Task (如 locality 信息, 如果没有相关信息,则 round robin 方式为其中的task分配计算资源) 分配对应计算资源. 并处理 successful/failed 的task.

上图中绿色的框表示在 Job/Stage/Task 的创建时机, 他们的关系图所下所示

![dag-entity](/docs/spark/spark-core/schedule/dag-ActiveJob.drawio.svg)

