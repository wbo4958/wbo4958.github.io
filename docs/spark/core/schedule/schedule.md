---
layout: page
title: Spark Task调度
nav_order: 6
parent: core 
grand_parent: spark
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

什么是 RDD? 分布式计算的本质是 `移动数据`与`计算函数` 到计算节点进行计算并返回计算结果, 这里的移动数据仅仅表示是数据的Meta信息而非真实的数据. 在 Spark 中, RDD 即是数据与计算函数的载体. 即 Spark scheduler 会将 RDD 与 其计算函数打包发送到 Executor 计算. Spark 中有很多种不同类型的 RDD, 有些 RDD 只提供数据, 如 ParallelCollectionRDD, 而有些 RDD 只提供计算函数, 如 MapPartitionsRDD.

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

- Pool

  保存提交的 TasksetManager, 通过对应的算法如 (FIFO 和 Fair scheduling)取得 Taskset 分配计算资源

上图中绿色的框表示在 Job/Stage/Task 的创建时机, 他们的关系图所下所示

![dag-entity](/docs/spark/spark-core/schedule/dag-ActiveJob.drawio.svg)

下图是描述 Task success 的流程图

![task success](/docs/spark/spark-core/schedule/dag-Task_Success.drawio.svg)

而下图描述 Task failure 的流程图

![task failure](/docs/spark/spark-core/schedule/dag-Task_Failed.drawio.svg)

当一个 Task 失败时会重新调度该 Task, 而当该 Task 失败的次数大于 `spark.task.maxFailures` 时, 会直接停掉 Task 对应的 Stage, 进而结束掉整个 Job.

## scheduling 案例

假设现在启动了一个 Spark Standalone cluster, 有且只有一个 Worker, 其中配置了 cpu cores = 12, memory = 64G.

用户提交了一个 Application, 参数如下所示

``` console
--executor-memory = 30G
--conf spark.executor.cores=6
--conf spark.task.cpus=1
--conf spark.sql.partitions=12
```

当初始化 SparkContext, Master 会通知 Worker 启动两个 Executor (`Executor-0` (30G, 6 cpu cores), 以及  `Executor-1` (30G, 6 cpu cores)). 这两个 Executor 分别向  Driver 的StandaloneSchedulerBackend 注册消息. 此时 Driver 获知, 当前有两个 Executor 资源,总共 12 CPU cores.

### 调度 Parents stages

SparkContext 初始化完成后, Application 开始向 Spark DAG 提交 job. 假设如下所示,

![DAG demo1](/docs/spark/spark-core/schedule/dag-demo-0.drawio.svg)

整个调度过程如下所示,

- 提交 parents stage - ShuffleStage_0 和 ShuffleStage_1

1. DAG 将整个 Job 划分为 4 个 stages
2. DAG 首先提交 ResultStage_3, 发现其 parents ShuffleStage_2 还没有执行完. 将 ResultStage_3 加入到 waitingStage 中.
3. DAG 提交 ShuffleStage_2, 同理发现 parents ShuffleStage_0 和 ShuffleStage_1 都还没有执行完. 将 ShuffleStage_2 加入到 waitingStage 中.
4. DAG 先提交 ShuffleStage_0, 为其生成 4 个 ShuffleMapTask, (每个 ShuffleMapTask 计算其中一个 Partition, partitionId分别为0,1,2,3)
5. DAG 将4个 ShuffleMapTask 加入到 TaskSet 并提交给 TaskSchedulerImpl.
6. TaskSchedulerImpl 为 TaskSet (包含4个ShuffleMapTask) 创建 `TaskSetManager_0`. 并将该 `TaskSetManager_0` 加入到 Pool 中.
7. 同理 DAG 提交 ShuffleStage_1, 为其生成 10 个 ShuffleMapTask, (每个 ShuffleMapTask 计算其中一个 Partition, partitionId分别为0,1 ... 9)
8. DAG 将10个 ShuffleMapTask 加入到 TaskSet 并提交给 TaskSchedulerImpl.
9. TaskSchedulerImpl 为 TaskSet (包含10个ShuffleMapTask) 创建 `TaskSetManager_1`. 并将该 `TaskSetManager_1` 加入到 Pool 中.

- 分配计算资源

1. TaskSchedulerImpl 从 Pool 中根据相应算法(FIFO, Fair Scheduling) 获得 `TaskSetManager_0`, `TaskSetManager_1`
2. 首先为 `TaskSetManager_0` 里的 Task 分配计算资源. 当前 `Executor-0`/`Executor-1` 分别有 6 个 free core. 每个 task 要求 1 个 cpu core.
3. 因为是 round-robin 方式分配计算资源, 所以首先将 TaskSetManager_0 中的 task 0, 分配给 `Executor-0`. 此时 `Executor-0` 还剩下 5 个 free cpu core.
4. TaskSetManager_0 中的 task 1, 分配给 `Executor-1`. 此时 `Executor-1` 还剩下 5 个 free cpu core.
5. TaskSetManager_0 中的 task 2, 分配给 `Executor-0`. 此时 `Executor-0` 还剩下 4 个 free cpu core.
6. TaskSetManager_0 中的 task 3, 分配给 `Executor-1`. 此时 `Executor-1` 还剩下 4 个 free cpu core.
7. 此时已经为 TaskSetManager_0 分配好计算资源, 接下来为 TaskSetManager_1 分配计算资源
8. TaskSetManager_1 中的 task 0, 分配给 `Executor-0`. 此时 `Executor-0` 还剩下 3 个 free cpu core.
9. TaskSetManager_1 中的 task 1, 分配给 `Executor-1`. 此时 `Executor-1` 还剩下 3 个 free cpu core.
10. 依此类推, 当为 TaskSetManager_1 分配完8个Task的计算资源时, 已经没有剩余的计算资源了.

- 提交 task 到对应的  executor 端进行计算

1. 依次提交已经分配好计算资源的 task 到对应的 executor 端进行计算

- Task return
  
  假设 TaskSetManager_0 中的 Task 0 先执行完.

1. CoarseGrainedSchedulerBackend 通知 TaskSchedulerImpl Task 0 已经执行完, TaskSchedulerImpl 开始异步处理 Task 0 的返回结果. 如更新 ShuffleStage 中 pendingPartitions等
2. Task 0 完成会释放一个 Executor-0 的一个 CPU core, 这样 Executor 0 又可以调度
3. 从 Pool 中找到 TaskSetManager_0, TaskSetManager_1 开始为没有 run 的 task 分配计算资源.
4. 上一轮的分配可以看出 TaskSetManager_0 已经全部分配好了,此轮只为 TaskSetManager_1 分配计算资源.
5. 将 TaskSetManager_1 的 task 8 分配给 Executor-0, 分配完后 Executor-0不再有剩余的计算资源
6. 将 task 8 发送到 Executor-0 运行.
7. 依次可以类推:
8. TaskSetManager_0 的 task1 完成. 此时再调度 TaskSetManager_1 的 task 9 到 Executor-1 上执行

### 调度 child stage

![demo-1](/docs/spark/spark-core/schedule/dag-demo-1.drawio.svg)

1. 假设 ShuffleStage_0 中最后一个 task 执行完并返回.
2. 标记 ShuffleStage_0 finished. 从 Pool 中移除掉 TaskSetManager_0
3. DAG 从 waitingStage 中找到 ShuffleStage_0 的 child stage 也就是 ShuffleStage_2 提交.
4. DAG 发现 ShuffleStage_2 的 parents ShuffleStage_1 还没有完成. 所以需要等着 ShuffleStage_1 完成. 再次将 ShuffleStage_2 加入到 waitingStage 中.
5. 当 ShuffleStage_1 中所有 task 执行完成后. 从 Pool 中移除掉 TaskSetManager_1. 此时可用计算资源又变成了 Executor-0 6个free cpu cores, Executor-1 6个free cpu core.
6. DAG 从 waitingStage 中查找 ShuffleStage_1 的 child stage 也就是 ShuffleStage_2 提交到 TaskSchedulerImpl 中, 并创建 TaskSetManager_2
7. 分配计算资源如上图所示. 由于一轮分配已经为 ShuffleStage_1 的所有 task 分配好计算资源, 所以每当一个 task 执行完成后, 没有其它 task 再需要分配计算资源了.

### 调度 final stage

![demo-2](/docs/spark/spark-core/schedule/dag-demo-2.drawio.svg)

- ShuffleStage_1 所有的 Task 执行完成并返回后.
- 标记 ShuffleStage_1 finished. 从 Pool 中移除掉 TaskSetManager_1
- DAG 从 waitingStage 中找到 ShuffleStage_1 的 child stage 也就是 ResultStage_3 提交到 TaskSchedulerImpl, 并创建 TaskSetManager_3.
- 分配计算资源如上图所示.
- 当 ResultTask 返回时, 更新最新的 partition 结果到对应的数组时, 当所有的 ResultTask 执行完成后, 整个 job 结束.
