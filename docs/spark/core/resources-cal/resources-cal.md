---
layout: page
title: Spark Resources Calculation
nav_order: 15
parent: core 
grand_parent: spark
---

# Spark Resource Calculation
{: .no_toc}

本文用于学习 Spark PR [GPU resource calculation](https://github.com/apache/spark/pull/43494) 

## 目录
{: .no_toc .text-delta}

1. TOC
{:toc}

## Issue

- Environment

Spark Standalone 3.5 Cluster, 只有一个 worker 节点，该 Worker 上有 8 cpu cores, 30G mem, 1 NVIDIA GPU.

- Spark Application

``` shell
spark-shell --master spark://192.168.0.105:7077 \
  --conf spark.executor.resource.gpu.amount=1 \
  --conf spark.task.resource.gpu.amount=0.125 \
  --conf spark.executor.cores=8 \
  --conf spark.task.cpus=1
```

测试代码,

``` scala
import org.apache.spark.resource.{ResourceProfileBuilder, TaskResourceRequests}
val rdd = sc.range(0, 20, 1, 12)
var rdd1 = rdd.repartition(3)

val treqs = new TaskResourceRequests().cpus(1).resource("gpu", 1.0)
val rp = new ResourceProfileBuilder().require(treqs).build

rdd1 = rdd1.withResources(rp)
rdd1.collect()
```

从上面的 spark 配置可以看出，task 的并行度为8, 也就是任意时刻最多有 8 个 task 同时运行，而该 Spark Application 分为两个 stage: 一个shuffle stage有 12 个 task, 另一个result stage 有 3 个 task，而 result stage 请求不同的 resource, 每个 Task 要求 1 个 GPU.

根据上述Spark配置，可以看出Task的并行度为8，也就是在任意时刻最多有8个Task同时运行。该Spark应用程序分为两个阶段：一个ShuffleStage (有12个Task)，另一个ResultStage (有3个 Task)。而ResultStage 需要请求不同的资源, 即每个Task需要1个GPU.

- Result

**Shuffle Stage**

![shuffle stage](/docs/spark/core/resources-cal/shuffle.png)

**Result Stage**

![result stage](/docs/spark/core/resources-cal//result-stage.png)

从上面的结果来看, ResultStage 中的 task 并行运行了，从代码来看，因为每个 task 请求 1 个 GPU, 所以这些task应该串行运行才是正确的. 那到底哪里出现问题了?

## BackGround

在找到 root cause 前，我们先了解一些 Spark Resource 相关的背景知识.

### ReourceProfile

从3.1版本开始，Spark将所有与资源相关的配置选项整合到ResourceProfile中。每个Spark应用程序都会解析与资源相关的配置（例如`spark.executor.cores`，`spark.task.cpus`，`spark.executor.memory`等），以生成默认的ResourceProfile。除了默认的ResourceProfile之外，还有自定义的ResourceProfile，即使用代码生成的ResourceProfile。这些ResourceProfile根据是否启用/禁用动态分配又可以细分为TaskResourceProfile和ResourceProfile。

- TaskResourceProfile

当 Dynamic Allocation Off 时, ResourceProfile 变为 TaskResourceProfile, 而该 TaskResourceProfile 只会修改 stage/task 的资源请求, 而不会请求新的 Executors.

- ResourceProfile

当 Dynamic Allocation Off 时, 新的 ResourceProfile 会申请新的 Executor, 并且同时会修改 stage/task 的资源请求.




