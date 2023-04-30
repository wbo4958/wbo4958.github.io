---
layout: page
title: Dynamic Allocation
nav_order: 7
parent: core 
grand_parent: spark
---

# Dynamic Allocation
{: .no_toc}

> 本文基于 Spark 3.4.0 学习 Spark Dynamic Allocation.

## 目录
{: .no_toc .text-delta}

1. TOC
{:toc}

## 测试用例

``` scala
val conf = new SparkConf()
  .set("spark.executor.cores", "7")
  .set("spark.task.cpus", "2")
  .set("spark.dynamicAllocation.enabled", "true")
  .set("spark.executor.memory", "1314M")

val sc = new SparkContext(
  "spark://xxx:7077",
  "stage level scheduling", conf)

val rdd = sc.range(0, 90, 1, 87).mapPartitions(itr => {
  Thread.sleep(1000000)
  itr
})

rdd.collect()
```

测试环境,

standalone cluster, 只有一个 Worker (12 cpu cores, 60G memory), 从上面可知,　总共有 87 个 partition,
对应 87 个 task. 但是 cluster 里只能申请一个 Executor, 每个 Executor 可以并行运行　7/2 = 3 个 task.

## Background

动态资源分配是分配 running/pending tasks 的个数动态调整 Executor
动态资源分配 (dynamic resource allocation) 是相对于静态分配. 那什么是静态分配呢? 当用户通过配置文件比如

- 对于 yarn
  - `--num-executor` 就决定了 Yarn 分配多少个 executor 给该 Spark Application
- 对于 Standalone, 通过 `--spark.executor.cores` 也同样决定了 这个 Application 分配到的 executor 数量.

那静态分配有什么缺点呢? 静态分配对于不同的 stage, 都是同样的 executor. 如果有的 stage task 数量很少，那 executor 就会资源浪费，而如果 stage task 数量很多，那已经分配好的 executor 资源又不太够用. 另外静态分配会导致 cluster 利用率降低 (同时跑 application 的个数也会被限制)

那什么是动态分配呢？ 不同 Stage 可以动态调整 executor 资源,　以及该 Stage 的 task 资源, 可增可减.

那动态分配有什么缺点呢？ 因为是动态分配与删减，这样会导致 kill executor 进程 以及 launch Executor overhead. 另外，静态分配是一次性将 executor 分配好，executor 的启动都可以是并行的。而如果变成动态分配，那 executor 的启动可以看成是串行的，可能会有 performance issue.

### Dynamic allocation 配置

从官网上来看到目前任何 coarsed-grained cluster， 比如 Standalone, yarn 是支持动态资源分配的 [dynamic-resource-allocation](http://spark.apache.org/docs/latest/job-scheduling.html#dynamic-resource-allocation)

另外 [dynamic-configuration](http://spark.apache.org/docs/latest/configuration.html#dynamic-allocation) 列出了 dynamic resource allocation 相关的配置项.

下面以 standalone 来分析 动态分配的是怎么实现的. 要在 Standalone 上 enable 动态资源分配，需要配置如下.

``` xml
--conf spark.dynamicAllocation.enabled=true \
```

### Disable 静态资源分配

动态资源分配首先要做的就是禁掉静态分配。 很简单, 静态分配发生在 Driver 向 Master 注册 Application 时， ApplicationDescription 里有一个 initialExecutorLimit, 默认是 None, 即表示是静态分配. 如果改成具体的值，则就禁止了静态分配.

注意：这里的禁止并不是完全的禁止，只是初始化的 executor 数量被限制了，否则如果有无数个 Worker, 那静态分配可能会分配无数个 executor.

``` scala
// StandaloneSchedulerBackend.scala

// If we're using dynamic allocation, set our initial executor limit to 0 for now.
// ExecutorAllocationManager will send the real initial limit to the Master later.
val initialExecutorLimit = //初始化的动态分配个数
  if (Utils.isDynamicAllocationEnabled(conf)) {
    Some(0) //如果动态分配 enable, 默认一个也不需要分配
  } else {
    None
  }

// 向 ApplicationDescription 设置 initialExecutorLimit
val appDesc = ApplicationDescription(sc.appName, maxCores, command,
    webUrl, defaultProfile = defaultProf, sc.eventLogDir, sc.eventLogCodec, initialExecutorLimit)
```

## Overview

![flowchart](/docs/spark/spark-core/dynamic-allocation/dynamic-allocation-flow.drawio.svg)


默认 initialNumExecutors = 0

- **Step 1. tryRegisterAllMasters**

当 StandaloneAppClient 启动后, 会向 Master 注册 Application, 默认的 ResourceProfile 解析
出来如下所示,

``` console
Profile: id = 0,
    executor resources:
        cores -> name: cores, amount: 7, script: , vendor: ,
        memory -> name: memory, amount: 1314, script: , vendor: ,
        offHeap -> name: offHeap, amount: 0, script: , vendor: ,
    task resources:
        cpus -> name: cpus, amount: 2.0
```

由于默认的 `initialNumExecutors=0`, 则 Master 对于该 RegisterApplication 并不会在 Worker
上分配 Executor.

- **Step 2. ExecutorAllocationManager.start**

``` console
numExecutorsTargetPerResourceProfileId(defaultRpId)=0
onStageSubmitted -> 
    numExecutorsToAddPerResourceProfileId(defaultRpId)=1
```

由于 `initialNumExecutors=0`, 所以,　此时 Master 即使收到 RequestExecutors, 也并不会
向 Worker 请求 Executor

- **Step 3. onStageSubmitted**

onStageSubmitted 后会更新变量 addTime,　这时 schedule 才能正常的运行

- **Step 4. schedule**

第一次schedule
  
> **此时 numExecutorsToAddPerResourceProfileId=1 numExecutorsTargetPerResourceProfileId=0**

``` console
updateAndSyncNumExecutorsTarget ->
  addExecutorsToTarget
    numExecutorsTargetPerResourceProfileId=numExecutorsTargetPerResourceProfileId+numExecutorsToAddPerResourceProfileId
    numExecutorsTargetPerResourceProfileId = 1

推测出 => updates = TargetNumUpdates(delta=1, oldNumExecutorsTarget=0)
doUpdateRequest
  => 向 cluster 请求 numExecutorsTargetPerResourceProfileId＝１个executor
  => 因为 updates中 delta == numExecutorsToAddPerResourceProfileId = 1, 所以
   更新  numExecutorsToAddPerResourceProfileId = numExecutorsToAddPerResourceProfileId*2=2
```

第二次schedule

> **此时 numExecutorsToAddPerResourceProfileId=2 numExecutorsTargetPerResourceProfileId=1**

``` console
updateAndSyncNumExecutorsTarget ->
  addExecutorsToTarget
    numExecutorsTargetPerResourceProfileId=numExecutorsTargetPerResourceProfileId+numExecutorsToAddPerResourceProfileId
    numExecutorsTargetPerResourceProfileId = 3

推测出 => updates = TargetNumUpdates(delta=2, oldNumExecutorsTarget=1)
doUpdateRequest
  => 向 cluster 请求 numExecutorsTargetPerResourceProfileId＝2 个executor
  => 因为 updates中 delta == numExecutorsToAddPerResourceProfileId = 2, 所以
   更新  numExecutorsToAddPerResourceProfileId = numExecutorsToAddPerResourceProfileId*2=4
```

第三次 schedule

> **此时 numExecutorsToAddPerResourceProfileId=4 numExecutorsTargetPerResourceProfileId=3**

``` console
updateAndSyncNumExecutorsTarget ->
  addExecutorsToTarget
    numExecutorsTargetPerResourceProfileId=numExecutorsTargetPerResourceProfileId+numExecutorsToAddPerResourceProfileId
    numExecutorsTargetPerResourceProfileId = 7

推测出 => updates = TargetNumUpdates(delta=4, oldNumExecutorsTarget=3)
doUpdateRequest
  => 向 cluster 请求 numExecutorsTargetPerResourceProfileId＝4 个executor
  => 因为 updates中 delta == numExecutorsToAddPerResourceProfileId = 4, 所以
   更新  numExecutorsToAddPerResourceProfileId = numExecutorsToAddPerResourceProfileId*2=8
```

依此类推 ...

第四次　schedule 请求 8　个 executor.

因为总共只请求 `(总共87个task/3个同时run的task)` = 29 个 executor. 前面４次已经请求了 (1+2+4+8)=15 个了,

所以　每五次　schedule 只能请求 `29-15=14` 个 executor,
后面的　schedule 不会在请求 executor 了.
