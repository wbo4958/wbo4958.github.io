---
layout: page
title: Standalone deploy
nav_order: 15
parent: Spark 
---

# Spark SQL window
{: .no_toc}

Spark 支持多种集群的 deploy, 包括 Standalone, Yarn, Kubernets, Mesos, 其中 Standalone 是 Spark 自带的不依赖第三方的集群模式. 本文学习 Spark Standalone 的 deploy 过程以及资源(包括 Spark 3.0 新加的 GPU/FPGA 资源)等相关知识. 本文基于 Spark 3.1.1.

## 目录
{: .no_toc .text-delta}

1. TOC
{:toc}

## Overview

![cluster overview](/docs/spark/standalone/cluster-overview.png)

上图是[spark官网](https://spark.apache.org/docs/latest/cluster-overview.html)上关于 Spark cluster的 overview. 对于 Standalone 来说, 上图主要涉及到 Master/Worker/Driver/Executor 4个模块.

- Master

也就是Spark自带的 Standalone Manager, Master是spark的主节点, 它负责管理Worker, Application, 并负责为 Spark Application 在worker上分配executor等。

- Worker

管理Worker所在Node上被分配的资源, 并负责启动Executor

- Executor

负责执行 Spark Application 的Task

- Driver

用户程序, 主要是对于用户隐藏的 SparkContext, **每个 Application 只有一个 SparkContext**. 它负责往 Master 中注册 Application, 并接收 Executor 的注册, 接着调度Task, 将 Task 发送到 Executor 端执行, 并接收 Task 运行后的结果返回给用户.

## Worker 资源

在 Spark Standalone Worker中, 资源是 cpu cores/ memory 和其它硬件如 GPU/ FPGA等的虚拟资源，并不是获得真实的cpu, 只是一种计算单位. Worker 在初始化时都会设置可用资源.

- cores
  
  用户可以通过 SPARK_WORKER_CORES 设置 worker 可用的 cpu cores, 如果没有设置， worker 会使用系统中物理 cpu cores 作为 worker的 cores 资源.

- memory

  用户可以通过 SPARK_WORKER_MEMORY 设置 worker 可用的 memory, 如果没有设置， worker 会使用系统中物理内存 - 1G 作为 worker 的 memory 资源.

- GPU 资源, FPGA资源一样

  对于 GPU 资源，有两种方式指定, 一种是事先分配，另一种是写代码动态发现.

  - Allocated
  
    该方式主要是通过 spark.worker.resourcesFile 指定的配置文件来事先分配给该 worker 相关资源

    如创建 gpu_fpga_conf.json 文件, 内容如下

    ``` json
    [
      { 
        "id": {
          "componentName": "spark.worker",
          "resourceName": "gpu"
        },
        "addresses": [
          "0",
          "1"
        ]
      },
      { 
        "id": {
          "componentName": "spark.worker",
          "resourceName": "fpga"
        },
        "addresses": [
          "f1",
          "f2",
          "f3"
        ]
      }
    ]
    ```

  - Discovery

    该方式主要是通过 spark.worker.resource.gpu.discoveryScript 指定的脚本在 runtime 时动态发现相关资源.

    创建 getGpuResources.sh

    ``` bash
    # Example output: {"name": "gpu", "addresses":["0","1","2","3","4","5","6","7"]}

    ADDRS=`nvidia-smi --query-gpu=index --format=csv,noheader | sed -e ':a' -e 'N' -e'$!ba' -e 's/\n/","/g'`
    echo {\"name\": \"gpu\", \"addresses\":[\"$ADDRS\"]}
    ```
  
  Worker 在启动过程中通过 setupWorkerResources 函数来获得相关资源

  ``` scala
  private def setupWorkerResources(): Unit = {
    try {
      resources = getOrDiscoverAllResources(conf, SPARK_WORKER_PREFIX, resourceFileOpt)
      logResourceInfo(SPARK_WORKER_PREFIX, resources)
    } catch {
      // ...
    }
    resources.keys.foreach { rName =>
      resourcesUsed(rName) = MutableResourceInfo(rName, new HashSet[String])
    }
  }

  def getOrDiscoverAllResources(
      sparkConf: SparkConf,
      componentName: String,
      resourcesFileOpt: Option[String]): Map[String, ResourceInformation] = {
    val requests = parseAllResourceRequests(sparkConf, componentName)
    val allocations = parseAllocatedOrDiscoverResources(sparkConf, componentName, resourcesFileOpt)
    assertAllResourceAllocationsMeetRequests(allocations, requests)
    val resourceInfoMap = allocations.map(a => (a.id.resourceName, a.toResourceInformation)).toMap
    resourceInfoMap
  }
  ```
  

## Standalone Deploy

![standalone deploy](/docs/spark/standalone/standalone-deploy.svg)

**step 1:**

- Master 启动后 **对于非 recoveryMode** 模式的情况下, Master 通过 MonarchyLeaderAgent 选择该 Master 作为 Leader. 状态从 STANDBY 变为 ALIVE. 后面所有的操作都是基本 ALIVE 的, 否则会直接pass掉.

**step 2:**

- Worker 启动后会设置 worker 所有的资源.

**step 2 - step 4**

- Worker向Master发起注册信息, Master会给该Worker分配ＷorkerInfo用来表示该Worker的一些资源与属性, 并将该WorkerInfo保存到Master的本地变量中, 请参考第一节
- Worker收到Master的注册成功信息后, 会开启心跳线程定时向worker发送HeartBeat,保持连接. 接着向Master报告WorkerLasteState, 主要是excutors和drivers

**step 5 - step ６**

- App向Master注册Application,　Master为该Application生成ApplicationInfo, 并保存到Master的变量中, 请参考第一节

**step 7 - step 11**
- Master在注册的worker中遍历找到可用的worker(根据worker的状态以及Application需要的资源,worker是否满足))
- 然后根据Application的需求, 分配合适的Executor给Application,并通知worker开启executor进程, 用来执行Application的Command
- Worker收到Master的LaunchExecutor信息后, 生成一个ExecutorRunner线程来启动新executor进程

**step 12 - step 14**
- ExecutorRunner开启新进程并运行Application的main函数, 也就是`CoarseGrainedExecutorBackend`
- CoarseGrainedExecutorBackend会先从Driver中获得App的配置信息, 然后向Driver注册Executor,即发送RegisterExecutor消息
- CoarseGrainedExecutorBackend生成Executor类来处理App端提交过来的task

# 3. Master如何分配Executor的

这里的schedule主要是master为waitingApps调度可用的resources, 代码流程如下所示(`主要是step5-step8`)

``` java
schedule ->
startExecutorsOnWorkers ->

private def startExecutorsOnWorkers(): Unit = {
  // Right now this is a very simple FIFO scheduler. We keep trying to fit in the first app
  // in the queue, then the second app, etc.
  for (app <- waitingApps) { // waitingApps表示step5向Master注册的Application
    //coresPerExecutor是Application指定处理该Application时, executor所使用的cpu cores个数, 如果没有指定, 则默认为１个
    val coresPerExecutor = app.desc.coresPerExecutor.getOrElse(1) 
    // If the cores left is less than the coresPerExecutor,the cores left will not be allocated
    //　一般情况下app.coresLeft会非常大, 所以会进入if分支
    if (app.coresLeft >= coresPerExecutor) {
      // Filter out workers that don't have enough resources to launch an executor
      // canLaunchExecutor主要判断该worker上free的cpu cores/memory/resource是否满足application需要
      val usableWorkers = workers.toArray.filter(_.state == WorkerState.ALIVE)
        .filter(canLaunchExecutor(_, app.desc))
        .sortBy(_.coresFree).reverse
      if (waitingApps.length == 1 && usableWorkers.isEmpty) {
        logWarning(s"App ${app.id} requires more resource than any of Workers could have.")
      }
      val assignedCores = scheduleExecutorsOnWorkers(app, usableWorkers, spreadOutApps)
      // Now that we've decided how many cores to allocate on each worker, let's allocate them
      for (pos <- 0 until usableWorkers.length if assignedCores(pos) > 0) {
        allocateWorkerResourceToExecutors(
          app, assignedCores(pos), app.desc.coresPerExecutor, usableWorkers(pos))
        }
      }
    }
}
```
其中 `app.desc.coresPerExecutor`　由 `spark.executor.cores`(表示每个executor需要的 cpu cores) 指定, 如果该值太大, 超过了已经注册的worker的free cores,　那么不会为该app分配executor, 而该app将会一直处于waitingApps中, 等着resource可用。

而`app.coresLeft`由下面的计算可得, 一般情况下该值会非常大
``` java
//　desc.maxCores由spark.cores.max指定
// defaultCores由　spark.deploy.defaultCores　指定, 默认为 Int.MAX_VALUE
private val requestedCores = desc.maxCores.getOrElse(defaultCores)
// coresGranted为已经获得的cpu cores
def coresLeft: Int = requestedCores - coresGranted
```

``` java
private def scheduleExecutorsOnWorkers(
    app: ApplicationInfo,
    usableWorkers: Array[WorkerInfo],
    spreadOutApps: Boolean): Array[Int] = {
  val coresPerExecutor = app.desc.coresPerExecutor
  //　如果没有定义spark.executor.cores, minCoresPerExecutor则默认为１, 表示executor最少使用的cpu cores
  val minCoresPerExecutor = coresPerExecutor.getOrElse(1) 
  //　如果没有定义spark.executor.oneExecutorPerWorker为true, 它的意思是每个worker只产生一个executor
  val oneExecutorPerWorker = coresPerExecutor.isEmpty
  val memoryPerExecutor = app.desc.memoryPerExecutorMB
  val resourceReqsPerExecutor = app.desc.resourceReqsPerExecutor
  val numUsable = usableWorkers.length
  // assignedCores与assingedExecutors是一个数组, 数组里每个元素表示该worker上分配的cpu cores数量, 与对应的executor个数
  val assignedCores = new Array[Int](numUsable) // Number of cores to give to each worker
  val assignedExecutors = new Array[Int](numUsable) // Number of new executors on each worker
  var coresToAssign = math.min(app.coresLeft, usableWorkers.map(_.coresFree).sum)
  /** Return whether the specified worker can launch an executor for this app. */
  def canLaunchExecutorForApp(pos: Int): Boolean = {
    // 如果要分配的cores已经不足一个executor所需要的cores时就停止
    val keepScheduling = coresToAssign >= minCoresPerExecutor
    // worker上总共可用的core(coresFree)-已经预定的(assignedCores(pos)), 如果少于executor所需要的cores时就停止
    val enoughCores = usableWorkers(pos).coresFree - assignedCores(pos) >= minCoresPerExecutor
    val assignedExecutorNum = assignedExecutors(pos)
    // If we allow multiple executors per worker, then we can always launch new executors.
    // Otherwise, if there is already an executor on this worker, just give it more cores.
    // assignedExecutorNum＝0表示至少要allocate一个executor, 而 !oneExecutorPerWorker表示允许worker开启多个executor
    // 参见上面的注释
    val launchingNewExecutor = !oneExecutorPerWorker || assignedExecutorNum == 0
    if (launchingNewExecutor) { //生成一个新的executor
      val assignedMemory = assignedExecutorNum * memoryPerExecutor //计算需要assign多少memeory
      //同cpu cores,
      val enoughMemory = usableWorkers(pos).memoryFree - assignedMemory >= memoryPerExecutor
      // 其它resource, 这里的resource主要是GPU和FPGA
      val assignedResources = resourceReqsPerExecutor.map {
        req => req.resourceName -> req.amount * assignedExecutorNum
      }.toMap
      val resourcesFree = usableWorkers(pos).resourcesAmountFree.map {
        case (rName, free) => rName -> (free - assignedResources.getOrElse(rName, 0))
      }
      val enoughResources = ResourceUtils.resourcesMeetRequirements(
        resourcesFree, resourceReqsPerExecutor)
      //　分得的executor数量不能多于app.executorLimit // 该值由 dynamic resource allocation 决定
      val underLimit = assignedExecutors.sum + app.executors.size < app.executorLimit
      keepScheduling && enoughCores && enoughMemory && enoughResources && underLimit
    } else {
      // We're adding cores to an existing executor, so no need
      // to check memory and executor limits
      keepScheduling && enoughCores
    }
  }
  // Keep launching executors until no more workers can accommodate any
  // more executors, or if we have reached this application's limits
  var freeWorkers = (0 until numUsable).filter(canLaunchExecutorForApp)
  while (freeWorkers.nonEmpty) {
    freeWorkers.foreach { pos =>
      var keepScheduling = true
      //　如果worker条件满足, 开始分配cpu cores或executor给application
      while (keepScheduling && canLaunchExecutorForApp(pos)) { 
        coresToAssign -= minCoresPerExecutor
        assignedCores(pos) += minCoresPerExecutor //分配的cpu core保存到 assignedCores里
        // If we are launching one executor per worker, then every iteration assigns 1 core
        // to the executor. Otherwise, every iteration assigns cores to a new executor.
        if (oneExecutorPerWorker) {
          assignedExecutors(pos) = 1
        } else {
          assignedExecutors(pos) += 1
        }
        // Spreading out an application means spreading out its executors across as
        // many workers as possible. If we are not spreading out, then we should keep
        // scheduling executors on this worker until we use all of its resources.
        // Otherwise, just move on to the next worker.
        if (spreadOutApps) {
          keepScheduling = false
        }
      }
    }
    freeWorkers = freeWorkers.filter(canLaunchExecutorForApp)
  }
  assignedCores
}
```

`spreadOutApps ＝ true`　表示横向遍历,即依次遍历每个worker来满足app.反之, 为纵向遍历, 先遍历第一个worker的资源, 如果还不满足app, 再遍历第二个worker, 依此类推.

`spreadOutApps`由`spark.deploy.spreadOut`控制, 默认为true,　即横向遍历.

在获得每个worker可分配的cpu cores后,　即`val assignedCores = scheduleExecutorsOnWorkers(app, usableWorkers, spreadOutApps)`

``` java
val assignedCores = scheduleExecutorsOnWorkers(app, usableWorkers, spreadOutApps)
// Now that we've decided how many cores to allocate on each worker, let's allocate them
for (pos <- 0 until usableWorkers.length if assignedCores(pos) > 0) {
  allocateWorkerResourceToExecutors(
    app, assignedCores(pos), app.desc.coresPerExecutor, usableWorkers(pos))
}
```
前面可知, assignedCores是一个数组, 它里面保存每个worker可分配的cpu core个数, 如果大于0, 表示可以在该worker上分配executor.

``` java
private def allocateWorkerResourceToExecutors(
    app: ApplicationInfo,
    assignedCores: Int,
    coresPerExecutor: Option[Int],
    worker: WorkerInfo): Unit = {
  // If the number of cores per executor is specified, we divide the cores assigned
  // to this worker evenly among the executors with no remainder.
  // Otherwise, we launch a single executor that grabs all the assignedCores on this worker.
  // 计算需要在该worker上开启多少个executor
  val numExecutors = coresPerExecutor.map { assignedCores / _ }.getOrElse(1)
  val coresToAssign = coresPerExecutor.getOrElse(assignedCores)
  for (i <- 1 to numExecutors) {
    val allocated = worker.acquireResources(app.desc.resourceReqsPerExecutor)
    val exec = app.addExecutor(worker, coresToAssign, allocated)
    launchExecutor(worker, exec)
    app.state = ApplicationState.RUNNING
  }
}
```

总结一下

假设有３个worker, 每个worker分别有10个cpu core.

Application最大的请求**CPU cores**即maxCores由`spark.cores.max`控制, 默认是None, 但是在Master调度的时候, 默认为Int.MAX_VALUE

而每个Executor的cpu core即 coresPerExecutor 个数由`spark.executor.cores`指定, 默认为１


- Application maxCores=Int.Max, coresPerExecutor=1 (显示设置 spark.executor.cores=1), `spreadOut=true`, memoryPerExecutorMB << total executor memory

| worker  	| cpu cores to assign  	| num executor 	| cores each executor 	|
|---------	|------------	|--------------	|---------------------	|
| worker1 	| 10          	| 10            	| 1                   	|
| worker2 	| 10          	| 10            	| 1                   	|
| worker3 	| 10          	| 10            	| 1                   	|

- Application maxCores=8, coresPerExecutor=None, `spreadOut=true`, memoryPerExecutorMB << total executor memory

| worker  	| cpu cores to assign  	| num executor 	| cores each executor 	|
|---------	|------------	|--------------	|---------------------	|
| worker1 	| 3          	| 1            	| 3                   	|
| worker2 	| 3          	| 1            	| 3                   	|
| worker3 	| 2          	| 1            	| 2                   	|

coresPerExecutor=None (没有显示设置), 则每个worker只分一个executor

- Application maxCores=8, coresPerExecutor=None, `spreadOut=false`, memoryPerExecutorMB << total executor memory

| worker  	| cpu cores to assign  	| num executor 	| cores each executor 	|
|---------	|------------	|--------------	|---------------------	|
| worker1 	| 8          	| 1            	| 8                   	|
| worker2 	| 0          	| 1            	| 0                   	|
| worker3 	| 0          	| 1            	| 0                   	|

spreadOut=false的情况, 先将一个worker分配完再去check另一个worker,

- Application maxCores=20, coresPerExecutor=2, `spreadOut=true`, memoryPerExecutorMB << total executor memory

| worker  	| cpu cores to assign 	| num executor 	| cores each executor 	|
|---------	|------------	|--------------	|---------------------	|
| worker1 	| 8          	| 4            	| 2                   	|
| worker2 	| 6          	| 3            	| 2                   	|
| worker3 	| 6          	| 3            	| 2                   	|

- Application maxCores=8, coresPerExecutor=None, `spreadOut=true`
  - memoryPerExecutor＝3072, Total memory executor=4096
  
| worker  	| cpu cores  to assign 	| num executor 	| cores each executor 	|
|---------	|------------	|--------------	|---------------------	|
| worker1 	| 3          	| 1            	| 3                   	|
| worker2 	| 3          	| 1            	| 3                   	|
| worker3 	| 2          	| 1            	| 2                   	|

在这种情况下, 只有一个exeuctor, 只要 memoryPerExecutor <= Total executor memeory即可.同理可以推得其它种case, 可以参考 Master　的测试类。