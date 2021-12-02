---
layout: page
title: Standalone Deploy 
nav_order: 5
parent: SparkCore 
grand_parent: Spark
---

# Spark Standalone Deploy
{: .no_toc}

> 本文基于 Spark 3.2.0 学习 Spark Standalone deploy 过程.

## 目录
{: .no_toc .text-delta}

1. TOC
{:toc}

## OverView

下面这张图是从[spark官网](https://spark.apache.org/docs/latest/cluster-overview.html)获得

![cluster overview](/docs/spark/spark-core/deploy/cluster-overview.png)

Spark applications run as independent sets of processes on a cluster, coordinated by the SparkContext object in your main program (called the driver program).

Specifically, to run on a cluster, the SparkContext can connect to several types of cluster managers (either Spark’s own standalone cluster manager, Mesos or YARN), which allocate resources across applications. Once connected, Spark acquires executors on nodes in the cluster, which are processes that run computations and store data for your application. Next, it sends your application code (defined by JAR or Python files passed to SparkContext) to the executors. Finally, SparkContext sends tasks to the executors to run.

下面是Bing的翻译

在main程序(被称为driver程序)的SparkContext对象的协调下，Spark Application作为独立的进程集在集群中运行

具体来说，为了运行在集群环境中， SparkContext可以连接到多种集群Manager连接(如 Spark自带的standalone cluster maanger, Mesos or YARN)), 这些集群Manager跨进程为Spark Application分配资源. 一旦连接成功后， Spark获得了集群节点上的executor,　这些executor是为你的Application运行和保存数据的进程. 然后SparkContext将你的应用程序代码(传递给SparkContext的JAR文件或Python文件)发送给executor. 最后SparkContext将Task发送给exeuctor去运行

Spark 的 Deploy 过程如图所示

![](/docs/spark/spark-core/deploy/standalone-deploy-standalone-deploy.drawio.svg)

## Deploy涉及到的的组件

- Master

也就是Spark自带的Standalone Manager，Master是spark的主节点，它负责管理Worker,并为注册App的在worker上分配executor等。

- Worker

管理Worker所在Node上被分配的资源，负责启动Executor

- Executor

负责执行Spark Application的Task

- Driver

往Master中注册Application, 根据注册给Driver端的Executor信息，调度Task, 并将结果传给用户.

**step 1:**

- Master状态从STANDBY到ALIVE, 后面所有的操作都是基本ALIVE的，否则会直接pass掉

**step 2 - step 4**

- Worker向Master发起注册信息，Master会给该Worker分配ＷorkerInfo用来表示该Worker的一些资源与属性，并将该WorkerInfo保存到Master的本地变量中
- Worker收到Master的注册成功信息后，会开启心跳线程定时向worker发送HeartBeat,保持连接. 接着向Master报告WorkerLasteState，主要是excutors和drivers

**step 5 - step 7**

- App向Master注册Application,　Master为该Application生成ApplicationInfo，并保存到Master的变量中，请参考第一节

**step 8 - step 12**

- Master在注册的worker中遍历找到可用的worker(根据worker的状态以及Application需要的资源,worker是否满足)
- 然后根据Application的需求，分配合适的Executor给Application,并通知worker开启executor进程, 用来执行Application的Command
- Worker收到Master的LaunchExecutor信息后，生成一个ExecutorRunner线程来启动新executor进程

**step 13 - step 16**

- Worker创建Executor进程并运行 CoarseGrainedExecutorBackend 的main函数.
- CoarseGrainedExecutorBackend会先从Driver中获得App的配置信息，然后向Driver注册Executor,即发送RegisterExecutor消息
- CoarseGrainedExecutorBackend生成Executor类来处理App端提交过来的task

**step 17 - step 18**

- Driver 在收到 Executor 向 Driver 发出 LaunchedExecutor 信息后，开始 makeOffers, 准备向 Executor schedule tasks.
- Executor 收到　Task 并执行

## Master如何分配Executor的

这里的schedule主要是master为waitingApps调度可用的resources，代码流程如下所示(`主要是step5-step8`)

``` scala
schedule ->
startExecutorsOnWorkers ->

private def startExecutorsOnWorkers(): Unit = {
  // Right now this is a very simple FIFO scheduler. We keep trying to fit in the first app
  // in the queue, then the second app, etc.
  for (app <- waitingApps) { // waitingApps表示step5向Master注册的Application
    //coresPerExecutor是Application指定处理该Application时，executor所使用的cpu cores个数, 如果没有指定，则默认为１个
    val coresPerExecutor = app.desc.coresPerExecutor.getOrElse(1) 
    // If the cores left is less than the coresPerExecutor,the cores left will not be allocated
    //　一般情况下app.coresLeft会非常大，所以会进入if分支
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

其中 `app.desc.coresPerExecutor`　由 `spark.executor.cores`(表示每个executor需要的 cpu cores) 指定, 如果该值太大，超过了已经注册的worker的free cores,　那么不会为该app分配executor, 而该app将会一直处于waitingApps中，等着resource可用。

而`app.coresLeft`由下面的计算可得，一般情况下该值会非常大

``` scala
//　desc.maxCores由spark.cores.max指定
// defaultCores由　spark.deploy.defaultCores　指定，默认为 Int.MAX_VALUE
private val requestedCores = desc.maxCores.getOrElse(defaultCores)
// coresGranted为已经获得的cpu cores
def coresLeft: Int = requestedCores - coresGranted
```

``` scala
private def scheduleExecutorsOnWorkers(
    app: ApplicationInfo,
    usableWorkers: Array[WorkerInfo],
    spreadOutApps: Boolean): Array[Int] = {
  val coresPerExecutor = app.desc.coresPerExecutor
  //　如果没有定义spark.executor.cores，minCoresPerExecutor则默认为１, 表示executor最少使用的cpu cores
  val minCoresPerExecutor = coresPerExecutor.getOrElse(1) 
  //　如果没有定义spark.executor.oneExecutorPerWorker为true, 它的意思是每个worker只产生一个executor
  val oneExecutorPerWorker = coresPerExecutor.isEmpty
  val memoryPerExecutor = app.desc.memoryPerExecutorMB
  val resourceReqsPerExecutor = app.desc.resourceReqsPerExecutor
  val numUsable = usableWorkers.length
  // assignedCores与assingedExecutors是一个数组，数组里每个元素表示该worker上分配的cpu cores数量，与对应的executor个数
  val assignedCores = new Array[Int](numUsable) // Number of cores to give to each worker
  val assignedExecutors = new Array[Int](numUsable) // Number of new executors on each worker
  var coresToAssign = math.min(app.coresLeft, usableWorkers.map(_.coresFree).sum)
  /** Return whether the specified worker can launch an executor for this app. */
  def canLaunchExecutorForApp(pos: Int): Boolean = {
    // 如果要分配的cores已经不足一个executor所需要的cores时就停止
    val keepScheduling = coresToAssign >= minCoresPerExecutor
    // worker上总共可用的core(coresFree)-已经预定的(assignedCores(pos))，如果少于executor所需要的cores时就停止
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
      // 其它resource，这里的resource主要是GPU和FPGA
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
      //　如果worker条件满足，开始分配cpu cores或executor给application
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

`spreadOutApps ＝ true`　表示横向遍历,即依次遍历每个worker来满足app. 反之, 为纵向遍历，先遍历第一个worker的资源，如果还不满足app, 再遍历第二个worker，依此类推.

`spreadOutApps`由`spark.deploy.spreadOut`控制，默认为true,　即横向遍历.

在获得每个worker可分配的cpu cores后,　即`val assignedCores = scheduleExecutorsOnWorkers(app, usableWorkers, spreadOutApps)`

``` scala
val assignedCores = scheduleExecutorsOnWorkers(app, usableWorkers, spreadOutApps)
// Now that we've decided how many cores to allocate on each worker, let's allocate them
for (pos <- 0 until usableWorkers.length if assignedCores(pos) > 0) {
  allocateWorkerResourceToExecutors(
    app, assignedCores(pos), app.desc.coresPerExecutor, usableWorkers(pos))
}
```

前面可知, assignedCores是一个数组，它里面保存每个worker可分配的cpu core个数, 如果大于0，表示可以在该worker上分配executor.

``` scala
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

Application 最大的请求**CPU cores**即 maxCores 由`spark.cores.max` 控制，默认是None, 但是在Master调度的时候，默认为Int.MAX_VALUE

而每个 Executor 的 cpu core 即 coresPerExecutor 个数由`spark.executor.cores`指定，默认为１

- Application maxCores=Int.Max, coresPerExecutor=1 (显示设置 spark.executor.cores=1), `spreadOut=true`, memoryPerExecutorMB << total executor memory

| worker   | cpu cores to assign   | num executor  | cores each executor  |
|--------- |------------ |-------------- |--------------------- |
| worker1  | 10           | 10             | 1                    |
| worker2  | 10           | 10             | 1                    |
| worker3  | 10           | 10             | 1                    |

- Application maxCores=8, coresPerExecutor=None, `spreadOut=true`, memoryPerExecutorMB << total executor memory

| worker   | cpu cores to assign   | num executor  | cores each executor  |
|--------- |------------ |-------------- |--------------------- |
| worker1  | 3           | 1             | 3                    |
| worker2  | 3           | 1             | 3                    |
| worker3  | 2           | 1             | 2                    |

coresPerExecutor=None (没有显示设置), 则每个worker只分一个executor

- Application maxCores=8, coresPerExecutor=None, `spreadOut=false`, memoryPerExecutorMB << total executor memory

| worker   | cpu cores to assign   | num executor  | cores each executor  |
|--------- |------------ |-------------- |--------------------- |
| worker1  | 8           | 1             | 8                    |
| worker2  | 0           | 1             | 0                    |
| worker3  | 0           | 1             | 0                    |

spreadOut=false的情况，先将一个worker分配完再去check另一个worker,

- Application maxCores=20, coresPerExecutor=2, `spreadOut=true`, memoryPerExecutorMB << total executor memory

| worker   | cpu cores to assign  | num executor  | cores each executor  |
|--------- |------------ |-------------- |--------------------- |
| worker1  | 8           | 4             | 2                    |
| worker2  | 6           | 3             | 2                    |
| worker3  | 6           | 3             | 2                    |

- Application maxCores=8, coresPerExecutor=None, `spreadOut=true`
  - memoryPerExecutor＝3072, Total memory executor=4096
  
| worker   | cpu cores  to assign  | num executor  | cores each executor  |
|--------- |------------ |-------------- |--------------------- |
| worker1  | 3           | 1             | 3                    |
| worker2  | 3           | 1             | 3                    |
| worker3  | 2           | 1             | 2                    |

在这种情况下，只有一个exeuctor, 只要 `memoryPerExecutor <= Total executor memeory` 即可.
同理可以推得其它种case, 可以参考 Master　的测试类.
