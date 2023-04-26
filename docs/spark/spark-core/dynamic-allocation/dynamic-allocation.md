---
layout: page
title: Spark Dynamic Allocation
nav_order: 6
parent: SparkCore 
grand_parent: Spark
---

# Spark Dynamic Allocation
{: .no_toc}

> 本文基于 Spark 3.4.0 学习 Spark Dynamic Allocation 原理.

## 目录
{: .no_toc .text-delta}

1. TOC
{:toc}

动态资源分配 (dynamic resource allocation) 是相对于静态分配. 那什么是静态分配呢? 当用户通过配置文件比如

- 对于 yarn
  - `--num-executor` 就决定了 Yarn 分配多少个 executor 给该 Spark Application
- 对于 Standalone, 通过 `--spark.executor.cores` 也同样决定了 这个 Application 分配到的 executor 数量.

那静态分配有什么缺点呢? 静态分配对于不同的 stage, 都是同样的 executor. 如果有的 stage task 数量很少，那 executor 就会资源浪费，而如果 stage task 数量很多，那已经分配好的 executor 资源又不太够用. 另外静态分配会导致 cluster 利用率降低 (同时跑 application 的个数也会被限制)

那什么是动态分配呢？ 不同 Stage 可以动态调整 executor 资源,　以及该 Stage 的 task 资源, 可增可减.

那动态分配有什么缺点呢？ 因为是动态分配与删减，这样会导致 kill executor 进程 以及 launch Executor overhead. 另外，静态分配是一次性将 executor 分配好，executor 的启动都可以是并行的。而如果变成动态分配，那 executor 的启动可以看成是串行的，可能会有 performance issue.

## Dynamic allocation 配置

从官网上来看到目前任何 coarsed-grained cluster， 比如 Standalone, yarn 是支持动态资源分配的 [dynamic-resource-allocation](http://spark.apache.org/docs/latest/job-scheduling.html#dynamic-resource-allocation)

另外 [dynamic-configuration](http://spark.apache.org/docs/latest/configuration.html#dynamic-allocation) 列出了 dynamic resource allocation 相关的配置项.

下面以 standalone 来分析 动态分配的是怎么实现的. 要在 Standalone 上 enable 动态资源分配，需要配置如下.

``` xml
--conf spark.dynamicAllocation.enabled=true \
--conf spark.shuffle.service.enabled=true\
```

这组配置需要在每个 worker 上配置 external shuffle service, 这样 executor被删除后就不会删除掉相应的 shuffle 数据文件, 请参考 [why](http://spark.apache.org/docs/latest/job-scheduling.html#graceful-decommission-of-executors)

或

``` xml
--conf spark.dynamicAllocation.enabled=true \
--conf spark.dynamicAllocation.shuffleTracking.enabled=true\
```

该配置不需要配置 external shuffle service, 因此本文以该配置进行解释.

## Ｄisable 静态资源分配

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

## ExecutorAllocationManager

如果 dynamic allocation enable 后，在 SparkContext 里会生成一个 ExecutorAllocationManager, 该类用来实现 executor 的分配与移除.

- 初始化

``` scala
// SparkContext.scala, 如果 dynamicAllocation enabled, 则会初始化 ExecutorAllocationManager

// local 模式不支持dynamic allocation, local-cluster/standalone/yarn/... 支持 dynamic allocation
val dynamicAllocationEnabled = Utils.isDynamicAllocationEnabled(_conf)
_executorAllocationManager =
  if (dynamicAllocationEnabled) {
    schedulerBackend match {
      case b: ExecutorAllocationClient =>
        Some(new ExecutorAllocationManager(
          schedulerBackend.asInstanceOf[ExecutorAllocationClient], listenerBus, _conf,
          cleaner = cleaner, resourceProfileManager = resourceProfileManager))
      case _ =>
        None
    }
  } else {
    None
  }
_executorAllocationManager.foreach(_.start())
```

ExecutorAllocationManager 中一些常用的变量及其作用,

|Variables  | 作用 |
|----|----|
| initialNumExecutors | 初始化的 executor 个数, 每个 rp 想要的 executor个数, =max(`spark.dynamicAllocation.minExecutors`, `spark.dynamicAllocation.initialExecutors`,`spark.executor.instances`)|
|HashMap[Int, Int] numExecutorsToAddPerResourceProfileId| rp_id -> num_executor, 表示下一轮需要为 rp 增加的 executor 的数量, 对于 default rp, 默认值为 1|
|HashMap[Int, Int] numExecutorsTargetPerResourceProfileId| rp_id -> desired_num_executor, 表示 rp 想要的 executor 个数, 如果某个时刻,rp对应的所有的executor都挂掉了,那需要立即从cluster中获得 desired_num_executor 个 executor. 对于 default rp, 默认值为 initialNumExecutors|
|HashMap[Int, Int] numLocalityAwareTasksPerResourceProfileId| rp_id -> Number of locality aware tasks, rp与locality task之间的映射|

``` scala
// ExecutorAllocationManager.scala

def start(): Unit = {
  val scheduleTask = new Runnable() {
    override def run(): Unit = Utils.tryLog(schedule())
  }

  //每隔100ms 调度一次 schduleTask
  executor.scheduleWithFixedDelay(scheduleTask, 0, intervalMillis, TimeUnit.MILLISECONDS)
  //获得需要初始化的 executor 数量
  // copy the maps inside synchonize to ensure not being modified
  val (numExecutorsTarget, numLocalityAware) = synchronized {
    val numTarget = numExecutorsTargetPerResourceProfileId.toMap
    val numLocality = numLocalityAwareTasksPerResourceProfileId.toMap
    (numTarget, numLocality)
  }
  // 向 Master 请求每个 rp 对应的 executor 数量
  client.requestTotalExecutors(numExecutorsTarget, numLocalityAware, rpIdToHostToLocalTaskCount)
}
```

**numExecutorsTargetPerResourceProfileId** 表示需要请求的 executor 个数，初始化的 executor 个数由下面代码决定

max (`spark.dynamicAllocation.minExecutors`, `spark.dynamicAllocation.initialExecutors`, `spark.executor.instances`)

requestTotalExecutors 向 cluster manager 也就是 Master 请求我们需要的 executor 个数.

``` scala
// CoarseGrainedSchedulerBackend.scala
final override def requestTotalExecutors(
    resourceProfileIdToNumExecutors: Map[Int, Int],
    numLocalityAwareTasksPerResourceProfileId: Map[Int, Int],
    hostToLocalTaskCount: Map[Int, Map[String, Int]]
): Boolean = {
  val totalExecs = resourceProfileIdToNumExecutors.values.sum
  ...
  // resource profile id获得 resource profile 以及它们所需要的总共 executor 个数
  val resourceProfileToNumExecutors = resourceProfileIdToNumExecutors.map { case (rpid, num) =>
    (scheduler.sc.resourceProfileManager.resourceProfileFromId(rpid), num)
  }
  val response = synchronized {
    this.requestedTotalExecutorsPerResourceProfile.clear()
    this.requestedTotalExecutorsPerResourceProfile ++= resourceProfileToNumExecutors
    this.numLocalityAwareTasksPerResourceProfileId = numLocalityAwareTasksPerResourceProfileId
    this.rpHostToLocalTaskCount = hostToLocalTaskCount
    // ...
    doRequestTotalExecutors(requestedTotalExecutorsPerResourceProfile.toMap)
  }
  defaultAskTimeout.awaitResult(response)
}
```

``` scala
protected override def doRequestTotalExecutors(
    resourceProfileToTotalExecs: Map[ResourceProfile, Int]): Future[Boolean] = {
  // resources profiles not supported
  Option(client) match {
    case Some(c) =>
      c.requestTotalExecutors(resourceProfileToTotalExecs) //最终发送请求到 Master 请求分配 executor
    case None =>
      ...
  }
}
```

## schedule

ExecutorAllocationManager 启动一个定时任务去检查当前是否需要 allocate 或 remove 掉 executor

``` scala
private def schedule(): Unit = synchronized {
  // 检查是否需要移出掉executor
  val executorIdsToBeRemoved = executorMonitor.timedOutExecutors()
  if (executorIdsToBeRemoved.nonEmpty) {
    initializing = false
  }

  // Update executor target number only after initializing flag is unset
  updateAndSyncNumExecutorsTarget(clock.nanoTime())
  if (executorIdsToBeRemoved.nonEmpty) {
    removeExecutors(executorIdsToBeRemoved) //如果需要移出executor,则remove掉executor
  }
}
```

ExecutorMonitor 会监控 executor，包括是 `onExecutorAdded/onExecutorRemoved/onTaskStarted` ...
当一个 executor 中没有 task 执行, 也就是 idle 状态时，且 idle 超过一定的时间(对于非 shuffle或storage时,
默认是 60s, 由 `spark.dynamicAllocation.executorIdleTimeout` 控制) 会被remove掉

``` scala

// ExecutorAllocationManager.scala

private def updateAndSyncNumExecutorsTarget(now: Long): Int = synchronized {
  if (initializing) { // 在 ExecutorAllocationListener 中 onStageSubmmited 会设置 initializing=false
    // Do not change our target while we are still initializing,
    // Otherwise the first job may have to ramp up unnecessarily
    0
  } else {
    val updatesNeeded = new mutable.HashMap[Int, ExecutorAllocationManager.TargetNumUpdates]
    // Update targets for all ResourceProfiles then do a single request to the cluster manager
    numExecutorsTargetPerResourceProfileId.foreach { case (rpId, targetExecs) =>
      val maxNeeded = maxNumExecutorsNeededPerResourceProfile(rpId)
      if (maxNeeded < targetExecs) {
        // The target number exceeds the number we actually need, so stop adding new
        // executors and inform the cluster manager to cancel the extra pending requests
        // We lower the target number of executors but don't actively kill any yet.  Killing is
        // controlled separately by an idle timeout.  It's still helpful to reduce
        // the target number in case an executor just happens to get lost (eg., bad hardware,
        // or the cluster manager preempts it) -- in that case, there is no point in trying
        // to immediately  get a new executor, since we wouldn't even use it yet.
        decrementExecutorsFromTarget(maxNeeded, rpId, updatesNeeded)
      } else if (addTime != NOT_SET && now >= addTime) {
        addExecutorsToTarget(maxNeeded, rpId, updatesNeeded)
      }
    }
    doUpdateRequest(updatesNeeded.toMap, now)
  }
}
```

其中 addExecutorsToTarget 调用 addExecutors 计算需要加入多少个 executor

```scala
private def addExecutors(maxNumExecutorsNeeded: Int, rpId: Int): Int = {
  //如果已经申请好的executor个数超过了 maxNumExecutors, 那没有必要再申请了
  val oldNumExecutorsTarget = numExecutorsTargetPerResourceProfileId(rpId)
  // Do not request more executors if it would put our target over the upper bound
  // this is doing a max check per ResourceProfile
  if (oldNumExecutorsTarget >= maxNumExecutors) {
    logDebug("Not adding executors because our current target total " +
      s"is already ${oldNumExecutorsTarget} (limit $maxNumExecutors)")
    numExecutorsToAddPerResourceProfileId(rpId) = 1
    return 0
  }
  // There's no point in wasting time ramping up to the number of executors we already have, so
  // make sure our target is at least as much as our current allocation:
  // 获得当前 rpId 对应的 executor 数量.  executorMonitor.executorCountWithResourceProfile
  // 记录了 rpId 对应的 executor 数量，该值是最新的
  var numExecutorsTarget = math.max(numExecutorsTargetPerResourceProfileId(rpId),
      executorMonitor.executorCountWithResourceProfile(rpId))
  // Boost our target with the number to add for this round:
  // numExecutorsToAddPerResourceProfileId 表示下一轮需要加入的 executor 数量
  // 获得下一轮需要的总共的 executor
  numExecutorsTarget += numExecutorsToAddPerResourceProfileId(rpId)
  // Ensure that our target doesn't exceed what we need at the present moment:
  // application所需要的executor总数不能大于 maxNumExecutorsNeeded
  numExecutorsTarget = math.min(numExecutorsTarget, maxNumExecutorsNeeded)
  // Ensure that our target fits within configured bounds:
  // 不能小于最少的executor数
  numExecutorsTarget = math.max(math.min(numExecutorsTarget, maxNumExecutors), minNumExecutors)
  // 获得这次需要申请的 executor数，即差额 
  val delta = numExecutorsTarget - oldNumExecutorsTarget
  numExecutorsTargetPerResourceProfileId(rpId) = numExecutorsTarget
  // If our target has not changed, do not send a message
  // to the cluster manager and reset our exponential growth
  if (delta == 0) { //如果当前计数没有差额，即不需要申请executor,则重置numExecutorsToAddPerResourceProfileId为1, 重新正行指数增加
    numExecutorsToAddPerResourceProfileId(rpId) = 1
  }
  delta
}
```

``` scala
private def doUpdateRequest(
    // updates即表示需要申请的 executor 的个数
    updates: Map[Int, ExecutorAllocationManager.TargetNumUpdates],
    now: Long): Int = {
  // Only call cluster manager if target has changed.
  if (updates.size > 0) {
    val requestAcknowledged = try {
      logDebug("requesting updates: " + updates)
      testing ||
        client.requestTotalExecutors( //开始申请executor个数
          numExecutorsTargetPerResourceProfileId.toMap,
          numLocalityAwareTasksPerResourceProfileId.toMap,
          rpIdToHostToLocalTaskCount)
    } catch {
      case NonFatal(e) =>
        // Use INFO level so the error it doesn't show up by default in shells.
        // Errors here are more commonly caused by YARN AM restarts, which is a recoverable
        // issue, and generate a lot of noisy output.
        logInfo("Error reaching cluster manager.", e)
        false
    }
    if (requestAcknowledged) {
      // have to go through all resource profiles that changed
      var totalDelta = 0
      //updates表示rpId需要申请的executor数量,由TargetNumUpdates表示
      updates.foreach { case (rpId, targetNum) =>
        val delta = targetNum.delta //差额
        totalDelta += delta
        if (delta > 0) {
          val executorsString = "executor" + { if (delta > 1) "s" else "" }
          logInfo(s"Requesting $delta new $executorsString because tasks are backlogged " +
            s"(new desired total will be ${numExecutorsTargetPerResourceProfileId(rpId)} " +
            s"for resource profile id: ${rpId})")
          numExecutorsToAddPerResourceProfileId(rpId) =
           // 正常情况下 delta是等于numExecutorsToAddPerResourceProfileId, numExecutorsToAddPerResourceProfileId是下一次需要加入的executor
            if (delta == numExecutorsToAddPerResourceProfileId(rpId)) {
              // 指数增加
              numExecutorsToAddPerResourceProfileId(rpId) * 2
            } else {
              1
            }
          logDebug(s"Starting timer to add more executors (to " +
            s"expire in $sustainedSchedulerBacklogTimeoutS seconds)")
          addTime = now + TimeUnit.SECONDS.toNanos(sustainedSchedulerBacklogTimeoutS)
        } else {
          logDebug(s"Lowering target number of executors to" +
            s" ${numExecutorsTargetPerResourceProfileId(rpId)} (previously " +
            s"${targetNum.oldNumExecutorsTarget} for resource profile id: ${rpId}) " +
            "because not all requested executors " +
            "are actually needed")
        }
      }
      totalDelta
    } else {
      // request was for all profiles so we have to go through all to reset to old num
      updates.foreach { case (rpId, targetNum) =>
        logWarning("Unable to reach the cluster manager to request more executors!")
        numExecutorsTargetPerResourceProfileId(rpId) = targetNum.oldNumExecutorsTarget
      }
      0
    }
  } else {
    logDebug("No change in number of executors")
    0
  }
}
```

## 来看下整个流程

> initialNumExecutors = 0

1. ExecutorAllocationManager.start

  ``` console
  numExecutorsTargetPerResourceProfileId(defaultRpId)=0
  onStageSubmitted -> 
      numExecutorsToAddPerResourceProfileId(defaultRpId)=1
  ```

2. 第一次schedule
  
> **此时 numExecutorsToAddPerResourceProfileId=1 numExecutorsTargetPerResourceProfileId=0**

``` console
updateAndSyncNumExecutorsTarget ->
  addExecutorsToTarget
    numExecutorsTargetPerResourceProfileId=numExecutorsTargetPerResourceProfileId+numExecutorsToAddPerResourceProfileId
    numExecutorsTargetPerResourceProfileId = 1

推测出 => updates = TargetNumUpdates(delta=1, oldNumExecutorsTarget=0)
doUpdateRequest
  => 向 cluster 请求 numExecutorsTargetPerResourceProfileId＝１ 个executor
  => 因为 updates中 delta == numExecutorsToAddPerResourceProfileId = 1, 所以
   更新  numExecutorsToAddPerResourceProfileId = numExecutorsToAddPerResourceProfileId*2=2
```

3. 第二次schedule

> **此时 numExecutorsToAddPerResourceProfileId=2 numExecutorsTargetPerResourceProfileId=1**

``` console
updateAndSyncNumExecutorsTarget ->
  addExecutorsToTarget
    numExecutorsTargetPerResourceProfileId=numExecutorsTargetPerResourceProfileId+numExecutorsToAddPerResourceProfileId
    numExecutorsTargetPerResourceProfileId = 3

推测出 => updates = TargetNumUpdates(delta=2, oldNumExecutorsTarget=1)
doUpdateRequest
  => 向 cluster 请求 numExecutorsTargetPerResourceProfileId＝3 个executor
  => 因为 updates中 delta == numExecutorsToAddPerResourceProfileId = 2, 所以
   更新  numExecutorsToAddPerResourceProfileId = numExecutorsToAddPerResourceProfileId*2=4
```

4. 第三次 schedule

> **此时 numExecutorsToAddPerResourceProfileId=4 numExecutorsTargetPerResourceProfileId=3**

``` console
updateAndSyncNumExecutorsTarget ->
  addExecutorsToTarget
    numExecutorsTargetPerResourceProfileId=numExecutorsTargetPerResourceProfileId+numExecutorsToAddPerResourceProfileId
    numExecutorsTargetPerResourceProfileId = 7

推测出 => updates = TargetNumUpdates(delta=4, oldNumExecutorsTarget=3)
doUpdateRequest
  => 向 cluster 请求 numExecutorsTargetPerResourceProfileId＝3 个executor
  => 因为 updates中 delta == numExecutorsToAddPerResourceProfileId = 4, 所以
   更新  numExecutorsToAddPerResourceProfileId = numExecutorsToAddPerResourceProfileId*2=8
```

依此类推 ...

> 注意, 每次往Master去申请executor时，实际上是将传递了 executor 总数给 Master, 并不是 delta.
这是因为 Master 在计算是否可再分配 executor 时是按照

``` scala
val underLimit = assignedExecutors.sum + app.executors.size < app.executorLimit
keepScheduling && enoughCores && enoughMemory && enoughResources && underLimit
```

其中 app.executors 是该app已经分配到的 executor, 而 executorLimit 也就是该 app 总共最多申请的executor, 由 dynamic resource manager 控制。相当于还是只申请 delta的executor.

## 总结

1. 初始化只向 master 申请 initialized number executor, 可由参数控制
2. 仅接着按 指数 申请分配executor
3. 申请的executor 不能大于 maximized executor数量，可由参数控制
4. 当一个 executor idle 超时后，会kill掉executor，回收的executor可分配给其它 job
