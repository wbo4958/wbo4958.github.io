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
