---
layout: page
title: Spark Resources
nav_order: 14
parent: core 
grand_parent: spark
---

# Spark Resource
{: .no_toc}

Spark 在 2.x 版本支持 cpu cores 和 memory 等配置用于"资源"的目的, 但并没有把它们真正归结于资源. 从 Spark 3.1 开始, 
将 cpu/memory 归结于资源, 同时还包括增加的 GPU/FPGA 等资源.

## 目录
{: .no_toc .text-delta}

1. TOC
{:toc}

## OverView

![resource profile](/docs/spark/core/resources/stage-level-scheduling-ResourceProfile.drawio.svg)

每个 ResourceProfile 包含一个固定的 id, 以及 ExecutorResourceRequest 和 TaskResourceRequests.

ExecutorResourceRequest 仅代表 Executor 所有资源中的一个资源, 如 cores. 同理 TaskResourceRequest 也仅仅是 task 所有资源中的一个资源, 如 cpus.

Spark 支持 stage level scheduling, 对于不同的 stage, 所需求的 Resource 不同, 那 ResourceProfile 也不同. 本文主要是了解 ResourceProfile 并不会涉及到 stage-level scheduling, 所以默认 整个 spark application 中只有一个默认的 ResourceProfile.


## Cluster Resources

![cluster resources](/docs/spark/core/resources/stage-level-scheduling-resource-overview.drawio.svg)

上图是整个 spark cluster 包括 driver 的 Resource 流程图.


1. Step 1: Worker 配置 Resources

Spark 在启动 worker 时是可以指定 Worker 的 cores/memory 以及 ResourceProfile file. 
也可以通过环境变量比如 `SPARK_WORKER_CORES`, `SPARK_WORKER_MEMORY` 分别指定 Worker 的 cores/memory,
如果没有指定, worker 也可以自己推断出来.

其中可以通过下面三种方式来获得 worker 的资源.

- `spark.worker.resourcesFile` 指定分配给 worker 的 resource 信息, 保存在一个 JSON 文件中.

``` xml
--conf spark.worker.resource.gpu.amount=2  # worker申请2个GPU, 可以不配置
--conf spark.worker.resource.fpga.amount=3  #worker 申请3个fpga, 可以不配置
--conf spark.worker.resourcesFile=/tmp/gpu_fpga_conf.json #分配给该worker的gpu资源
```

gpu_fpga_conf.json 文件如下所示

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

上述的配置文件表明worker可用 `2个gpu(设备号为0, 1), ３个fpga（设备号为 f1 f2 f3)`

- discoverScript 脚本

discoverScript 脚本方式自动发现可用资源, 在 ${SPARK_HOME}/conf/spark-defaults.conf

``` console
spark.worker.resource.gpu.amount 1
spark.worker.resource.gpu.discoveryScript ${SPARK_HOME/examples/src/main/scripts/getGpusResources.sh
```

getGpusResources.sh 如下

``` bash
# Example output: {"name": "gpu", "addresses":["0","1","2","3","4","5","6","7"]}
ADDRS=`nvidia-smi --query-gpu=index --format=csv,noheader | sed -e ':a' -e 'N' -e'$!ba' -e 's/\n/","/g'`
echo {\"name\": \"gpu\", \"addresses\":[\"$ADDRS\"]}
```

*discoveryScript* 方式是通过 **ResourceDiscoveryScriptPlugin**　开启一个进程去执行 *discoverScript* 脚本

- Plugin 方式去发现资源

用户也可以自定义 `spark.resources.discoveryPlugin` 自定义发现资源的代码.

> 那 Worker 的resource 到底是什么.

Worker 的 `resources: Map[String, ResourceInformation]`　表示整个 worker resource.

以 gpu_fpga_conf.json 为例, `resources` 的值如下所示

``` console
gpu -> ResourceInformation(name=gpu, addresses=Seq(0,1))
fpga -> ResourceInformation(name=fpga, addresses=Seq(f1,f2,f3)
```

### Application端配置

``` console
--conf spark.executor.resource.gpu.amount=1
--conf spark.executor.resource.fpga.amount=2
```

上面配置表示请求给 exeuctor 分配1个gpu, 2个fpga

``` console
--conf spark.task.resource.gpu.amount=1
--conf spark.task.resource.fpga.amount=1
```

上面的config表明需要为每个task分配1个gpu, 1个fpga, 这两个配置与 `spark.task.cores` 一起可以控制Task 并发的数量.

``` console
spark.executor.resource.gpu.amount=1
spark.task.resource.gpu.amount=0.5
spark.executor.cores=12
spark.task.cores=1
```

并发的 task = `min(12/1, 1/0.5) = 2`

在StandaloneSchedulerBackend中, 在创建 ApplicationDesc 时，会将 `spark.executor.resource.XXXX` 相关配置解析为executorResourceReqs,
也就是需要向worker申请executor的gpu/fpga资源

此时`executorResourceReqs`结果为

``` java
ResourceRequirement("fpga", 2)
ResourceRequirement("gpu", 1)
```

``` scala
val executorResourceReqs = ResourceUtils.parseResourceRequirements(conf,
  config.SPARK_EXECUTOR_PREFIX) 
//此时会将 spark.executor.resources.XXXX 配置解析成需要申请的executor的资源
val appDesc = ApplicationDescription(sc.appName, maxCores, sc.executorMemory, command,
  webUrl, sc.eventLogDir, sc.eventLogCodec, coresPerExecutor, initialExecutorLimit,
  resourceReqsPerExecutor = executorResourceReqs)
```

### Master端在worker上分配资源

Master 收到 Spark Application 注册信息后, 然后 Master 在已经注册的 Worker 中根据 Resource 等相关信息查找否能在该 Worker 上 launch Executor.
具体参照 `startExecutorsOnWorkers`, 分配的资源会放到 `ExecutorDesc` 中, 接着Master通知Worker启动相关的Executor进程.

## Worker启动Executor

``` scala
private def fetchAndRunExecutor() {
   try {
     // 生成 resourceFile 文件
     val resourceFileOpt = prepareResourcesFile(SPARK_EXECUTOR_PREFIX, resources, executorDir)
     // Launch the executor process, 通过 --resourcesFile 指定 executor 的 resource 信息.
     val arguments = appDesc.command.arguments ++ resourceFileOpt.map(f =>
       Seq("--resourcesFile", f.getAbsolutePath)).getOrElse(Seq.empty)
     val subsOpts = appDesc.command.javaOpts.map {
       Utils.substituteAppNExecIds(_, appId, execId.toString)
     }
     ...
   }
}
```

worker端将分配给executor的`resources` 资源保存**到worker节点的文件上**，当创建executor时，将该文件作为 `--resourcesFile executor.json` 传入executor当中

`executor.json`如下所示, 可以看出资源名称已经变成 `spark.executor` 开头的了

```json
[
  {
    "id": {
      "componentName": "spark.executor",
      "resourceName": "gpu"
    },
    "addresses": [
      "1"
    ]
  },
  {
    "id": {
      "componentName": "spark.executor",
      "resourceName": "fpga"
    },
    "addresses": [
      "f1",
      "f3"
    ]
  }
]
```

## Executor端如何使用该resource

在**CoarseGrainedExecutorBackend**中

```scala
override def onStart() {
  logInfo("Connecting to driver: " + driverUrl)
  _resources = parseOrFindResources(resourcesFileOpt) //解析出资源文件

  // 向 driver 注册 Executor 信息,包括 executor total cores 与 resources
  driver = Some(ref)
  ref.ask[Boolean](RegisterExecutor(executorId, self, hostname, cores, extractLogUrls,
      extractAttributes, resources))
  ...
}
```

``` scala
// visible for testing
def parseOrFindResources(resourcesFileOpt: Option[String]): Map[String,ResourceInformation] = {
  // Executor端要解析出它所使用的资源，前提是要task申请才会解析
    // 解析 resourcesFileOpt 分配给 executor 的资源,
    // 解析 spark.executor.resources.XXX 开头的资源 request
    // Check request 与 allocation 是否 match
      val resources = getOrDiscoverAllResourcesForResourceProfile(
        resourcesFileOpt,
        SPARK_EXECUTOR_PREFIX,
        resourceProfile,
        env.conf)
      resources
}
```

resources = `(gpu,[name: gpu, addresses: 0])`, 将 gpu id=0 分配给该 Executor.

> 注意, `spark.executor.resource.gpu.discoveryScript` 在 spark standalone 上是没有用到的, 只在 YARN/K8S 上有用

## Driver端接受Executor注册信息

当DriverEndpoint收到RegisterExecutor消息后，生成ExecutorData，并放到executorDataMap中. 其中executor进程的资源信息也同时保存在了ExecutorData中了。

``` scala
val data = new ExecutorData(executorRef, executorAddress, hostname,
      cores, cores, logUrlHandler.applyPattern(logUrls, attributes), attributes,
      resourcesInfo)

CoarseGrainedSchedulerBackend.this.synchronized {
  executorDataMap.put(executorId, data)
  ...
}
```

通过上面的几个模块的部署，Executor上可用资源信息已经保存到Driver上了，此时Driver可以通过TaskSchedulerImpl去调度Task了

## Driver调度Task

如图中`step 12`开始就是Driver来调度Task, 其中 `resourceOffers` 会根据注册给Driver的可用Executor信息来找出合适的Executor运行Task.

resourceOffers 会调用 resourceOfferSingleTaskSet

``` scala
//只有同时满足可用cpu core与 resource满足task请求的情况下，则该executor才算available
val taskResAssignmentsOpt = resourcesMeetTaskRequirements(taskSet, availableCpus(i),
          availableResources(i))
 
private def resourcesMeetTaskRequirements(
    taskSet: TaskSetManager,
    availCpus: Int,
    availWorkerResources: Map[String, Buffer[String]]
    ): Option[Map[String, ResourceInformation]] = {
  val rpId = taskSet.taskSet.resourceProfileId
  val taskSetProf = sc.resourceProfileManager.resourceProfileFromId(rpId)
  val taskCpus = ResourceProfile.getTaskCpusOrDefaultForProfile(taskSetProf, conf)
  // check if the ResourceProfile has cpus first since that is common case
  if (availCpus < taskCpus) return None //是否有足够可用 cpu
  // only look at the resource other then cpus
  // 获得 task resources request,  也就是   spark.tasks.resource.XXX
  val tsResources = ResourceProfile.getCustomTaskResources(taskSetProf)
  if (tsResources.isEmpty) return Some(Map.empty)
  val localTaskReqAssign = HashMap[String, ResourceInformation]()
  // we go through all resources here so that we can make sure they match and also get what the
  // assignments are for the next task
  for ((rName, taskReqs) <- tsResources) {
    val taskAmount = taskSetProf.getSchedulerTaskResourceAmount(rName)
    // availableResources 是一个  buffer,    buffer.size  表示可以同时跑多少个 task
    availWorkerResources.get(rName) match {
      case Some(workerRes) =>
        if (workerRes.size >= taskAmount) {
          localTaskReqAssign.put(rName, new ResourceInformation(rName,
            workerRes.take(taskAmount).toArray)) //在 buffer 中预分配 taskAmount 个. 一般只有一个
        } else {
          return None
        }
      case None => return None
    }
  }
  Some(localTaskReqAssign.toMap)
}
```

上面这个阶段是预分配

真正分配Task Resource是在launchTasks中

``` scala
private def launchTasks(tasks: Seq[Seq[TaskDescription]]): Unit = {
  for (task <- tasks.flatten) {
    val serializedTask = TaskDescription.encode(task)
    if (serializedTask.limit() >= maxRpcMessageSize) {
      ...
    }
    else {
      val executorData = executorDataMap(task.executorId) //获得 executor 资源
      // Do resources allocation here. The allocated resources will get released after the task
      // finishes.
      val rpId = executorData.resourceProfileId
      val prof = scheduler.sc.resourceProfileManager.resourceProfileFromId(rpId)
      val taskCpus = ResourceProfile.getTaskCpusOrDefaultForProfile(prof, conf) // 获得 task.cores.
      executorData.freeCores -= taskCpus //更新  executor 可用 cores
      task.resources.foreach { case (rName, rInfo) =>
        assert(executorData.resourcesInfo.contains(rName))
        // 更新 addressAvailabilityMap  中 gpu-id 对应的计数.
        executorData.resourcesInfo(rName).acquire(rInfo.addresses)
      }
      logDebug(s"Launching task ${task.taskId} on executor id: ${task.executorId} hostname: " +
        s"${executorData.executorHost}.")
      executorData.executorEndpoint.send(LaunchTask(new SerializableBuffer(serializedTask)))
    }
  }
}

def acquire(addrs: Seq[String]): Unit = {
  // addrs: gpu id
  addrs.foreach { address =>
     // addressAvailabilityMap 表示 gpuId -> numParts 的映射, 比如 task.amount=0.08, 则 numParts=12,即允许 12个task同时运行
    if (!addressAvailabilityMap.contains(address)) {
      throw new SparkException(s"Try to acquire an address that doesn't exist. $resourceName " +
        s"address $address doesn't exist.")
    }
    val isAvailable = addressAvailabilityMap(address)
    if (isAvailable > 0) {
      addressAvailabilityMap(address) = addressAvailabilityMap(address) - 1 //减去1
    } else {
    }
  }
}
```

## Executor端处理Resource

TaskRunner.run 会触发 Task.run

```java
// Task.run

val taskContext = new TaskContextImpl(
  stageId,
  stageAttemptId, // stageAttemptId and stageAttemptNumber are semantically equal
  partitionId,
  taskAttemptId,
  attemptNumber,
  taskMemoryManager,
  localProperties,
  metricsSystem,
  metrics,
  resources) //将 resource保存到TaskContextImpl中
context = if (isBarrier) {
  new BarrierTaskContext(taskContext)
} else {
  taskContext
}
TaskContext.setTaskContext(context) //保存TaskContextImpl,可以通过TaskContext.get获得
runTask(context)
```

通过上面的代码可知，Executor将resource保存到TaskContextImpl中，而该变量可以直接通过`TaskContext.get`取得，第一节代码中就是这样使用的

当Task执行完成后，Executor会通过StatusUpdate返回给Driver Task执行后的结果.

## Driver端释放resource

DriverPoint在接收到 StatusUpdate后, 将Task占用Executor资源释放给Executor,这样该Executor可以去调度基它Task了。

```java
case StatusUpdate(executorId, taskId, state, data, resources) =>
  scheduler.statusUpdate(taskId, state, data.value)
  if (TaskState.isFinished(state)) { 
    executorDataMap.get(executorId) match {
      case Some(executorInfo) =>
      //如果Task执行finish了，释放资源给Executor
        executorInfo.freeCores += scheduler.CPUS_PER_TASK
        resources.foreach { case (k, v) =>
          executorInfo.resourcesInfo.get(k).foreach { r =>
            r.release(v.addresses)
          }
        }
        makeOffers(executorId)
      case None =>
        // Ignoring the update since we don't know about the executor.
        logWarning(s"Ignored task status update ($taskId state $state) " +
          s"from unknown executor with ID $executorId")
    }
  }
```
