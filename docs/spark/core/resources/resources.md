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


### Step 1: Worker 配置 Resources

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

*discoveryScript* 方式是通过 **ResourceDiscoveryScriptPlugin** 开启一个进程去执行 *discoverScript* 脚本

- Plugin 发现资源

用户也可以自定义 `spark.resources.discoveryPlugin` 自定义发现资源的代码.

> 那 Worker 的resource 到底是什么.

Worker 的 `resources: Map[String, ResourceInformation]` 表示整个 worker resource.

以 gpu_fpga_conf.json 为例, `resources` 的值如下所示

``` console
gpu -> ResourceInformation(name=gpu, addresses=Seq(0,1))
fpga -> ResourceInformation(name=fpga, addresses=Seq(f1,f2,f3)
```

### Step 2,3 Worker向Master注册

Worker启动后开始向 Master 进行注册. Worker 的所有信息由 WorkerInfo 表示, 包括了 worker 可用的 cores/memory 以及 Resource 信息.

### Step 4, 5 注册Application

- Application端配置

``` console
--conf spark.executor.resource.gpu.amount=1
--conf spark.executor.resource.fpga.amount=2
```

上面配置表示请求给 executor 分配1个gpu, 2个fpga

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

上面的配置会在创建 SparkContext 时, 创建 ResourceManager 时解析, 并将相关信息保存到 default 的 ResourceProfile 中.
最后将相关的信息一起打包发送到 Master registerApplication.

### Step 6, Master端在worker上分配资源

Master 收到 Spark Application 注册信息后, 开始在已经注册的 Worker 中根据 ResourceProfile 相关信息查找否能在该 Worker 上 launch Executor.
具体参照 `startExecutorsOnWorkers`, 分配的资源会放到 `ExecutorDesc` 中, 接着Master通知Worker启动相关的Executor进程.

### Step 7 Worker启动Executor

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

worker端将分配给executor的`resources` 资源保存**到worker节点的Json文件上**，当创建executor时，将该文件作为 `--resourcesFile executor.json` 启动executor.

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

### Step 8, 9 启动Executor

在**CoarseGrainedExecutorBackend**中

CoarseGrainedExecutorBackend 启动时, 首先会根据 `--resourceProfileId` 获得  rpId, 然后从 driver 端获得 SparkAppConfig 信息, 包括 rpId 对应的 ResourceProfile.

``` scala
val cfg = driver.askSync[SparkAppConfig](RetrieveSparkAppConfig(arguments.resourceProfileId))
```

```scala
override def onStart() {
  logInfo("Connecting to driver: " + driverUrl)
  _resources = parseOrFindResources(resourcesFileOpt) //解析出资源文件

  // 向 driver 注册 Executor 信息,包括 executor cores 与 resources 以及由哪个 rpId 申请创建的 resourceProfile
  driver = Some(ref)
  ref.ask[Boolean](RegisterExecutor(executorId, self, hostname, cores, extractLogUrls,
        extractAttributes, _resources, resourceProfile.id))
  ...
}
```

接着进入 CoarseGrainedExecutorBackend 的 onStart 阶段. 首先解析 resourceFile 获得 Worker 分配给 Executor 的 resource 信息, 然后将 Worker 分配给 Executor 的资源信息包括 cores 的 Resource 向  driver 注册Executor.

resources = `(gpu, ResourceInformation[name: gpu, addresses: 0])`, 将 gpu id=0 分配给该 Executor.

### Step 12. Driver端接受Executor注册信息

当DriverEndpoint收到RegisterExecutor消息后，生成ExecutorData，并放到executorDataMap中. executor的资源信息也同时保存在了ExecutorData中了。

``` scala
val data = new ExecutorData(executorRef, executorAddress, hostname,
            0, cores, logUrlHandler.applyPattern(logUrls, attributes), attributes,
            resourcesInfo, resourceProfileId, registrationTs = System.currentTimeMillis(),
            requestTs = reqTs)

CoarseGrainedSchedulerBackend.this.synchronized {
  executorDataMap.put(executorId, data)
  ...
}
```

通过上面的几个模块的部署，Executor上可用资源信息已经保存到Driver上了，此时Driver可以通过TaskSchedulerImpl去调度Task了

### Step 13. Driver调度Task

`resourceOffers` 将 executor 的 resource 以 round-robin way offer 给 task. 为每个 Task 生成 TaskDescription, 里面包含有 Task 的 Resource 信息.

### Step 14. Task 获得 Resource 信息.

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

通过上面的代码可知，Executor将 TaskDescription 的 resource 信息保存到TaskContextImpl中，而该变量可以直接通过`TaskContext.get`取得，第一节代码中就是这样使用的

当Task执行完成后，Executor会通过StatusUpdate返回给Driver Task执行后的结果.

### Driver端释放resource

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
  }
```
