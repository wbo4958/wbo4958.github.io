---
layout: page
title: QA
nav_order: 200000
parent: spark 
---

# Spark QA

## 目录
{: .no_toc .text-delta}

1. TOC
{:toc}

## Spark Connect configurations

在 spark connect 下, 对于 Runtime configurations 比如 `spark.sql.execution.arrow.maxRecordsPerBatch`, 每个 client 可以设置为不同的值么? 答案是可以的.

![spark-connect-configurations](/docs/spark/qa/images/spark-connect-configurations.svg)

- When the property is explicitly set during server startup:

If spark.sql.execution.arrow.maxRecordsPerBatch is explicitly configured when starting the connect server (e.g., via start-connect-server.sh), this configuration is stored in the local properties of the SparkContext. As a result, all tasks executed will use the specified value.

- When the property is not set during server startup:

If spark.sql.execution.arrow.maxRecordsPerBatch is not explicitly set during server startup, the local properties
of the SparkContext will not include this configuration. In this case, all tasks will fall back to the default value, which is 10,000.

- When a user explicitly sets the property in a SparkSession:

If a user explicitly sets spark.sql.execution.arrow.maxRecordsPerBatch within their SparkSession,
this value will override the global configuration (if any). Consequently, the tasks executed within this user's SparkSession
will use the user-specified value instead of the global or default value.


## Spark memory configurations

| conf    | Default|Meaning | Standalone | Yarn | K8s |
| -------- | ------- | ------- | ------- | ------- |
| spark.executor.memory | 1G | Heap Memory for Executor  | Y | Y| Y|
| spark.executor.memoryOverhead | None | Overhead Memory for Executor. Used for JVM overheads. internal strings, and other native library | **N** | Y | Y|
| spark.executor.memoryOverheadFactor | 0.1 | Fraction of executor memory to be allocated as additional non-heap memory | **N** | Y | Y |
| spark.memory.offHeap.size spark.memory.offHeap.enabled | 0, false | Specifies the amount of off-heap memory (memory outside the JVM heap) that Spark can use for specific operations, such as Tungsten memory management or other off-heap data structures. It is part of Spark's memory management system but is separate from the unified memory model (execution, storage, and user memory). | N | Y | Y |
| spark.executor.pyspark.memory | None | Defines the amount of off-heap memory allocated to the Python process (used by PySpark) within each Spark executor | Y | Y | Y | 

对于 **Standalone**, Executor 内存只受 `spark.executor.memory` 控制，　而 overhead 不受任何参数控制，意味着 executor native 操作可以使用整个计算机的所有内存.

对于 **Yarn** 和 **K8s**, 如果设置了　`spark.executor.memoryOverhead`，　那整个 executor 内存大小为 `spark.executor.memory` + `spark.executor.memoryOverhead`,
如果　`spark.executor.memoryOverhead`　没有设置，　则整个 executor 内存大小为 `spark.executor.memory` * (1 + `spark.executor.memoryOverheadFactor`)

对于 `spark.executor.pyspark.memory`　这是一个 executor 能使用到的 python memory, 而 **每个 task** 所需要的 `python worker memory` = `spark.executor.pyspark.memory` / executor_cores.

所以对于　Yarn 和 K8s,

`total memory required` = `spark.executor.memory` + `spark.memory.offHeap.size` + `executorMemoryOverhead` + `spark.executor.pyspark.memory`

executorMemoryOverhead = `spark.executor.memoryOverhead` if `spark.executor.memoryOverhead` is set, else `spark.executor.memory` * (1 + `spark.executor.memoryOverheadFactor`)
