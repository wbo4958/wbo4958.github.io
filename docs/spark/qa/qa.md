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
