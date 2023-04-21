---
layout: page
title: Spark connect 实现
nav_order: 10 
parent: Spark 
---

# Spark Connect

本文基于 spark3.4.0 学习 spark connect.

## 目录
{: .no_toc .text-delta}

1. TOC
{:toc}

## 测试代码

根据[官网](https://spark.apache.org/docs/latest/spark-connect-overview.html)的介绍

- 首先在终端上 start connect server

``` bash
./sbin/start-connect-server.sh --packages org.apache.spark:spark-connect_2.12:3.4.0
```

start-connect-server.sh 底层是通过 SparkSubmit 提交的一个 spark application. 因此可以有很多参数可以设置,
比如, 

``` console
--master MASTER_URL, 哪个 spark cluster 上创建 spark connect server, 且 application 要运行的 driver
--driver-memory, driver memory
```

- client 代码 

scala

``` scala
val spark = SparkSession.builder().remote("sc://localhost").build()
val df = spark.read.parquet("~/data/iris/parquet")
val df1 = df.select("class")
df1.collect()
```

python

``` python
from pyspark.sql.connect.session import SparkSession
spark = SparkSession.builder.remote("sc://localhost").getOrCreate()
df = spark.read.parquet("~/data/iris/parquet")
df = df.select("class")
df.collect()
```

## 实现

如官网所示 

![pic](https://spark.apache.org/docs/latest/img/spark-connect-communication.png)

Spark Connect Client 将用户的 Sql 请求通过 Grpc 传递给 Spark Connect Server 进行处理, 并返回 Arrow Format 的数据给用户.

下面是流程图

![connect](/docs/spark/connect/spark-connect.drawio.svg)

如上图所示, 整个流程图大致可分为下面以下 steps


- 1 启动 connect server.

start-connect-server.sh 通过 SparkSubmit 提交 Spark Application, 也就是启动 `org.apache.spark.sql.connect.service.SparkConnectServer.main()`, 而在 SparkConnectServer 的初始化过程中第一步就是创建 Spark Sql SparkSession, 此时初始化 SparkContext, 至此, driver/master/executor 连接将会被建立.

然后 SparkConnectServer 启动 GRPC SparkConnectService, 等着 client 的连接.

- 2 创建 connect client.

用户通过 scala `SparkSession.builder().remote("sc://localhost").build()` 或 python `SparkSession.builder.remote("sc://localhost").getOrCreate()` 建立起与 Spark Connect Server 的 GRPC 连接. 并返回 Spark Connect 的 SparkSession, 该 SparkSession wrap了 connect client 等相关信息. 注意,该 SparkSession 是 Spark Connect SparkSession, 而非 Spark Sql 的 SparkSession.

- 3 read.parquet()

Spark Connect 的 SparkSession API 基本与  Spark SQL SparkSession AI 一样, 比如 spark.read.parquet() 读取 parquet files. 生成 DataFrame 和一些 Plan(Relation_Read_DataSource), 这些 Plan 是 proto 生成的, 非语言相关的, 而非 Spark SQL 里的 LogicalPlan, 不过与 Spark SQL 里的 LogicalPlan 是一一对应的

- 4 select()

生成新的 DataFrame 和 新的 Plan, Relation_Project_Read_DataSource

- 5 执行 collect operation

collect() operation 会触发 Client 调用 execute 函数. 将 Plan 发送到 connect Server 端进行处理.

- 6 connect server 执行 executePlan()

connect server 收到 client execute 请求后在当前线程中 执行 executePlan. 更具体一点, 为每个 client 请求生成 SparkConnectStreamHandler 进行单独处理.

- 7 生成 isolated spark sql SparkSession

SparkConnectStreamHandler 首先要做的一点是为该 user/sessionId 创建一个新的 SparkSession. 然后开始进行处理.

- 8 transform plan

将 client 提交过来的 Plan (Relation_Project_Read_DataSource) map/transform 到 SparkSql 的 LogicalPlan. 最后生成 SparkSql 的 DataFrame

- 9 提交 Job

获得 SparkSql的DataFrame 后, 然后开始 analyze/optimize/physical plan/executedplan. 最后将 physical plan 转换为 RDD[InternalRow], 接着对该 RDD 作一次 mapPartition, 将 row 转换为 Arrow format bytes. 最后通过 SparkContext 提交 job. 因此最后的 collect 回来的结果是 Arrow 格式的 format.

- 10 返回结果

spark cluster 处理完结果后, 将 Arrow 格式的数据返回给 client 端.
- 