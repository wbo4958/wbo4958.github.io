---
layout: page
title: PySpark与Py4j 
nav_order: 5
parent: PySpark 
---

# PySpark 学习笔记
{: .no_toc}

本文通过实例学习 PySpark.

## 目录
{: .no_toc .text-delta}

1. TOC
{:toc}

## 示例代码

``` python
spark = SparkSession.builder.appName("pyspark learning").master("local[1]").getOrCreate()
df = spark.createDataFrame([
    (1, 2, 3),
    (4, 5, 6),
], ["a", "b", "c"])
df.show()
```

直接运行 `python learning.py`. 从 python 进程端启动一个 pyspark application.

![pyspark overview](/docs/pyspark/pyspark-py4j/pyspark-OverView.drawio.svg)

1. python 进程生成 JavaGateway, 启动 spark-submit 进程
2. SparkSubmit 进程创建一个 JavaServer, 创建 socket 通信, 等着 java 进程连接
3. python 进程通过 py4j 通过 SparkSubmit 进程 import 一些通用类, 比如

``` scala
import org.apache.spark.SparkConf
import org.apache.spark.api.java.*
import org.apache.spark.api.python.*
import org.apache.spark.ml.python.*
import org.apache.spark.mllib.api.python.*
import org.apache.spark.resource.*
import org.apache.spark.sql.*
import org.apache.spark.sql.api.python.*
import org.apache.spark.sql.hive.*
```

4. python 通过 py4j 在 java 进程中创建 SparkConf 对象, python 进程引用到 java 进程的 SparkConf 对象.
5. 当调用 SparkConf API 时, 如果是直接通过 py4j 调用到 java 进程对应的 API.

## java import 过程

下面是具体的 java import 过程.

![java import](/docs/pyspark/pyspark-py4j/pyspark-java_import.drawio.svg)

## py4j protocol

![py4j protocol](/docs/pyspark/pyspark-py4j/pyspark-protocol.drawio.svg)

