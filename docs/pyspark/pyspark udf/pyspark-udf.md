---
layout: page
title: PySpark与Py4j 
nav_order: 5
parent: PySpark 
---

# PySpark 学习笔记
{: .no_toc}

本文通过实例学习 PySpark udf 是怎么运行的.

## 目录
{: .no_toc .text-delta}

1. TOC
{:toc}


## Java 进程与 Python 进程.

pyspark 也可以直接使用 python udf 或 python 函数来处理数据, 但是 Spark cluster 是使用 jvm 构建
起来的, 也就是说 Spark Driver/Executor 运行在 JVM 进程中的, 显然 JVM 进程是不能直接运行 python 函数的.

为了能在 Spark Cluster 中运行 python 函数, Spark 做了一些特殊的支持. 也就是 Spark 提供特殊的 physical plan
或 RDD 来检测是否需要运行 python 相关的函数. 如果是的话, Spark 会创建 python 进程, 然后通过 socket 使用 java
进程能和 python 进程进行通信, 比如数据传输,计算函数传输等. 下面这张图描述了 python 进程是怎么样产生的,以及数据传输过程

![java python 进程](/docs/pyspark/pyspark%20udf/mapinpandas_pic-jvm-python.drawio.svg)

python 进程统一在 `BasePythonRunner.compute` 中启动, 

![base python runner](/docs/pyspark/pyspark%20udf/mapinpandas_pic-BasePythonRunner.drawio.svg) 目前 spark 3.x
版本上主要是上面4种, 但是在 3.4 中新加入了一个 `ApplyInPandasWithStatePythonRunner`.

在 java 进程中直接 fork 一个 python 进程是比较 expensive. Spark 为了 performance, 默认会


Question:
python iterator 有多少个元素? 即能 hasNext/next 多少次?

对于 PandasUDF, 会将所有的行写入 arrow stream, 然后由
Arrow RecordBatch 个数决定, 默认是 total row/10000 个
由 spark.sql.execution.arrow.maxRecordsPerBatch 控制

对于 Python UDF, 默认是 total rows/100 个 batches


Java 进程向python 进程写入时,会进行很多次 flush,
https://stackoverflow.com/questions/914286/what-does-it-mean-to-flush-a-socket.

比如当把 Task 信息写完后,会强制 flush, 不必等待 socket 本地 buffer 写满, flush的意思
将数据从socket 本地 buffer发送到 python 进程, python 进程就可以读取数据了.

writer 线程一边往python进程写, 而python 进程一边在读, 它们是一起运行. 另外 python 进程
处理完一个 arrow batch 后会把处理结果发送回 java 进程.  spark 读取到了 python 传
过来的数据后 iterator 开始工作, 当 python 还没有返回数据给 spark 时, spark 会 阻塞
在 PythonArrowOutput.newReaderIterator 的 stream.readInt(), 也就是 socket 的 read当中.