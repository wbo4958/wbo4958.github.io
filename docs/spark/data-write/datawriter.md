---
layout: page
title: Data writer
nav_order: 5
parent: Spark 
---

# Spark Data writer
{: .no_toc}

本文sample学习 Spark 3.2.0 中 Data Writer 的过程.

## 目录
{: .no_toc .text-delta}

## Spark DataWritingCommand

Spark catalyst 有一类特别的 non-query LogicalPlan - Command, 对应于 DDL (Data Definition Language) 如 `CREATE DATABASE xxx` 以及 DML (Data Manipulation Language) 如 `INSERT INTO` 操作. Spark query plan 是 lazy 执行的， 而 Command 是非 lazy, 是 "eagerly" 执行的.

DataWritingCommand 是写数据的 Command, 下面是与非 Hive 相关的实现类

![DataWritingCommand](/docs/spark/data-write/datawrite-DataWritingCommand.svg)

DataWritingCommand 中定义了 query, 即生成待写入到文件中的数据.

InsertIntoHadoopFsRelationCommand 将数据写入到 HDFS-compatible 的文件系统中, 常见于如下代码中

``` scala
df.write.parquet("hdfs:/xxx")
df.write.orc("hdfs:/xxx)
```

CreateDataSourceTableAsSelectCommand 是使用 query 的结果来创建 Data source Table 的 command. 常见于

``` sql
CREATE TABLE xxx
USING format parquet
AS SELECT ...
```

或直接 saveAsTable

``` scala
df.write.saveAsTable("xxx")
```

为什么说 Command 是 eagerly 执行的呢?

``` scala
lazy val commandExecuted: LogicalPlan = mode match {
  case CommandExecutionMode.NON_ROOT => analyzed.mapChildren(eagerlyExecuteCommands)
  case CommandExecutionMode.ALL => eagerlyExecuteCommands(analyzed)
  case CommandExecutionMode.SKIP => analyzed
}

// eagerlyExecuteCommands
private def eagerlyExecuteCommands(p: LogicalPlan) = p transformDown {
  case c: Command =>
    val qe = sparkSession.sessionState.executePlan(c, CommandExecutionMode.NON_ROOT)
    val result = SQLExecution.withNewExecutionId(qe, Some(commandExecutionName(c))) {
      qe.executedPlan.executeCollect()
    }
    CommandResult(
      qe.analyzed.output,
      qe.commandExecuted,
      qe.executedPlan,
      result)
  case other => other
}
```

在 QueryExecution 中定义了 **commandExecuted**, 对于 ALL|NON_ROOT 模式，会触发 eagerlyExecuteCommands, 在 eagerlyExecuteCommands 执行过程中，对于 Command的 LogicalPlan 会执行触发 executeCollect 操作. 而 commandExecuted 会在 `QueryExecution.optimizedPlan` 或 `DataFrameWriter.runCommand` 以及 `Dataset.logicalPlan` 中触发. 这些触发条件往往发生的很早，远早于 执行Dataset的 action.

BasicOperators 将所有的 DataWritingCommand LogicalPlan 替换为 DataWritingCommandExec, 在 DataWritingCommandExec 中 executeCollect 触发 DataWritingCommand.run

## InsertIntoHadoopFsRelationCommand

``` scala
val df = Seq(
  (1, "apple"), (2, "orange"), (3, "kiwi"), (4, "pear"), (5, "lemon"),
  (6, "banana"), (7, "blackberry"), (8, "strawberry"), (9, "cherry"), (10, "grape"),
  (11, "mango"), (12, "peach"), (13, "pineapple")
).toDF("id", "fruit")

df.write.mode(SaveMode.Overwrite).parquet("/tmp/parquet")
```

上述代码将 Local 数据写入到本地文件中.

![InsertIntoHadoopFsRelationCommand](/docs/spark/data-write/datawrite-cpu_save.svg)

上述 data write 非为 2 步， 第一步生成 InsertIntoHadoopFsRelationCommand, 第二次 run InsertIntoHadoopFsRelationCommand.

## CreateDataSourceTableAsSelectCommand

``` scala
val df = Seq(
  (1, "apple"), (2, "orange"), (3, "kiwi"), (4, "pear"), (5, "lemon"),
  (6, "banana"), (7, "blackberry"), (8, "strawberry"), (9, "cherry"), (10, "grape"),
  (11, "mango"), (12, "peach"), (13, "pineapple")
).toDF("id", "fruit")

df.write.mode(SaveMode.Overwrite)
  .bucketBy(3, "id")
  .option("path", "/tmp/bucket")
  .saveAsTable("bucket")
```

CreateDataSourceTableAsSelectCommand 会生成 InsertIntoHadoopFsRelationCommand, 然后通过 InsertIntoHadoopFsRelationCommand.run 来进行 data write.
