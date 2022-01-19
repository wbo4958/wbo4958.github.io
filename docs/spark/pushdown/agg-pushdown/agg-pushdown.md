---
layout: page
title: Aggregate pushdown
nav_order: 7
parent: Pushdown
grand_parent: Spark
---

# Aggregate Pushdown
{: .no_toc}

Spark 3.2 加入了 Aggregate pushdown feature. 该功能通过将部分 Aggreget 函数如(min/max/count 等) push down 到 DataSource, 利用
DataSource(如 parquet/orc) 自带的 statistic 来减少不必要的数据加载进而提升 performance. 目前 Aggregate pushdown 只针对 V2 的 DataSource.

本文基于 Spark 3.3.0-SNAPSHOT 学习 Aggregate pushdown 的实现原理.

## 目录
{: .no_toc .text-delta}

1. TOC
{:toc}

## sample

本地 `~/data/student/student-parquet` 目录下有如下的文件, 可以看得出是按 `class` key 分区的 parquet 文件.

``` bash
xxx:~/data/student/student-parquet$ tree
.
├── class=1
│   └── part-00000-420b3c0a-508e-42d8-bb5d-fbdf645e7206.c000.snappy.parquet
├── class=2
│   └── part-00000-420b3c0a-508e-42d8-bb5d-fbdf645e7206.c000.snappy.parquet
├── class=3
│   └── part-00000-420b3c0a-508e-42d8-bb5d-fbdf645e7206.c000.snappy.parquet
└── _SUCCESS

3 directories, 4 files
```

``` scala
val df = spark.read.parquet("~/data/student/student-parquet")
df.printSchema

root
 |-- name: string (nullable = true)
 |-- number: integer (nullable = true)
 |-- english: float (nullable = true)
 |-- math: integer (nullable = true)
 |-- history: float (nullable = true)
 |-- class: integer (nullable = true) // partition schema
```

df 的 full schema = `partition schema(class)` + `data schema(name, number, english, math, history)`

- spark.sql.parquet.aggregatePushdown = false

``` scala
val df = spark.read.parquet("~/data/student/student-parquet")
df.select(functions.max("english")).explain(true)

== Optimized Logical Plan ==
Aggregate [max(max(english)#15) AS max(english)#13]
+- RelationV2[max(english)#15] parquet file:/home/bobwang/data/student/student-parquet

== Physical Plan ==
*(2) HashAggregate(keys=[], functions=[max(max(english)#15)], output=[max(english)#13])
+- Exchange SinglePartition, ENSURE_REQUIREMENTS, [id=#20]
   +- *(1) HashAggregate(keys=[], functions=[partial_max(max(english)#15)], output=[max#17])
      +- *(1) ColumnarToRow
         +- BatchScan[max(english)#15] ParquetScan DataFilters: [], Format: parquet, Location: InMemoryFileIndex(1 paths)[file:/home/bobwang/data/student/student-parquet], PartitionFilters: [], PushedAggregation: [MAX(english)], PushedFilters: [], PushedGroupBy: [], ReadSchema: struct<max(english):float>, PushedFilters: [], PushedAggregation: [MAX(english)], PushedGroupBy: [] RuntimeFilters: []
```

可以从 Pysical Plan 看出 PushedGroupBy, PushedAggregation 为 空.

- spark.sql.parquet.aggregatePushdown = true

``` scala
val df = spark.read.parquet("~/data/student/student-parquet")
df.select(functions.max("english")).explain(true)

== Optimized Logical Plan ==
Aggregate [max(max(english)#15) AS max(english)#13]
+- RelationV2[max(english)#15] parquet file:/home/bobwang/data/student/student-parquet

== Physical Plan ==
*(2) HashAggregate(keys=[], functions=[max(max(english)#15)], output=[max(english)#13])
+- Exchange SinglePartition, ENSURE_REQUIREMENTS, [id=#20]
   +- *(1) HashAggregate(keys=[], functions=[partial_max(max(english)#15)], output=[max#17])
      +- *(1) ColumnarToRow
         +- BatchScan[max(english)#15] ParquetScan DataFilters: [], Format: parquet, Location: InMemoryFileIndex(1 paths)[file:/home/bobwang/data/student/student-parquet], PartitionFilters: [], PushedAggregation: [MAX(english)], PushedFilters: [], PushedGroupBy: [], ReadSchema: struct<max(english):float>, PushedFilters: [], PushedAggregation: [MAX(english)], PushedGroupBy: [] RuntimeFilters: []
```

PushedAggregation 已经被设置为 `MAX(english)` 了, 且 HashAggregate 的functions也变成了 `max(max(english)`

## push aggregate 到 V2 DataSource (Parquet/OrcScan)

任何支持 aggregate pushdown 的 DataSource 都需要实现 `SupportsPushDownAggregates`, Spark 3.3.0 版本中只有 `ParquetScanBuilder/OrcScanBuilder/JDBCScanBuilder` 实现了该接口.

另外 Spark 在 connector 中定义了 AggregateFunc 来对应支持的 Catalyst Aggregate 函数, 目前, 只有 Sum/Min/Max/Count/CountStar/GenericAggregateFunc 支持.  GenericAggregateFunc 目前也只支持 AVG/VAR_POP/VAR_SAMP/STDDEV_POP 等.

Spark Optimizer 通过 `V2ScanRelationPushDown` rule 将 Aggregate push down 到 scan builder.

具体流程图如下所示

![optimizer-agg-pushdown](/docs/spark/pushdown/agg-pushdown/datareader-agg-pushdown.drawio.svg))

V2ScanRelationPushDown 通过查找 `Aggregate -> [Project, Filter] -> Scan Builder` pattern, 然后找到需要 pushdown 的 Aggregate, 并转换成 connector 的 Aggregate 函数. 最后更新到 Scan builder 里, 并 build 对应的 Scan.

## Parquet 支持 pushdown aggregate

Parquet 文件的 Footer 记录着每个 RowGroup 的统计信息如 (max/min/count), 所以对 Parquet 文件可以进行 Aggregate pushdown. ORC 同理.

![parquet pushdown](/docs/spark/pushdown/agg-pushdown/datareader-parquet-agg-pushdown.drawio.svg)

Spark 读取每个 Parquet 文件的 Footer 信息,　获得每个 RowGroup 的统计信息,　然后根据 pushdown 的　Aggregate 进行计算, 获得最后的结果.　可以看出 Aggregate pushdown 不会读取任何的真实数据,　仅仅读取每个文件的 Footer 信息,　然后进行 Aggregate, 可以说大大的提升 Performance.

但是 Aggregate pushdown 有限制条件.

- 有 data filter 时不能 pushdown.

``` scala
val df = spark.read.parquet("~/data/student/student-parquet")
df.filter("number > 1000").agg(functions.max("english")).explain(true)

== Optimized Logical Plan ==
Aggregate [max(english#2) AS max(english)#19]
+- Project [english#2]
   +- Filter (isnotnull(number#1) AND (number#1 > 1000))
      +- RelationV2[number#1, english#2] parquet file:/home/bobwang/data/student/student-parquet

== Physical Plan ==
*(2) HashAggregate(keys=[], functions=[max(english#2)], output=[max(english)#19])
+- Exchange SinglePartition, ENSURE_REQUIREMENTS, [id=#28]
   +- *(1) HashAggregate(keys=[], functions=[partial_max(english#2)], output=[max#24])
      +- *(1) Project [english#2]
         +- *(1) Filter (isnotnull(number#1) AND (number#1 > 1000))
            +- *(1) ColumnarToRow
               +- BatchScan[number#1, english#2] ParquetScan DataFilters: [isnotnull(number#1), (number#1 > 1000)], Format: parquet, Location: InMemoryFileIndex(1 paths)[file:/home/bobwang/data/student/student-parquet], PartitionFilters: [], PushedAggregation: [], PushedFilters: [IsNotNull(number), GreaterThan(number,1000)], PushedGroupBy: [], ReadSchema: struct<number:int,english:float>, PushedFilters: [IsNotNull(number), GreaterThan(number,1000)], PushedAggregation: [], PushedGroupBy: [] RuntimeFilters: []
```

因为 filter 条件可能会把 max/min 等统计信息过滤掉,　此时从 Footer 中无法准备的知道 max/min/count 等信息

- 当对非 partition key 进行 groupBy agg 时,　不能进行　filter pushdown.

``` scala
val df = spark.read.parquet("~/data/student/student-parquet")
df.groupBy("number").agg(functions.max("english")).explain(true)

== Optimized Logical Plan ==
Aggregate [name#0], [name#0, max(english#2) AS max(english)#19]
+- RelationV2[name#0, english#2] parquet file:/home/bobwang/data/student/student-parquet

== Physical Plan ==
*(2) HashAggregate(keys=[name#0], functions=[max(english#2)], output=[name#0, max(english)#19])
+- Exchange hashpartitioning(name#0, 200), ENSURE_REQUIREMENTS, [id=#20]
   +- *(1) HashAggregate(keys=[name#0], functions=[partial_max(english#2)], output=[name#0, max#25])
      +- *(1) ColumnarToRow
         +- BatchScan[name#0, english#2] ParquetScan DataFilters: [], Format: parquet, Location: InMemoryFileIndex(1 paths)[file:/home/bobwang/data/student/student-parquet], PartitionFilters: [], PushedAggregation: [], PushedFilters: [], PushedGroupBy: [], ReadSchema: struct<name:string,english:float>, PushedFilters: [], PushedAggregation: [], PushedGroupBy: [] RuntimeFilters: []
```

number 为非 partition key, 所以不能进行 agg pushdown. 因为 Parquet 文件中并没有对某些 group 的 max/min/count 相关的统计信息.
