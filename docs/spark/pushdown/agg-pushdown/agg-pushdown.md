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

有3种 scenario

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

- group by on partiton key when spark.sql.parquet.aggregatePushdown = true

``` scala
val df = spark.read.parquet("~/data/student/student-parquet")
df.groupBy("class").agg(functions.max("english")).explain(true)

== Optimized Logical Plan ==
Aggregate [class#5], [class#5, max(max(english)#23) AS max(english)#19]
+- RelationV2[class#5, max(english)#23] parquet file:/home/bobwang/data/student/student-parquet

== Physical Plan ==
*(2) HashAggregate(keys=[class#5], functions=[max(max(english)#23)], output=[class#5, max(english)#19])
+- Exchange hashpartitioning(class#5, 200), ENSURE_REQUIREMENTS, [id=#20]
   +- *(1) HashAggregate(keys=[class#5], functions=[partial_max(max(english)#23)], output=[class#5, max#25])
      +- *(1) ColumnarToRow
         +- BatchScan[class#5, max(english)#23] ParquetScan DataFilters: [], Format: parquet, Location: InMemoryFileIndex(1 paths)[file:/home/bobwang/data/student/student-parquet], PartitionFilters: [], PushedAggregation: [MAX(english)], PushedFilters: [], PushedGroupBy: [class], ReadSchema: struct<class:int,max(english):float>, PushedFilters: [], PushedAggregation: [MAX(english)], PushedGroupBy: [class] RuntimeFilters: []
```

同时 push down 了 GroupBy columns 和 Aggregation

- group by on non-partition key when spark.sql.parquet.aggregatePushdown = true

``` scala
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

没有下推任何 aggregate.

## push aggregate 到 V2 DataSource (Parquet/OrcScan)

任何支持 aggregate pushdown 的 DataSource 都需要实现 `SupportsPushDownAggregates`, 当前 Spark 版本中只有 ParquetScanBuilder/OrcScanBuilder/JDBCScanBuilder 实现了该接口.

另外 Spark 在 connector 中定义了 AggregateFunc 来对应支持的 Catalyst Aggregate 函数,　目前, 只有 Sum/Min/Max/Count/CountStar/GenericAggregateFunc 支持.  GenericAggregateFunc 目前也只支持 AVG/VAR_POP/VAR_SAMP/STDDEV_POP 等.

Spark Optimizer 通过 `V2ScanRelationPushDown` rule 将 Aggregate push down 到 scan builder.

具体流程图如下所示

![optimizer-agg-pushdown](/docs/spark/pushdown/agg-pushdown/datareader-agg-pushdown.drawio.svg))

V2ScanRelationPushDown 通过找查 `Aggregate -> [Project, Filter] -> Scan Builder` pattern, 然后找到需要 pushdown 的 Aggregate, 并转换成 connector 的 Aggregate 函数. 最后更新到 Scan builder 里, 最后生成相应的 Scan.

## Parquet 支持 pushdown aggregate


