---
layout: page
title: Spark pushdown
nav_order: 7
parent: Spark 
---

# Spark Pushdown
{: .no_toc}

减少数据的加载能极大的提升 Spark Performance. Spark 通过将 Filter, Aggregate 下推到距 datasource 最近的地方, 结合具体的 datasource 实现
数据 filter 的功能. 比如 Parquet filter 掉不需要的 RowGroup, Orc filter 掉不需要的 Stripe. 本文基于 Spark 3.3.0-SNAPTHOT 学习 Spark 中 pushdown 功能.

## 目录
{: .no_toc .text-delta}

1. TOC
{:toc}

## filter pushdown

### Spark 对 filter pushdown 的 rule

Spark 中内置很多 rule 来优化 filter pushdown, 其中大多数据 rule 都要求 filter experssion 是 deterministic, 因为 spark optimization 不能保证 expression 计算的顺序, 因此仅对 deterministic 的 filter expression 进行 filter pushdown 的优化

#### CombineFilters

CombineFilters 遍历整个 Analyzed LogicalPlan, 将相邻的两个 Filter (且 child Filter 的 filter expression 是 deterministic)
进行合并.

``` scala
'Filter ('b > 3)
+- 'Filter ('a > 2)
   +- 'Filter ('a > 1)
      +- 'Project ['a]
         +- LocalRelation <empty>, [a#0, b#1, c#2]
```

如上面的 LogicalPlan 经过 CombineFilters 优化后的 LogicalPlan 如下所示,

``` scala
Project [a#0]
+- Filter ((a#0 > 1) AND ((a#0 > 2) AND (b#1 > 3)))
   +- Project [a#0, b#1]
      +- LocalRelation <empty>, [a#0, b#1, c#2]
```

#### PushPredicateThroughNonJoin

非 Join 算子的 filter pushdown, PushPredicateThroughNonJoin 提供了以下的几种 pattern

- Filter -> Project
- Filter -> Aggregate
- Filter -> Window
- Filter -> Union
- Filter -> UnaryNode (任何的 UnaryNode 在满足一定条件时都可以将 Filter 下推)

PushPredicateThroughNonJoin 要求 operator 和 filter expression 是 deterministic.

以 window 为例

``` scala
Filter (a#0 > 1)
+- Project [a#0, b#1, c#2, window#11L]
   +- Project [a#0, b#1, c#2, window#11L, window#11L]
      +- Window [count(b#1) windowspecdefinition(a#0, b#1 ASC NULLS FIRST, specifiedwindowframe(RangeFrame, unboundedpreceding$(), currentrow$())) AS window#11L], [a#0], [b#1 ASC NULLS FIRST]
         +- Project [a#0, b#1, c#2]
            +- LocalRelation <empty>, [a#0, b#1, c#2]
```

Filter 已经 push 到 window 下面了

``` scala
Project [a#0, b#1, c#2, window#11L]
+- Project [a#0, b#1, c#2, window#11L, window#11L]
   +- Window [count(b#1) windowspecdefinition(a#0, b#1 ASC NULLS FIRST, specifiedwindowframe(RangeFrame, unboundedpreceding$(), currentrow$())) AS window#11L], [a#0], [b#1 ASC NULLS FIRST]
      +- Project [a#0, b#1, c#2]
         +- Filter (a#0 > 1)
            +- LocalRelation <empty>, [a#0, b#1, c#2]
```

#### PushPredicateThroughJoin

PushPredicateThroughJoin 单独针对 Join 进行 filter pushdown 的规则. 目前 PushPredicateThroughJoin 匹配如下的规则

- Filter -> Join

  将 Filter 下推到 Join 下, 且更新 Join 的 condition. 该规则针对 Join type 不同, 下推规则也不同 有可能只下推 buildSide, 也有可能只下推 streamSide. 具体的情况具体分析.

如下所示, filter pushdown 到 join left/right 两边

``` scala
x.join(y).where("x.b".attr === 1 && "y.b".attr === 2)
```

``` scala
  Filter (b#11 = 2)
+- Filter (b#1 = 1)
   +- Join Inner // condition = None
      :- LocalRelation <empty>, [a#0, b#1, c#2]
      +- LocalRelation <empty>, [a#10, b#11, c#12]
```

如上所示, Optimizer 首先通过 CombineFilters 将 Filter (b#11 = 2) 与 Filter (b#1 = 1) 合并 为 Filter (b#1 = 1 AND b#11 = 2), 然后再通过 PushPredicateThroughJoin 将 Filter push 到 Join 之下.

``` scala
Join Inner
:- Filter (b#1 = 1)
:  +- LocalRelation <empty>, [a#0, b#1, c#2]
+- Filter (b#11 = 2)
   +- LocalRelation <empty>, [a#10, b#11, c#12]
```

下面的例子只 push 到一边

``` scala
Filter (b#1 = 1)
+- Join Inner
   :- LocalRelation <empty>, [a#0, b#1, c#2]
   +- LocalRelation <empty>, [a#10, b#11, c#12]
```

``` scala
Join Inner
:- Filter (b#1 = 1) // 只 push到一边
:  +- LocalRelation <empty>, [a#0, b#1, c#2]
+- LocalRelation <empty>, [a#13, b#14, c#15]
```

- Join

另一种是Join 中隐式的 Filter, 则直接将 Join condition 下推.

``` scala
Join Inner, ((b#1 = 1) AND (a#10 = 2))
:- LocalRelation <empty>, [a#0, b#1, c#2]
+- LocalRelation <empty>, [a#10, b#11, c#12]
```

如上所示, 整个 Plan 中并没有 Filter. 但是依然需要将 Join condition 下推 

``` scala
Join Inner
:- Filter (b#1 = 1)
:  +- LocalRelation <empty>, [a#0, b#1, c#2]
+- Filter (a#10 = 2)
   +- LocalRelation <empty>, [a#10, b#11, c#12]
```

### push filter 给 DataSource

#### V1 Datasource

针对 V1 Datasource(HadoopFsRelation 表示的 Datasource, 以及 CatalogTable 表示的 table), Spark 通过 FileSourceStrategy rule 将

`LogicalRelation(fsRelation: HadoopFsRelation, _, table, _))`

替换为 FileSourceScanExec Physical Plan 将数据从对应的 Datasource 读出.

正常情况下 Spark Optimzer 已经将 Filter push到离 Datasource 很近了, 因此 Spark 在 `FileSourceStrategy` 中会将 Filter 中的 filter expression 进一步下推到 DataSource.

具体是找到相邻的 [Project, Filter] -> Datasource pattern, 然后从 Filter 中抽取出 Filter condition, 获得 Partition filter 以及 data filter, 最后生成 FileSourceScanExec

``` scala
case class FileSourceScanExec(
    @transient relation: HadoopFsRelation,
    output: Seq[Attribute],
    requiredSchema: StructType,
    partitionFilters: Seq[Expression],           // partition filters
    optionalBucketSet: Option[BitSet],
    optionalNumCoalescedBuckets: Option[Int],
    dataFilters: Seq[Expression],                // data filters
    tableIdentifier: Option[TableIdentifier],
    disableBucketedScan: Boolean = false)
```

FileSourceScanExec首先通过 partitionFilters 与 dataFilters 过滤到不需要读取的 partition 目录.

``` scala
  @transient lazy val selectedPartitions: Array[PartitionDirectory] = {
    ...
    // 根据 partition filter 与 data filter 过滤掉不需要读取的 partitions
    val ret = relation.location.listFiles(
        partitionFilters.filterNot(isDynamicPruningFilter), dataFilters)
    ...
    ret
  }.toArray
```

然后将 data filters 从 catalyst expression 转换为 data source Filter.

最后将转换后的 data source filter 交给对应的 data source 创建 reader

``` scala
relation.fileFormat.buildReaderWithPartitionValues(
  sparkSession = relation.sparkSession,
  dataSchema = relation.dataSchema,
  partitionSchema = relation.partitionSchema,
  requiredSchema = requiredSchema,
  filters = pushedDownFilters, // 转换后的 pushed down filters
  options = relation.options,
  hadoopConf = relation.sparkSession.sessionState.newHadoopConfWithOptions(relation.options))
```

如图所示

![v1 filter pushdown](/docs/spark/filter-pushdown/datareader-v1-buildReader.drawio.svg)

#### V2 Datasource

V2 版的 DataSource LogicalPlan 为 DataSourceV2Relation, Spark Optimizer 为该 LogicalPlan 提供了 `V2ScanRelationPushDown` 优化 aggregate, filter, sample 相关的 pushdown.

对于 filter 的 pushdown, V2ScanRelationPushDown 检查 `Filter -> ScanBuilderHolder(DataSourceV2Relation 的wrapper)` pattern, 然后通过
`PushDownUtils.pushFilters(sHolder.builder, normalizedFiltersWithoutSubquery)` 将 filter push down 到 data source scan builder.

紧接着 `V2ScanRelationPushDown` 通过 `applyColumnPruning` 删除掉不需要的 nested columns, 并更新 read data schema, 创建对应的 scan, 最后更新 DataSourceV2ScanRelation 指向生成好的 scan.

最后在 `DataSourceV2Strategy` 中将 DataSourceV2ScanRelation 替换成 BatchScanExec.

BatchScanExec 通过 Scan 执行 partition filter, 然后创建 FilePartitionReaderFactory, 最后在真正创建 reader 时执行对应的 filter pushdown. 如图所示,

![v2 filter pushdown](/docs/spark/filter-pushdown/datareader-v2-filterpushdown.drawio.svg)

## Aggregate pushdown

TODO