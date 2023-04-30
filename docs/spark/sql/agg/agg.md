---
layout: page
title: Aggregation
nav_order: 13 
parent: sql
grand_parent: spark 
---

# Spark Aggregation
{: .no_toc}

本文通过代码学习 Spark 中 Aggregation 的实现. 本文基于 Spark 3.2.0

## 目录
{: .no_toc .text-delta}

1. TOC
{:toc}

## 测试代码

``` scala
val df0 = Seq(
  ("IT", "aaa", 21),
  ("Sales", "bbb", 43),
  ("IT", "ccc", 35),
  ("IT", "ddd", 32),
  ("Sales", "eee", 50),
  ("Sales", "fff", 33),
  ("HR", "ggg", 25),
  ("HR", "hhh", 27),
  ("IT", "iii", 27),
).toDF("dept", "name", "age")

// Test HashAggregateExec
//      val df = df0.groupBy("dept").max("name")
val df = df0.groupBy("dept").agg(functions.max('age))

// Test ObjectHashAggregateExec
// val df = df0.groupBy("dept").agg(functions.collect_list('age))

// Test SortAggregateExec
// spark.conf.set("spark.sql.execution.useObjectHashAggregateExec", "false")
// val df = df0.groupBy("dept").agg(functions.max('name))

// Test Reduction
// val df = df0.agg(functions.max('name))

df.explain(true)
df.show()
```

## LogicalPlan

Spark 使用 Aggregate 来表示一个 GroupBy operator.

### Aggregate

| Aggregate | 描述 GroupBy operator |
| --- | --- |
| groupingExpressions: Seq[Expression] | 需要对哪些 column 进行 group by |
| aggregateExpressions: Seq[NamedExpression] | group by columns + agg(聚合) expressions |

> `spark.sql.retainGroupColumns`　控制是否保留 group by 的 columns. 默认为 true.

`val df = df0.groupBy("dept").agg(functions.max('age))` 优化后的 LogicalPlan 如下所示,

``` scala
== Optimized Logical Plan ==

//第一个参数表示 group by columns
// 第二个参数是 group by column + agg expression(max("age"))
Aggregate [dept#10], [dept#10, max(age#12) AS max(age)#20]
+- LocalRelation [dept#10, age#12]
```

如果 Aggregate 中 groupingExpressions 为空, 此时 Aggregatiion 退化为 Reduction.

如

``` scala
scala> df0.select(max('age)).explain(true) //或 df0.agg(max('age)).show()

== Optimized Logical Plan ==
Aggregate [max(age#12) AS max(age)#17]
+- LocalRelation [age#12]

+--------+
|max(age)|
+--------+
|      50|
+--------+
```

此时对整个 agg 列做 reduction 操作.

如果带上 group by 后, 只对具有相同值的 group 分别做 reduction. 如,

``` scala
scala> df0.groupBy("dept").max("age").show()
+-----+--------+
| dept|max(age)|
+-----+--------+
|Sales|      50|
|   HR|      27|
|   IT|      35|
+-----+--------+
```

### AggregateExpression

| AggregateExpression | 聚合表达式 |
| --- | --- |
| aggregateFunction: AggregateFunction | 聚合函数 |
| mode: AggregateMode | 聚合方式, Complete/Partial/PartialMerge/Final |
| isDistinct | 是否有 DISTINCT 修饰该 function |
| filter: Option[Expression | 该 function 是否有 filter |

下图是 Spark built-in 的 AggregateFunction

![AggregateFunction](/docs/spark/agg/AggregationFunction.png)

AggregateFunction 分为2类

- DeclarativeAggregate (声明式的函数)
  
  告诉机器我想要什么样的结果

- ImperativeAggregate (命令式的函数)

  告诉机器怎么去实现得到结果

比如

- CollectList(ImperativeAggregate) 创建一个 ArrayBuffer， 然后不断往里加入元素
- Max(DeclarativeAggregate) 告诉机器 `初始化值/怎么更新/怎么merge` 即可

关于 Declarative/Imperative 的区别可以参考 https://stackoverflow.com/questions/1784664/what-is-the-difference-between-declarative-and-imperative-paradigm-in-programmin

## Aggregation 的实现

Spark 目前提供3种不同的方式实现 Aggregation

![plan-agg](/docs/spark/agg/agg-plan_agg.svg)

分别为 HashAggregateExec/ObjectHashAggregateExec/SortAggregateExec

### HashAggregateExec

HashAggregateExec 只适用于那些 `Agg column` 能在 UnSafeRow 上直接更新的数据类型，如

`Boolean/Byte/Short/Integer/Long/Float/Double/Date/TimeStamp/Decimal/Calendar/DayTime/YearMonth`

因此可以大概猜出 HashAggregateExec 的实现是为每个 `group by key` 生成一个 Agg buffer, 而该 agg buffer 是基于 UnsafeRow, initialize/update/merge 直接在该 UnsafeRow 进行操作.

- SparkPlan

![HashAggregateExec](/docs/spark/agg/agg-HashAggregateExec.svg)

HashAggregateExec 支持 CodeGen, 它生成的代码可以参考 [aggregation-max](https://github.com/wbo4958/wbo4958.github.io/blob/master/data/groupby_max.java)

- HashAggregateExec doAgg
  
![doAgg](/docs/spark/agg/agg-HashAgg_doAgg.svg)

HashAggregateExec 首先尝试 FastHashMap, 然后再尝试 Regular HashMap.

FastHashMap 是一个固定数量的 HashMap, 内部采用 `int[(1<<16) * 2] buckets` 来记录 `row number`, 这个 `row number` 是 `(group by key, agg buffer)` 保存在 RowBasedKeyValueBatch 中的位置. 即 `group by key` 的hash值先从 buckets 中索引到 `row number`，然后再根据 `row number` 在 RowBasedKeyValueBatch 找到 `(group by key, agg buffer)`

之所以称为 Fast HashMap 是因为它是固定大小，即最多保存 `(1<<16)*2` 个 `group by key`. 且当存在碰撞冲突时，只往下尝试2次，如果没有找到，则直接返回，不会增加 size

而 Regular HashMap 当初始化分配的 size 不够存储时，此时会重新申请 memory, 并且 rehash. 当内存不足时，会 spill到磁盘. 这是一个比较 pool performance 的地方

### ObjectHashAggregateExec

![ObjectHashAggregateExec](/docs/spark/agg/agg-ObjectHashAggregateExec.svg)

`spark.sql.execution.useObjectHashAggregateExec` 控制是否使用ObjectHashAggregateExec, 且 ObjectHashAggregateExec 只支持 TypedImperativeAggregate(允许任意的user defined jvm 对象作为 agg buffer), 如 CollectList/CollectSet.

ObjectHashAggregateExec 首先使用 HashMap 存储直接以 `group By` 整个 UnsafeRow 作为 Key, 然后 agg buffer Unsafe Row 作为 HashMap Value. HashMap 最多保存 `spark.sql.objectHashAggregate.sortBased.fallbackThreshold` 默认 128 个 group by keys. 当 group by keys 大于该 threshold, fallback 到 SortBased aggregation, SortBased agg 方式可以动态增加 memory 以及 spill.

- ObjectAggregationIterator

![ObjectAggregationIterator](/docs/spark/agg/agg-ObjectAggregationIterator.svg)

### SortAggregateExec

![SortAggregateExec](/docs/spark/agg/agg-SortAggregateExec.svg)

当 plan aggregate 时，如果 `HashAggregateExec/ObjectHashAggregateExec` 没有被匹配上，则默认使用 SortAggregateExec. SortAggregateExec 会要求对 group by 先进行 sort. 然后再做 agg.

![SortBasedAggregationIterator](/docs/spark/agg/agg-SortBasedAggregationIterator.svg)
