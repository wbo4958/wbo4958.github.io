---
layout: page
title: Dataset cache
nav_order: 10
parent: BlockManager
grand_parent: Spark
---

# Dataset cache
{: .no_toc}

本文通过代码学习 Dataset cache的工作机制. 本文基于 Spark 3.1.1.

## 目录
{: .no_toc .text-delta}

1. TOC
{:toc}

## 示例代码

``` scala
import spark.sqlContext.implicits._

val df0 = Seq(1, 2, 3, 4).toDS()
val df1 = df0.mapPartitions { iter => iter.map { v => v + 10 } }
val df2 = df1.mapPartitions { iter => iter.map { v => v + 100 } }

df2.cache()

val df3 = df2.mapPartitions { iter => iter.map { v => v + 1000 } }
df3.explain(true)
assert(df3.collect().toList === List(1111, 1112, 1113, 1114))
assert(df3.collect().toList === List(1111, 1112, 1113, 1114))
```

在学习 Dataset cache 功能前,我们先来看看 **没有cache** 和 **有cache** 时,df3 对应的 SparkPlan 的变化

- 去掉 df2.cache

``` console
== Analyzed Logical Plan ==
value: int
SerializeFromObject [input[0, int, false] AS value#15]
+- MapPartitions org.apache.spark.sql.BobbySparkSql$$Lambda$1355/1151370725@6a756082, obj#14: int
   +- DeserializeToObject assertnotnull(cast(value#11 as int)), obj#13: int
      +- SerializeFromObject [input[0, int, false] AS value#11]
         +- MapPartitions org.apache.spark.sql.BobbySparkSql$$Lambda$1354/104070545@6b63e6ad, obj#10: int
            +- DeserializeToObject assertnotnull(cast(value#7 as int)), obj#9: int
               +- SerializeFromObject [input[0, int, false] AS value#7]
                  +- MapPartitions org.apache.spark.sql.BobbySparkSql$$Lambda$1342/1346667529@7beae796, obj#6: int
                     +- DeserializeToObject assertnotnull(cast(value#1 as int)), obj#5: int
                        +- LocalRelation [value#1]

== Optimized Logical Plan ==
SerializeFromObject [input[0, int, false] AS value#15]
+- MapPartitions org.apache.spark.sql.BobbySparkSql$$Lambda$1355/1151370725@6a756082, obj#14: int
   +- MapPartitions org.apache.spark.sql.BobbySparkSql$$Lambda$1354/104070545@6b63e6ad, obj#10: int
      +- MapPartitions org.apache.spark.sql.BobbySparkSql$$Lambda$1342/1346667529@7beae796, obj#6: int
         +- DeserializeToObject value#1: int, obj#5: int
            +- LocalRelation [value#1]

== Physical Plan ==
*(2) SerializeFromObject [input[0, int, false] AS value#15]
+- MapPartitions org.apache.spark.sql.BobbySparkSql$$Lambda$1355/1151370725@6a756082, obj#14: int
   +- MapPartitions org.apache.spark.sql.BobbySparkSql$$Lambda$1354/104070545@6b63e6ad, obj#10: int
      +- MapPartitions org.apache.spark.sql.BobbySparkSql$$Lambda$1342/1346667529@7beae796, obj#6: int
         +- *(1) DeserializeToObject value#1: int, obj#5: int
            +- *(1) LocalTableScan [value#1]
```

对应的图如下所示

![df-no-cache](/docs/spark/blockmanager/cache/cache-df-no-cache.svg)

其中 WholeStage1 将 InternalRow 转变 (Deserialize) 成 JVM 对象传递给后面的 Operator 也就是 mapPartition,
而 WholeStage2 将 JVM 对象 (Serialize) 成 InternalRow 传递给后面的 Operator 继续处理.

三个 MapPartitions 对应的是代码中的 3 个 mapPartions 函数. 对应转换成 3 个 MapPartitionsRDD.

- 加上 df2.cache

``` console
== Analyzed Logical Plan ==
value: int
SerializeFromObject [input[0, int, false] AS value#20]
+- MapPartitions org.apache.spark.sql.BobbySparkSql$$Lambda$1471/313334570@4232b34a, obj#19: int
   +- DeserializeToObject assertnotnull(cast(value#11 as int)), obj#18: int
      +- SerializeFromObject [input[0, int, false] AS value#11]
         +- MapPartitions org.apache.spark.sql.BobbySparkSql$$Lambda$1355/104070545@5d4e13e1, obj#10: int
            +- DeserializeToObject assertnotnull(cast(value#7 as int)), obj#9: int
               +- SerializeFromObject [input[0, int, false] AS value#7]
                  +- MapPartitions org.apache.spark.sql.BobbySparkSql$$Lambda$1343/1346667529@3e0fbeb5, obj#6: int
                     +- DeserializeToObject assertnotnull(cast(value#1 as int)), obj#5: int
                        +- LocalRelation [value#1]

== Optimized Logical Plan ==
SerializeFromObject [input[0, int, false] AS value#20]
+- MapPartitions org.apache.spark.sql.BobbySparkSql$$Lambda$1471/313334570@4232b34a, obj#19: int
   +- DeserializeToObject value#11: int, obj#18: int
      +- InMemoryRelation [value#11], StorageLevel(disk, memory, deserialized, 1 replicas)
            +- *(2) SerializeFromObject [input[0, int, false] AS value#11]
               +- MapPartitions org.apache.spark.sql.BobbySparkSql$$Lambda$1355/104070545@5d4e13e1, obj#10: int
                  +- MapPartitions org.apache.spark.sql.BobbySparkSql$$Lambda$1343/1346667529@3e0fbeb5, obj#6: int
                     +- *(1) DeserializeToObject value#1: int, obj#5: int
                        +- *(1) LocalTableScan [value#1]

== Physical Plan ==
*(2) SerializeFromObject [input[0, int, false] AS value#20]
+- MapPartitions org.apache.spark.sql.BobbySparkSql$$Lambda$1471/313334570@4232b34a, obj#19: int
   +- *(1) DeserializeToObject value#11: int, obj#18: int
      +- *(1) ColumnarToRow
         +- InMemoryTableScan [value#11]
               +- InMemoryRelation [value#11], StorageLevel(disk, memory, deserialized, 1 replicas)
                     +- *(2) SerializeFromObject [input[0, int, false] AS value#11]
                        +- MapPartitions org.apache.spark.sql.BobbySparkSql$$Lambda$1355/104070545@5d4e13e1, obj#10: int
                           +- MapPartitions org.apache.spark.sql.BobbySparkSql$$Lambda$1343/1346667529@3e0fbeb5, obj#6: int
                              +- *(1) DeserializeToObject value#1: int, obj#5: int
                                 +- *(1) LocalTableScan [value#1]
```

从上面的 SparkPlan 链可以看出 `df2.cache` 改变了 Optimized Logical Plan, 新增加了一个 InMemoryRelation Plan.
下面我们来继续介绍 Dataset cache 的原理.

## Dataset cache 原理

- df2.cache()

![cache API](/docs/spark/blockmanager/cache/cache-df-cache-flow.svg)

上图是 cache 的流程图.

df2.cache 首先通过 key (df2中 analyzed logical plan) 向 CacheManager 查询是否已经缓存过了,如果缓存过了
直接pass, 如果没有缓存,会生成一个 InMemoryRelation 来缓存 df2 的 executedPlan, 并记录 storageLevel.
最后缓存到 CacheManager 中.

`df3 = df2.mapPartition { v => v + 1000 }` 在 df2 之上新增一个 mapPartition transform. df3 创建
executedPlan 的过程如下

`executedPlan <- sparkPlan <- optimizedPlan <- withCachedData <- analyzedPlan`

``` scala
  lazy val withCachedData: LogicalPlan = sparkSession.withActive {
    assertAnalyzed()
    assertSupported()
    // clone the plan to avoid sharing the plan instance between different stages like analyzing,
    // optimizing and planning.
    sparkSession.sharedState.cacheManager.useCachedData(analyzed.clone())
  }

  def useCachedData(plan: LogicalPlan): LogicalPlan = {
    val newPlan = plan transformDown { // 递归的查找 plan 是否已经被 cache
      case command: IgnoreCachedData => command

      case currentFragment =>
        lookupCachedData(currentFragment).map { cached =>
          // After cache lookup, we should still keep the hints from the input plan.
          val hints = EliminateResolvedHint.extractHintsFromPlan(currentFragment)._2
          val cachedPlan = cached.cachedRepresentation.withOutput(currentFragment.output)
          // The returned hint list is in top-down order, we should create the hint nodes from
          // right to left.
          hints.foldRight[LogicalPlan](cachedPlan) { case (hint, p) =>
            ResolvedHint(p, hint)
          }
        }.getOrElse(currentFragment)
    }

    newPlan transformAllExpressions {
      case s: SubqueryExpression => s.withNewPlan(useCachedData(s.plan))
    }
  }  
```

withCacheData 递归遍历 df3 的 analyzed logical plan, 如果发现某个 logical plan 已经被缓存过,
那用缓存的 InMemoryRelation 来替换该 LogicalPlan.

如 explain 所示

``` console
== Optimized Logical Plan ==
SerializeFromObject [input[0, int, false] AS value#20]
+- MapPartitions org.apache.spark.sql.BobbySparkSql$$Lambda$1471/313334570@4232b34a, obj#19: int
   +- DeserializeToObject value#11: int, obj#18: int
      +- InMemoryRelation [value#11], StorageLevel(disk, memory, deserialized, 1 replicas)
            +- *(2) SerializeFromObject [input[0, int, false] AS value#11]
               +- MapPartitions org.apache.spark.sql.BobbySparkSql$$Lambda$1355/104070545@5d4e13e1, obj#10: int
                  +- MapPartitions org.apache.spark.sql.BobbySparkSql$$Lambda$1343/1346667529@3e0fbeb5, obj#6: int
                     +- *(1) DeserializeToObject value#1: int, obj#5: int
                        +- *(1) LocalTableScan [value#1]
```

最后 ExecutedPlan 转换成 RDD 如下所示

![whole stage](/docs/spark/blockmanager/cache/cache-df3-cache-wholestage.svg)

可以看到 InMemoryTableScanExec 会调用 `MapPartitionsRDD[6].persist`, 后面的 RDD cache 过程可以参考
[RDD cache](cache.md)
