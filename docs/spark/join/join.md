---
layout: page
title: Join
nav_order: 13 
parent: Spark 
---

# Spark Join
{: .no_toc}

本文通过代码学习 Spark 中 Join 的实现. 本文基于 Spark 3.2.0

## 目录
{: .no_toc .text-delta}

1. TOC
{:toc}

## 测试代码

``` console
inner join
+------+---------+---------+------+
|std_id|dept_name|dept_name|std_id|
+------+---------+---------+------+
|     2|     math|     math|     7|
|     2|     math|     math|     1|
|     4|     math|     math|     7|
|     4|     math|     math|     1|
+------+---------+---------+------+

leftouter join
+------+---------+---------+------+
|std_id|dept_name|dept_name|std_id|
+------+---------+---------+------+
|     3|  history|     null|  null|
|     2|     math|     math|     7|
|     2|     math|     math|     1|
|     5|  history|     null|  null|
|     4|     math|     math|     7|
|     4|     math|     math|     1|
+------+---------+---------+------+

leftsemi join
+------+---------+
|std_id|dept_name|
+------+---------+
|     2|     math|
|     4|     math|
+------+---------+

leftanti join
+------+---------+
|std_id|dept_name|
+------+---------+
|     3|  history|
|     5|  history|
+------+---------+

rightouter join
+------+---------+---------+------+
|std_id|dept_name|dept_name|std_id|
+------+---------+---------+------+
|  null|     null|    piano|     3|
|     4|     math|     math|     1|
|     2|     math|     math|     1|
|  null|     null|   guitar|     3|
|     4|     math|     math|     7|
|     2|     math|     math|     7|
+------+---------+---------+------+
```

## xxx

| Join | |
| ---- | --- |
| left: LogicalPlan | |
| right: LogicalPlan | |
| joinType | InnerLike(Cross/Inner) / LeftSemi / RightOuter / NaturalJoin /LeftOuter / FullOuter / LeftAnti |
| condition: Option[Expression] | Join条件|
| hint: JoinHint | Join hint |

## Join

Join 是一个算子， Spark对 Join 算子有多种实现, 如 BHJ/SMJ/SHJ ... 这些实现最后生成的 Join 结果是一致的, 但是不同的实现应用的场景或许不一样，自然不同的实现带来的 performance 也不一样.

![join plan](/docs/spark/join/join-plan-join.svg)

简化成如下这张表

![join plan condition](/docs/spark/join/join-plan-condition.svg)

### BroadcastHashJoinExec

- BuildSide

  有 BuildLeft/BuildRight 之分, BuildSide 表示先 **build(执行)** 哪边的 plan, 一般来说是 build 小表, 然后将该小表 广播到 executor 中.

- buildPlan
  
  buildPlan 是需要事先 build 的 plan. Driver端事先异步执行 `buildPlan.doExecuteBroadcast`, 获得 buildPlan 的结果，并保存到 BlockManager 中， executor 端使用到broadcast,再从 driver 端获得该广播值.

- streamPlan

  BroadcastHashJoinExec 进行 join 时，依次遍历 streamPlan 的行，然后在 buildPlan 中通过 joinKey 的 hash 值找到 match 时行 join.

![bhj](/docs/spark/join/join-bhj-execute.svg)

如果简单的来说就是

第一步. BroadcastHashJoinExec 首先将小表 collect 回 driver, 然后依次遍历所有行，并插入到 Relation (可以看成是一个 HashMap, 实际上是 UnsafeHashedRelation, 内部通过 BytesToBytesMap 实现) 中. 并将建立好的 Relation 以 Broadcast 的形式保存到 BlockManager 中. 当executor中运行的 task 需要使用到该广播值时(也就是 Relation) 再通过 BlockManager 从 driver 端获得并保存到 executor 端.

第二步. BroadcastHashJoinExec 在executor 端获得 buildPlan 的Relation, 然后依次遍历所有的 streamPlan， 并算出 join key 的hash值，并从 Relation中查找 match. 然后根据不同的 join type, 进行不同的 join.

BroadcastHashJoinExec 中 requiredChildDistribution 定义如下,

``` scala
  override def requiredChildDistribution: Seq[Distribution] = {
    val mode = HashedRelationBroadcastMode(buildBoundKeys, isNullAwareAntiJoin)
    buildSide match {
      case BuildLeft =>
        BroadcastDistribution(mode) :: UnspecifiedDistribution :: Nil
      case BuildRight =>
        UnspecifiedDistribution :: BroadcastDistribution(mode) :: Nil
    }
  }
```

即 BroadcastHashJoinExec 要求 children 提供 BroadcastDistribution, 如果 children 不满足数据分布，在 EnsureRequirements 中会在 BroadcastHashJoinExec 与 children 之间插入一个 `BroadcastExchangeExec`, BroadcastExchangeExec 的功能即先在 driver 端 collect 小表并建立 HashRelation, 最后进行广播.

可以看出 BroadcastHashJoinExec 自己不会引入任何的 shuffle. 但是考虑到广播需要将 relation 从 driver 端发送到 executor 端，那 relation 不能太大，否则会引起 performance issue. Spark 默认是广播 10M (可由 spark.sql.autoBroadcastJoinThreshold 配置) 以下的小表. 大于 10M 的话, spark 将不会采用 BroadcastHashJoinExec 来实现 Join了.

### SortMergeJoinExec

