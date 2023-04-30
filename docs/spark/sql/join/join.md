---
layout: page
title: Join
nav_order: 13 
parent: Spark 
---

# Spark Join
{: .no_toc}

本文通过代码学习 Spark 中 Join 的几种不同的实现. 本文基于 Spark 3.2.1-SNAPSHOT

## 目录
{: .no_toc .text-delta}

1. TOC
{:toc}

## 测试代码

``` scala
import spark.sqlContext.implicits._

val leftDf = Seq (
  (3, "history"),
  (2, "math"),
  (5, "history"),
  (4, "math")).toDF("std_id", "dept_name")

val rightDf = Seq (
  ("piano", 3),
  ("math", 1),
  ("guitar", 3),
  ("math", 7)).toDF("dept_name", "std_id")

// 改变 hint 可以切换到不同的实现, 如 broadcast/merge/shuffle_hash/shuffle_replicate_nl
val df = leftDf.join(rightDf.hint("broadcast"), leftDf("std_id").equalT(rightDf("std_id")), "inner")
df.explain(true)
df.show()
```

输出结果:

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

## Join LogicalPlan

| Join | |
| ---- | --- |
| left: LogicalPlan | 参与 Join 的 left LogicalPlan |
| right: LogicalPlan | 参与 Join 的 right LogicalPlan |
| joinType | InnerLike(Cross/Inner) / LeftSemi / RightOuter / NaturalJoin /LeftOuter / FullOuter / LeftAnti |
| condition: Option[Expression] | Join条件|
| hint: JoinHint | Join hint |

Join LogicalPlan 包含上述几个内容， 参与 join 的 plans, join条件是什么, join类型以及 join Hint.

## Join

Join 是一个算子， Spark对 Join 算子有多种实现, 如 BHJ/SMJ/SHJ ... 这些实现最后生成的 Join 结果是一致的, 但是不同的实现应用的场景或许不一样，因此不同的实现带来的 performance 也不一样.

![join plan](/docs/spark/join/join-plan-join.svg)

简化成如下这张表

![join plan condition](/docs/spark/join/join-plan-condition.svg)

Join 中几个比较常见的术语

- BuildSide

  有 BuildLeft/BuildRight 之分, BuildSide 表示先 **build(执行)** 哪边的 plan, 一般来说是 build 小表, 然后将该小表 广播到 executor 中.

- buildPlan
  
  buildPlan 是需要事先 build 的 plan. Driver端事先异步执行 `buildPlan.doExecuteBroadcast`, 获得 buildPlan 的结果，并保存到 BlockManager 中， executor 端使用到broadcast,再从 driver 端获得该广播值.

- streamPlan

  BroadcastHashJoinExec 进行 join 时，依次遍历 streamPlan 的行，然后在 buildPlan 中通过 joinKey 的 hash 值找到 match 时行 join.

### BroadcastHashJoinExec

``` console
== Physical Plan ==
*(1) BroadcastHashJoin [std_id#7], [std_id#19], Inner, BuildRight, false
:- *(1) LocalTableScan [std_id#7, dept_name#8]
+- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[1, int, false] as bigint)),false), [id=#11]
   +- LocalTableScan [dept_name#18, std_id#19]
```

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

``` console
== Physical Plan ==
*(3) SortMergeJoin [std_id#7], [std_id#19], Inner
:- *(1) Sort [std_id#7 ASC NULLS FIRST], false, 0
:  +- Exchange hashpartitioning(std_id#7, 200), ENSURE_REQUIREMENTS, [id=#12]
:     +- LocalTableScan [std_id#7, dept_name#8]
+- *(2) Sort [std_id#19 ASC NULLS FIRST], false, 0
   +- Exchange hashpartitioning(std_id#19, 200), ENSURE_REQUIREMENTS, [id=#13]
      +- LocalTableScan [dept_name#18, std_id#19]
```

![smj exec](/docs/spark/join/join-smj-exec.svg)

``` scala
  override def requiredChildDistribution: Seq[Distribution] = {
    if (isSkewJoin) { // skew join, 默认是 false.
      // We re-arrange the shuffle partitions to deal with skew join, and the new children
      // partitioning doesn't satisfy `HashClusteredDistribution`.
      UnspecifiedDistribution :: UnspecifiedDistribution :: Nil
    } else {
      HashClusteredDistribution(leftKeys) :: HashClusteredDistribution(rightKeys) :: Nil
    }
  }
```

由SortMergeJoinExec 所定义的 requiredChildDistribution 可知, SortMergeJoinExec 要求 left 和 right child 同时具有 HashCluster 分布. 即如果 child 的数据分布不满足需求. Spark EnsureRequirements 会在 SortMergeJoinExec 与 child 之间插入一个 ShuffleExchangeExec.

``` scala
  override def requiredChildOrdering: Seq[Seq[SortOrder]] =
    requiredOrders(leftKeys) :: requiredOrders(rightKeys) :: Nil

  private def requiredOrders(keys: Seq[Expression]): Seq[SortOrder] = {
    // This must be ascending in order to agree with the `keyOrdering` defined in `doExecute()`.
    keys.map(SortOrder(_, Ascending))
  }
```

同理要求 child 的数据是按 join key 排序好的, 如果不满足， EnsureRequirements 会在 SortMergeJoinExec 与 child 之间插入一个 SortExec.

因为 SMJ 要求 left/right 排序好了，且 left/right 具有相同的数据分布，因此同一个 reducer task 都会获得具有相同的 join key 的 left/right 数据. 考虑到 left/right 是排序好的，因此只需要同时往下移动 left/right 的指针 (指向不同的行) 进行比较, 就可以很简单的实现不同的 join 类型. 如上图所示.

### ShuffledHashJoinExec

``` console
== Physical Plan ==
*(1) ShuffledHashJoin [dept_name#8], [dept_name#18], Inner, BuildRight
:- Exchange hashpartitioning(dept_name#8, 200), ENSURE_REQUIREMENTS, [id=#12]
:  +- LocalTableScan [std_id#7, dept_name#8]
+- Exchange hashpartitioning(dept_name#18, 200), ENSURE_REQUIREMENTS, [id=#13]
   +- LocalTableScan [dept_name#18, std_id#19]
```

![shj](/docs/spark/join/join-shj.svg)

ShuffledHashJoinExec 与 SortMergeJoinExec 都继承同一个 ShuffledJoin, 即它们有相同的 requiredChildDistribution 定义， 也就是当 children 的数据分布不符合要求时，此时需要插入 ShuffleExchangeExec.

但 requiredChildOrdering 不要求 children 排序. 所以不需要插入 SortExec.

``` scala
def requiredChildOrdering: Seq[Seq[SortOrder]] = Seq.fill(children.size)(Nil)
```

ShuffledHashJoin 在每个 Task 中会先将 buildPlan 建立起 HashRelation, 然后依次遍历 steaming Plan, 从 streaming row 中取出 join key, 再从 buildPlan 的 HashRelation 查找是否 match 来进行不同的 join 操作. 如上图所示.

由于 SHJ 需要在每个 Task 中对 buildPlan build HashRelation, 因此 SHJ 需要很大的内存，否则会 spill 到磁盘，引起 performance 问题

### CartesianProductExec

``` console
== Physical Plan ==
CartesianProduct (dept_name#8 = dept_name#18)
:- LocalTableScan [std_id#7, dept_name#8]
+- LocalTableScan [dept_name#18, std_id#19]
```

迪卡尔乘积不会引入任何的 shuffle 和 sort, 它支持 equal join 和 非 equal join. 它将 left child 和 right child 进行迪卡尔乘积， 然后通过 join condition 进行 eval, 最后再将得到的结果再进行合并到一个 buffer byte数组 中. 整个过程如下所示,

![cartesian product](/docs/spark/join/join-CartesianProduct.svg)

### BroadcastNestedLoopJoinExec

``` console
== Physical Plan ==
*(1) BroadcastNestedLoopJoin BuildRight, Inner, (std_id#7 > std_id#19)
:- *(1) LocalTableScan [std_id#7, dept_name#8]
+- BroadcastExchange IdentityBroadcastMode, [id=#11]
   +- LocalTableScan [dept_name#18, std_id#19]
```

![BroadcastNestedLoopJoinExec](/docs/spark/join/join-BroadcastNestedLoopJoin.svg)

BroadcastNestedLoopJoinExec 可以用于 EqualJoin 和 Non-EqualJoin 两种.

BroadcastNestedLoopJoinExec 也是以广播方式实现的 join, 它没有大小的限制，哪怕小表有几十个G，它依然会先将小表 collect 回 driver 端， 然后再广播出去. BroadcastNestedLoopJoinExec 与 BroadcastHashJoinExec 有点不同的是 BroadcastExchangeExec会将 collect 回来的数据建立 HashRelation, 而 BroadcastNestedLoopJoinExec不需要.

虽然 BroadcastNestedLoopJoinExec 没有引入 shuffle, 但是当小表很大时， 非常容易 OOM.

执行 Join 时, 对于每一个 stream plan row, 它会依次遍历 build plan 的所有 row, 判断 join 条件是否满足，如果满足，则 join. 可以看出 BroadcastNestedLoopJoinExec 执行 join 的时间复杂度是 O(n*n).

## Spark 如何 pick join 的实现

在理解了每种 Join 实现后，现在可以看看 Spark 如何挑选 Join 实现的

``` console
对于 Equal join, 首先查看 Join Hint.
If it is an equi-join, we first look at the join hints w.r.t. the following order:
  1. broadcast hint: pick broadcast hash join if the join type is supported. If both sides
     have the broadcast hints, choose the smaller side (based on stats) to broadcast.
  2. sort merge hint: pick sort merge join if join keys are sortable.
  3. shuffle hash hint: We pick shuffle hash join if the join type is supported. If both
     sides have the shuffle hash hints, choose the smaller side (based on stats) as the
     build side.
  4. shuffle replicate NL hint: pick cartesian product if join type is inner like.

If there is no hint or the hints are not applicable, we follow these rules one by one:
  1. Pick broadcast hash join if one side is small enough to broadcast, and the join type
     is supported. If both sides are small, choose the smaller side (based on stats)
     to broadcast.
  2. Pick shuffle hash join if one side is small enough to build local hash map, and is
     much smaller than the other side, and `spark.sql.join.preferSortMergeJoin` is false.
  3. Pick sort merge join if the join keys are sortable.
  4. Pick cartesian product if join type is inner like.
  5. Pick broadcast nested loop join as the final solution. It may OOM but we don't have
     other choice.
```

对于没有 Join Hint 的情况，首先看下能否广播, 广播不需要进行 shuffle, performance 最好.
其次再看下能否使用 ShuffleHashJoin, ShuffleHashJoin 相比于 SortMergeJoin 不需要对数据时行排序.
但是 SHJ 需要有一张表足够小，这样在 build relation 时不引起 spill, performance 会比较好;
但是一旦 spill, 那 performance 将会不理想.
接着就是看能否用 SMJ, 最后才会考虑 CartesianProductExec 和 BroadcastNestedLoopJoinExec.

## Bucket Join

TODO