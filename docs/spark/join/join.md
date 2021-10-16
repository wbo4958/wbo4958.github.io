---
layout: page
title: Aggregation
nav_order: 13 
parent: Spark 
---

# Spark Aggregation
{: .no_toc}

本文通过代码学习 Spark 中 Aggregation 的实现. 本文基于 Spark 3.2.0

## 目录
{: .no_toc .text-delta}

1. TOC
{:toc}

## 测试代码

## xxx

| Join | |
| ---- | --- |
| left: LogicalPlan | |
| right: LogicalPlan | |
| joinType | InnerLike(Cross/Inner) / LeftSemi / RightOuter / NaturalJoin /LeftOuter / FullOuter / LeftAnti |
| condition: Option[Expression] | Join条件|
| hint: JoinHint | Join hint |


## Join

- build side

### BroadcastHashJoinExec

BroadcastHashJoinExec 术语

- BuildSide

  有 BuildLeft/BuildRight 之分, BuildSide 表示先 build(执行) 哪边的 plan, 一般来说是 build 小表, 然后将该小表 广播到 executor 中.

- buildPlan
  
  buildPlan 是需要事先 build 的 plan. Driver端事先异步执行 `buildPlan.doExecuteBroadcast`, 获得 buildPlan 的结果，并广播到 executor.

- streamPlan

  streamPlan 是 Join 实现时，依次匹配的表.


