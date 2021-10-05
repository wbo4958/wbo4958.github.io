---
layout: page
title: Shuffle 实现
nav_order: 9 
parent: Spark 
---

# Spark Shuffle
{: .no_toc}

本文通过代码学习 Spark 中 window 函数的实现. 本文基于 Spark 3.2.0

``` scala
val df = Seq(1, 2, 3, 4, 5, 6).toDS()
val df1 = df.repartition(2)
df1.show()
```

## 目录
{: .no_toc .text-delta}

1. TOC
{:toc}

## HashAggregateExec

基于 `group by key` 的 hash值 查找 agg buffer.

## ObjectHashAggregateExec


## FastHashMap vs Regular HashMap

FastHashMap 是一个固定数量的 HashMap, 内部采用 `int[(1<<16) * 2] buckets` 来记录 `row number`, 这个 `row number` 是 `(group by key, agg buffer)` 保存在 RowBasedKeyValueBatch 中的位置. 即 `group by key` 的hash值先从 buckets 中索引到 `row number`，然后再根据 `row number` 在 RowBasedKeyValueBatch 找到 `(group by key, agg buffer)`

之所以称为 Fast HashMap 是因为它是固定大小，即最多保存 `(1<<16)*2` 个 `group by key`. 且当存在碰撞冲突时，只往下尝试2次，如果没有找到，则直接返回，不会增加 size

而 Regular HashMap 当初始化分配的 size 不够存储时，此时会 grow size 并且 rehash, 这是一个比较 pool performance 的地方
