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
DataSource(如 parquet/orc) 的statistic 来减少数据的加载进而提升 performance. 目前 Aggregate pushdown 只针对 V2 的 DataSource.

本文基于 Spark 3.3.0-SNAPSHOT 学习 Aggregate pushdown 的实现原理.

## 目录
{: .no_toc .text-delta}

1. TOC
{:toc}

## Aggregate pushdown 相关的 rule

