---
layout: page
title: DataNode 第一次启动
nav_order: 15
parent: HDFS
---

# DataNode 第一次启动
{: .no_toc }

TODO. 本文基于 hadoop/branch-3.3.0

## 目录
{: .no_toc .text-delta}

1. TOC
{:toc}

## overview

DataNode 的 Storage location 由 `dfs.datanode.data.dir` 指定， 如果没有设置，则默认为 `${hadoop.tmp.dir}/dfs/data`

生成 StorageLocationChecker 在 DataNode 启动阶段检查 DN 的 storage location. 如果 storage location 不存在， 那创建该 dir, 然后设置 dir 权限为 `dfs.datanode.data.dir.perm` 默认为 `700`

创建 DataNode 以及相关的类，最后 startDataNode 启动相关的服务， 如 HttpServer, DataXceiverServer RPC 等.


DataNode||
----|----
clusterId ||



BlockPoolManager||
-----|----
Map<String, BPOfferService> bpByNameserviceId | name service ID 与 BPOfferService 的映射|
Map<String, BPOfferService> bpByBlockPoolId|blockpool ID  与 BPOfferService 映射|