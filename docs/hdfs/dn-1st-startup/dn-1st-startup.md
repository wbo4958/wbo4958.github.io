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

## DataNode side

- **Step 1: refresh NameNodes**

一个HDFS集群可以同时配置多个独立的 NameSpace, 而同一个 NameSpace 又可以配置多个 NameNode (如 ACTIVE NN, STANDBY NN等), 参见 [HDFS Federation](https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-hdfs/Federation.html).

DataNode 通过调用 BlockPoolManager.refreshNameNodes register/refresh NameNode

BlockPoolManager||
-----|----
Map<String, BPOfferService> bpByNameserviceId | namespaceID 与 BPOfferService 的映射|
Map<String, BPOfferService> bpByBlockPoolId|blockpoolID  与 BPOfferService 映射|

可以看出一个 namespace 对应一个 BPOfferService.

refreshNameNodes 函数首先获得 namespaceID 与 其对应的 所有的 NN 地址. 然后创建相关的 BPOfferService.

- **Step 2: 创建 BPOfferService**

BPOfferService 是一个用来管理 BlockPool与 namespace 联系的的类， 包括 register/heartbeat. 

BPOfferService | |
----| ----
NamespaceInfo bpNSInfo | NN 的 NameSpace 信息
DatanodeRegistration bpRegistration | Block pool 的注册信息
String nameserviceId | NN 的 nameserviceID
String bpId| block pool id
List<BPServiceActor> bpServices| 所有的 BPServiceActor

BPServiceActor 用于管理 BlockPool 与某个 NameNode 联系的类

BPServiceActor ||
--- | ----
InetSocketAddress nnAddr | NN 的地址
String serviceId | nameservice ID
String nnId|NN id
BPServiceActor bpServiceToActive | 哪个 BPServiceActor 与 Active NameNode 进行的连接
RunningState runningState |运行状态, CONNECTING, INIT_FAILED, RUNNING, EXITED, FAILED
DatanodeRegistration bpRegistration | DN 注册信息
DatanodeProtocolClientSideTranslatorPB bpNamenode | NN server的proxy

BPServiceActor 会开启两个线程， 一个线程与 NN 通信， 另一个线程处理从 NN 返回来的命令.

- **Step 3 - 6: connect to NameNode**

  当 BPOfferService与相应的 BPServiceActor 创建好后，调用 startAll, BPServiceActor 开始工作.

  - 创建 DatanodeProtocolClientSideTranslatorPB 用于与 NN 进行 rpc
  - 调用 versionRequest 从 NN 获得 NamespaceInfo 信息
  - 验证并设置 NamespaceInfo

- **Step 7: 初始化 BlockPool**

  BlockPool 已经与 NN handshake 了，通知 DataNode 初始化 BlockPool.

- **Step 8: register Datanode**
- **Step 9: BPServiceActor offer service**
- **Step 10: 往 NN 发送 heartbeat**
- **Step 11: blockReport**
- **Step 12: BPServiceActor 处理命令**