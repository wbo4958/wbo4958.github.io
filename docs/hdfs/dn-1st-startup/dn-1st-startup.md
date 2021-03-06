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
  
  创建 DatanodeRegistration 类,该类包含了 NN 需要的所有的注册信息.

  DatanodeRegistration | |
  -----|-----
  ipAddr/hostName| NN的ip与host信息
  xferPort/xferAddr| data streaming 信息
  infoPort| info server 的商品
  ipcPort| ipc端口
  datanodeUuid| datanode uuid
  storageInfo|BlockPoolSliceStorage 信息

- **Step 9 - Step 11: BPServiceActor offerService**
  
  offerService 进入死循环, 主要做以下事情  
  
  1. 向 NN 发送心跳

      发送心跳的间隔由 dfs.heartbeat.interval控制, 默认为 3s. 心跳的 workloads 包括每个 Volume 上的 StorageReport
      StorageReport ||
      ---|---
      failed|
      capacity| volume 的 capacity
      dfsUsed| volume 中所有的 BlockPoolSlice 使用之和
      nonDfsUsed| 非 dfs 使用的空间, 包括 reserve 的空间
      remaining|剩下可用空间
      blockPoolUsed| 当前 BlockPoolSlice使用的空间

      以及 VolumeFailureSummary/SlowPeerReports/SlowDiskReports 相关信息.

      最后还包括一些 cache capacity以及 cache used 信息

  2. 向 NN 发送 IBRs
  3. 向 NN 发送 block 信息

- **Step 12: BPServiceActor 处理命令**
  
  当 DN 往 NN 进行 RPC 后，返回 DatanodeCommand, BPServiceActor 将这些命令丢给 CommandProcessingThread 接着处理.