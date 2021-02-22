---
layout: page
title: NameNode 1st startup
nav_order: 10
parent: HDFS
---

本文通过学习 NameNode 第一次启动流程来了解 NameNode 中使用到的 rpc, 线程结构以及 NameNode 相关的类. 本文基于 hadoop/branch-3.3.0

## NameNode startup overview

启动 NameNode 的命令行:

``` bash
hdfs --daemon start namenode
```

![1st-startup](/docs/hdfs/nn-1st-startup/hdfs-NN-1st-startup.svg)

## loadNameSpace

### readAndInspectDirs

依次遍历所有的 NameNode 目录，读取 VERSION 文件获得 layout version. 然后根据 layout version 创建 FSImageStorageInspector. 当前 layout version 是支持 `Feature.TXID_BASED_LAYOUT`, 创建的 FSImageStorageInspector 为 FSImageTransactionalStorageInspector.

FSImageTransactionalStorageInspector 主要作用是

1. 读取 seen_txid 文件，获得最后一次 transaction id
2. 遍历 NN 目录下所有的文件，查找 fsimage 和 fsimage_rollback 文件
3. getLatestImages 根据 image 的 txid 获得最新的 image 文件

### initEditLog

1. 创建 JournalSet - 用于管理所有的 Journal
2. recoverUnfinalizedSegments  查找 edit 和 edits_inprogress 相关的文件. 然后将 edits_inprogress 重命名为 edit 文件. 第一次启动并没有任何 edit 文件.

### loadFSImageFile

加载最新的 image file

1. 读取 VERSION 文件， 获得并设置 namespaceID/clusterID/storageType/blockpoolID 等
2. 开一个新的线程 DigestThread 来计算 fsimage 文件的 md5 值
3. loadSummary 从 fsimage 获得每个 section 的 offset 和 size

### loadEdits

第一次启动， NameNode 里并没有 edits.

## createRpcServer

![rpc](/docs/hdfs/nn-1st-startup/hdfs-NN-rpc.svg)

NameNodeRpcServer 实现了 NameNode 所需要的 RPC 协议. 如

RPC 对象 |  RPC 协议
----|-----
client | ClientProtocol
datanode | DatanodeProtocol, DatanodeLifelineProtocol
secondary namenode | NamenodeProtocol

其中每一种具体的协议接口都有一个对应的实现类， 如 ClientNamenodeProtocolServerSideTranslatorPB -> DatanodeProtocolPB, 当生成 ClientNamenodeProtocolServerSideTranslatorPB 对象时， 传入了DatanodeProtocol的真正的实现也就是 NameNodeRpcServer. 所有的 API 最后都会回调 NameNodeRpcServer.

NameNodeRpcServer 当收到 API 请求时， NameNodeRpcServer 利用 NameNode, FsNameSpace等完成相关的功能.

``` java
  public DatanodeProtocolServerSideTranslatorPB(DatanodeProtocol impl, //impl -> NameNodeRpcServer
      int maxDataLength) {
    this.impl = impl;
    this.maxDataLength = maxDataLength;
  }
```

1. NameNodeRpcServer 通过 `fs.defaultFS` 获得 NameNode server 地址.

   然后根据 `dfs.namenode.rpc-bind-host` 获得绑定的 host, 如果该值没有设置，则默认是 hostname. 即如果 **fs.defaultFS=hdfs://192.168.3.12:9000**, 如果 dfs.namenode.rpc-bind-host 没有设置， 那绑定的 host 为 192.168.3.12 的 hostname 而非 ip 地址.

2. 创建所有 NameNode 协议的实现类, 并加入到 RPC server 中.

## startCommonServices

### 调用NameSystem的startCommonServices 启动必要的服务线程

- checkAvailableResources
  
  FSNameSystem 创建 NameNodeResourceChecker 检查 edits 目录所在的 volume是否有可用空间

  - 启动一个进程执行 **df -k -P ~/bigdata/hadoop/dfs/name** 获得 **volume=/dev/nvme0n1p2**
  
  ``` console
  $ df -k -P ~/bigdata/hadoop/dfs/name
  Filesystem     1024-blocks      Used Available Capacity Mounted on
  /dev/nvme0n1p2   490691512 312122100 153573932      68% /
  ```

  - 检查 volume 的剩余可用空间是否大于 **dfs.namenode.resource.du.reserved**的设定值默认 100M
  
- BlockManager.active
  
  激活 BlockManager, 也就是开启一系列服务线程, 如图中的绿色的框

  - BlockManagerSafeMode.active
  
  最后初始化 safemode 相关的信息. 通过 BlocksMap 获得当前的 block 数量并减去 LeaseManager 中当前正在构建的 Block 数量. 当 **blocksafe > blockThreshold** 时，进入 safemode.

  BlockManagerSafeMode|
  -----|-----
  blockTotal | total blocks - under construction blocks
  blockThreshold | safemode threshold, 由**dfs.namenode.safemode.threshold-pct**指定，默认为 blockTotal*0.999
  blockSafe | safe block 的数量

### start rpc

NameNode rpc 开始工作

## startActiveServices

NameNode在初始化之间会创建一个 HAState, 没有开启 HA 模式时， 默认会创建 ActiveState. 在 NameNode 初始化后， NameNode 进入 Active Stage, 并启动一些仅当 Active Stage 时才能开启的服务线程. 如,

service |
----- | -----
NameNodeResourceMonitor | 检查 NN 可用资源， 必要时进入 safe mode
NameNodeEditLogRoller | roll edit log, 关闭当前 edit log, 创建一个新的 edit log
