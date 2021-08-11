---
layout: page
title: BlockManager Overview
nav_order: 5
parent: BlockManager
grand_parent: Spark
---

# array
{: .no_toc}

本文通过在 Driver 端往 BlockManager 中写入一个Block的代码来学习 BlockManager 的原理. 本文基于 Spark 3.1.1.

## 目录
{: .no_toc .text-delta}

1. TOC
{:toc}

## 示例代码

``` scala
case class Bean(x: Int)


val sc = SparkContext.getOrCreate(conf)
val blockId = TestBlockId("Bean")
// 下面提交的 Job 没有实质用处，仅仅是用于确定 Executor 已经注册成功
sc.parallelize(Seq(1,2,3), 1).collect() // Ensure executor up

// 往 BlockManager 写入 Bean(10)
sc.env.blockManager.putSingle(blockId, Bean(10), StorageLevel.MEMORY_ONLY_SER)
sc.stop()
```

注意: `putSingle`函数用于向 BM 中写入 JVM 对象, 而该对象一定是可以序列化的，否则会报错.

## BlockManager OverView

BlockManager 管理着 Driver/Executor 存储的所有 Block, 提供 replication 机制.
内部提供多种存储介质(Memory/Off Heap/Disk)用于满足不同的存储需求.

BlockManager 中常用的数据结构如下

| BlockManagerId| BlockManager 的唯一 ID |
| ----    | --- |
| String executorId_ | 标识 driver/executor, 对于 driver 端，该值为 "driver", executor端通常为 0, 1 ..|
| String host_ | BlockManager 所在的 Node 的 host地址|
| Strign port_ | port 信息 |
| Option[String] topologyInfo_ | BlockManager 的物理 topo 信息|

### OverView

下面这张流程图基本上可以表示 putSingle 的整个过程,

![overview](/docs/spark/blockmanager/bm/blockmanager-putSingle.svg)

## BlockManager 通信模型

### BlockManagerMasterEndpoint

BlockManager 是 Per-JVM 进程的， 所有 executor 和 driver 都**有且只有一个** BlockManager. 当 Spark App launch 时，
它首先向 Spark Cluster 注册 Application, 并申请创建 Executor, 进而创建并初始化 BlockManager. Spark 会为 Driver
生成一个 SparkContext, 进而创建并初始化 Driver 端的 BlockManager. 如图中 1, 2 所示.

Driver 端在生成 BlockManager 前会先向 RPC 注册名为`BlockManagerMaster.DRIVER_ENDPOINT_NAME` 的 BlockManagerMasterEndpoint,
Executor在初始化 BlockManager 前会向 RPC 中查询 `BlockManagerMaster.DRIVER_ENDPOINT_NAME` 并获得
该 BlockManagerMasterEndpoint 的ref, 也就是远程代理， 这样 Executor 就可以通过该 EndPointRef 向 BlockManagerMasterEndpoint
发送 RPC call了.

|BlockManagerMasterEndpoint|     |
| ----    | --- |
| Map[String, Array[String]] executorIdToLocalDirs | BM 的 executor id -> Local Directory, 如 "driver"->"/tmp/blockmgr...."|
| Map[BlockManagerId, BlockManagerInfo] | BlockManagerId -> BlockManagerInfo的映射 |
| Map[String, BlockManagerId] blockManagerIdByExecutor | BM的executor id -> BlockManagerId 之间的映射 |
| JHashMap[BlockId, mutable.HashSet[BlockManagerId]]　blockLocations | block 在哪些 BlockManager 中|

| BlockManagerInfo| 注册的 BlockManager 相关信息 |
| ----    | --- |
| BlockManagerId blockManagerId | BlockManagerInfo属于哪个 BlockManagerId |
| Int maxOnHeapMem | max onHeap 内存, 这个值是不固定的， maxHeapMemory - onHeapExecutionMemoryPool.memoryUsed |
| Int maxOffHeapMem | max offHeap 内存,  maxOffHeapMemory - offHeapExecutionMemoryPool.memoryUsed |
| RpcEndpointRef storageEndpoint | RPC ref, 通过它可以与BlockManager的BlockManagerMasterEndpoint进行通信 |
| JHashMap[BlockId, BlockStatus] _blocks | 该 BlockManager 中保存的 block 信息. blockId 与其 BlockStatus 之间的映射关系 |
| Long _remainingMem | 该 BlockManager 剩余可用的 memory 大小　|

- register

不管是 Driver 还是　Executor, `BlockManager.initialize` 都会向 Driver 端的 BlockManagerMasterEndpoint 注册 BlockManager.
BlockManagerMasterEndpoint 为注册的 BlockManager 生成一个 BlockManagerInfo, 该 BlockManagerInfo 保存了对应的 BlockManager
的相关信息以及其 RPC ref.

BlockManagerMasterEndpoint　提供的 RPC 除了　register, 还有

- UpdateBlockInfo 更新 Block 信息
- GetLocations 获得 Block 所在的 BlockManager
- GetPeers 获得其它非 driver 的 BlockManager peer
- RemoveRdd/Shuffle ... 从 BlockManager 中删除对应的 Block

### BlockTransferService

如果说 BlockManagerMasterEndpoint　用于更新 BlockManager 信息以及 Block 状态等 meta 信息. 那 BlockTransferService 就用于传输
真正的 Block 数据.

BlockTransferService 是一个 C/S 架构，目前只有一个实现 NettyBlockTransferService, 即底层使用 Netty 传输. BlockTransferService
用于将 Block 数据上传到　Peer 端的 BlockManager, 也用于向 Peer 端的 BlockManager 获得 Block 数据.

## putSingle

**putSingle** API将一个 JVM 对象保存到 BlockManager 中, 它可以以 JVM 对象形式保存到 On Heap 中, 也可以以序列化后的字节数组形式保存到
OFF HEAP或Disk中,　这些取决于 Storage Level.

在继续讲解 putSingle 之间，先看下几个重要的类

|StorageLevel | Block 的存储级别 |
| --- | --- |
| _useDisk | 是否将 Block 保存到磁盘里 |
| _useMemory | 是否将 Block 保存到 On Heap 中 |
| _useOffHeap | 是否将 Block 保存到 Off Heap 中 |
| _deserialized | 是否将Block 以 JVM 对象存储 |
| _replication | Block 的备份个数 |

|BlockInfo| Block的信息   |
| ----    | --- |
|StorageLevel level| Block 在 Storage 存储级别 |
|ClassTag[_] classTag| Block 数据的 ClassTag, 用于选择对应的序列器|
|Boolean tellMaster |是否向 BlockManagerMaster 更新该Block的状态的变化, 对于大多数据　block, 该值为true, 对于 Broadcast blocks, 该值为 false|

|BlockInfoManager| BlockInfo |
| ----    | --- |
| StorageLevel level| Block 在 Storage 存储级别 |
| Map[BlockId, BlockInfo] infos | BlockId 与 BlockInfo 的 map 关系|
| HashMap[TaskAttemptId, mutable.Set[BlockId]] writeLocksByTask | 跟踪每个任务为写入而锁定的Blocks|
| HashMap[TaskAttemptId, ConcurrentHashMultiset[BlockId]] readLocksByTask | 跟踪每个任务为读取而锁定的Blocks |

| BlockStatus | Block在 storage 中的状态 |
| --- | --- |
| StorageLevel level | Block 的storage级别|
| Long memSize | Block 在 MemoryStore 中的大小 |
| Long diskSize | Block 在 DiskStore 中的大小 |

putSingle 作用很简单，为待保存的 Block 数据生成一个 BlockInfo, 然后 Block 的 StorageLevel 将 Block 保存到对应的 Memory 或都 Disk 中并获得 BlockStatus.
然后向 BlockManagerMaster 上报 BlockStatus, 最后根据 StorageLevel 中的 replication, 再将 Block 备份到非 driver 的其它几个 BlockManager 中.

## Block 保存的 Storage

## MemoryStore

## DiskStore

