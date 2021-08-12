---
layout: page
title: BlockManager Overview
nav_order: 5
parent: BlockManager
grand_parent: Spark
---

# BlockManager
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
// 下面提交的 Job 没有实质用处, 仅仅是用于确定 Executor 已经注册成功
sc.parallelize(Seq(1, 2, 3), 1).collect() // Ensure executor up

// 往 BlockManager 写入 Bean(10)
sc.env.blockManager.putSingle(blockId, Bean(10), StorageLevel.MEMORY_ONLY_SER)
sc.stop()
```

注意: `putSingle`函数用于向 BM 中写入 JVM 对象, 而该对象一定是可以序列化的, 否则会报错.

## BlockManager OverView

BlockManager 管理着 Driver/Executor 存储的所有 Block, 提供 replication 机制.
内部提供多种存储介质(Memory/Off Heap/Disk)用于满足不同的存储需求.

BlockManager 中常用的数据结构如下

| BlockManagerId| BlockManager 的唯一 ID |
| ---- | --- |
| String executorId_ | 标识 driver/executor, 对于 driver 端, 该值为 "driver", executor端通常为 0, 1 ..|
| String host_ | BlockManager 所在的 Node 的 host地址|
| Strign port_ | port 信息 |
| Option[String] topologyInfo_ | BlockManager 的物理 topo 信息|

### OverView

下面这张流程图基本上可以表示依存一个JVM对象的整个过程,

![overview](/docs/spark/blockmanager/bm/blockmanager-putSingle.svg)

## BlockManager 通信模型

### BlockManagerMasterEndpoint

BlockManager 是 Per-JVM 进程的, 所有 executor 和 driver 都**有且只有一个** BlockManager. 当 Spark App launch 时,
它首先向 Spark Cluster 注册 Application, 并申请创建 Executor, 进而创建并初始化 BlockManager. Spark 会为 Driver
生成一个 SparkContext, 进而创建并初始化 Driver 端的 BlockManager. 如图中 1, 2 所示.

Driver 端在生成 BlockManager 前会先向 RPC 注册名为`BlockManagerMaster.DRIVER_ENDPOINT_NAME` 的 BlockManagerMasterEndpoint,
Executor在初始化 BlockManager 前会向 RPC 中查询 `BlockManagerMaster.DRIVER_ENDPOINT_NAME` 并获得
该 BlockManagerMasterEndpoint 的ref, 也就是远程代理, 这样 Executor 就可以通过该 EndPointRef 向 BlockManagerMasterEndpoint
发送 RPC call了.

|BlockManagerMasterEndpoint| |
| ---- | --- |
| Map[String, Array[String]] executorIdToLocalDirs | BM 的 executor id -> Local Directory, 如 "driver"->"/tmp/blockmgr...."|
| Map[BlockManagerId, BlockManagerInfo] | BlockManagerId -> BlockManagerInfo的映射 |
| Map[String, BlockManagerId] blockManagerIdByExecutor | BM的executor id -> BlockManagerId 之间的映射 |
| JHashMap[BlockId, mutable.HashSet[BlockManagerId]] blockLocations | block 在哪些 BlockManager 中|

| BlockManagerInfo| 注册的 BlockManager 相关信息 |
| ---- | --- |
| BlockManagerId blockManagerId | BlockManagerInfo属于哪个 BlockManagerId |
| Int maxOnHeapMem | max onHeap 内存, 这个值是不固定的, maxHeapMemory - onHeapExecutionMemoryPool.memoryUsed |
| Int maxOffHeapMem | max offHeap 内存, maxOffHeapMemory - offHeapExecutionMemoryPool.memoryUsed |
| RpcEndpointRef storageEndpoint | RPC ref, 通过它可以与BlockManager的BlockManagerMasterEndpoint进行通信 |
| JHashMap[BlockId, BlockStatus] _blocks | 该 BlockManager 中保存的 block 信息. blockId 与其 BlockStatus 之间的映射关系 |
| Long _remainingMem | 该 BlockManager 剩余可用的 memory 大小 |

- register

不管是 Driver 还是 Executor, `BlockManager.initialize` 都会向 Driver 端的 BlockManagerMasterEndpoint 注册 BlockManager.
BlockManagerMasterEndpoint 为注册的 BlockManager 生成一个 BlockManagerInfo, 该 BlockManagerInfo 保存了对应的 BlockManager
的相关信息以及其 RPC ref.

BlockManagerMasterEndpoint 提供的 RPC 除了 register, 还有

- UpdateBlockInfo 更新 Block 信息
- GetLocations 获得 Block 所在的 BlockManager
- GetPeers 获得其它非 driver 的 BlockManager peer
- RemoveRdd/Shuffle ... 从 BlockManager 中删除对应的 Block

### BlockTransferService

如果说 BlockManagerMasterEndpoint 用于更新 BlockManager 信息以及 Block 状态等 meta 信息. 那 BlockTransferService 就用于传输
真正的 Block 数据.

BlockTransferService 是一个 C/S 架构, 目前只有一个实现 NettyBlockTransferService, 即底层使用 Netty 传输. BlockTransferService
用于将 Block 数据上传到 Peer 端的 BlockManager, 也用于向 Peer 端的 BlockManager 获得 Block 数据.

## 存储Block API

- putBytes 将序列化后的字节数组存储到BlockManager中
- putBlockData 底层调用 putBytes
- putBlockDataAsStream 存储的Block数据是 Stream
- putIterator 将整个JVM对象的Iterator存储到BlockManager中
- putSingle 将单个JVM对象存储到BlockManager中

所有的 API 基本原理都差不多，下面以 putSingle 为例进行说明.

**putSingle** API将一个 JVM 对象保存到 BlockManager 中, 它可以以 JVM 对象形式保存到 On Heap 中, 也可以以序列化后的字节数组形式保存到
OFF HEAP或Disk中, 这些取决于 Storage Level.

在继续讲解 putSingle 之间, 先看下几个重要的类

|StorageLevel | Block 的存储级别 |
| --- | --- |
| _useDisk | 是否将 Block 保存到磁盘里 |
| _useMemory | 是否将 Block 保存到 On Heap 中 |
| _useOffHeap | 是否将 Block 保存到 Off Heap 中 |
| _deserialized | 是否将Block 以 JVM 对象存储 |
| _replication | Block 的备份个数 |

|BlockInfo| Block的信息 |
| ---- | --- |
|StorageLevel level| Block 在 Storage 存储级别 |
|ClassTag[_] classTag| Block 数据的 ClassTag, 用于选择对应的序列器|
|Boolean tellMaster |是否向 BlockManagerMaster 更新该Block的状态的变化, 对于大多数据 block, 该值为true, 对于 Broadcast blocks, 该值为 false|

|BlockInfoManager| BlockInfo |
| ---- | --- |
| StorageLevel level| Block 在 Storage 存储级别 |
| Map[BlockId, BlockInfo] infos | BlockId 与 BlockInfo 的 map 关系|
| HashMap[TaskAttemptId, mutable.Set[BlockId]] writeLocksByTask | 跟踪每个任务为写入而锁定的Blocks|
| HashMap[TaskAttemptId, ConcurrentHashMultiset[BlockId]] readLocksByTask | 跟踪每个任务为读取而锁定的Blocks |

| BlockStatus | Block在 storage 中的状态 |
| --- | --- |
| StorageLevel level | Block 的storage级别|
| Long memSize | Block 在 MemoryStore 中的大小 |
| Long diskSize | Block 在 DiskStore 中的大小 |

putSingle 功能很简单

1. 为待保存的 Block 数据生成一个 BlockInfo,
2. 接着根据 Block 的 StorageLevel 将 Block 保存到对应的 Memory/Disk 中并获得 BlockStatus.

   如果 Block Storage Level 为 onMem 和 onDisk, 那也会先尝试将该 Block 先保存到 Memory 当中, 如果 Memory 没有足够内存，此时才将 Block 保存到磁盘中.

3. 然后向 BlockManagerMaster 上报 BlockStatus
4. 最后根据 StorageLevel 中的 replication, 再将 Block 备份到其它非 driver的BlockManager 中.

## BlockManager 的存储介质

![storage](/docs/spark/blockmanager/bm/blockmanager-storage.svg)

BlockManager 存储的 Block 主要是在 Memory 或 Disk 中. Memory 又分为 OnHeap 和 OffHeap 两种.

### MemoryStore

BlockManager 将 block 保存到 Memory 当中, Memory 进一步又可以分为 OnHeap 和 OffHeap 两种模式.

|MemoryStore| |
| ---- | --- |
| HashMap[Long, Long] onHeapUnrollMemoryMap | taskAttemptId 与 task 所 unroll 的total block size之间的映射, onHeap Roll既可以保存序列化也可以保存非序列化的数据|
| HashMap[Long, Long] offHeapUnrollMemoryMap | taskAttemptId 与 task 所 unroll 的total block size之间的映射, offHeap 只能保存序列化的数据|
| LinkedHashMap[BlockId, MemoryEntry[_]] entries | BlockId 与它在 MemoryStore 中的储存形式的映射 |
| Long unrollMemoryThreshold | 在 unroll Block之前需要申请的初始化大小, 由 spark.storage.unrollMemoryThreshold 控制, 默认为1M|

**MemoryEntry** 的子类有如下几种

|DeserializedMemoryEntry|以 JVM 对象进行储存, 只能储存在 OnHeap 中|
| ---- | ----|
| value: Array[T]| JVM 对象数组 |
| size: Long | 在 OnHeap 中的大小|
| classTag: ClassTag[T] | T 的 ClassTag, 用于获得序列器|
| memoryMode = ON_HEAP||

|SerializedMemoryEntry|以字节形式进行储存, 可储存在 OnHeap/OffHeap 中|
| ---- | ----|
| buffer: ChunkedByteBuffer |
| size: Long | 在Memory中的大小, buffer.size|
| classTag: ClassTag[T] | T 的 ClassTag, 用于获得序列器|
| memoryMode: MemoryMode | 可以 ON HEAP或 OFF HEAP|

Memory的两种模式又可以进一步细分为 Execution memory与 storage memory, 它们分用于不同的目的. execution memory 通常是指那些涉及到**计算操作**的 Memory, 
如 shuffles/joins/sorts/aggregations. 而 storage memory 通常指 caching 和跨集群的内部数据的传输的 memory.

#### UnifiedMemoryManager

**UnifiedMemoryManager** 管理着 Execution 和 Storage Memory, 并实现一些策略与算法来使得 execution memory 和 storage memory 可以相互共享.
比如从 storage memory 申请内存时, 但此时 storage 可用内存不足以 unroll 这个 Block, 那这时可以向 execution memory 中借一部份内存给
storage memory. storage memory 的值相应增加, 而相应的, execution memory 就会相应的减小.

|UnifiedMemoryManager| |
| ---- | --- |
| onHeapStorageRegionSize | |
| maxHeapMemory | (JVM内存 - 1.5*300M) * `spark.memory.fraction`, 包括 onHeapStorageMemory + onHeapExecutionMemory |
| onHeapStorageMemory | onHeap storage memory 大小|
| onHeapExecutionMemory | onHeap Execution memory 大小 `= maxHeapMemory - onHeapStorageMemory` |
| StorageMemoryPool onHeapStorageMemoryPool | on heap storage bookmarking |
| StorageMemoryPool offHeapStorageMemoryPool | off heap storage bookmarking |
| ExecutionMemoryPool onHeapExecutionMemoryPool | 实现 task 之间的 on heap storage分享的策略与 bookmarking |
| ExecutionMemoryPool offHeapExecutionMemoryPool | 实现 task 之间的 off heap storage分享的策略与 bookmarking |
|maxOffHeapMemory|$(spark.memory.offHeap.size) 最大的 Off Heap使用内存|
|offHeapStorageMemory|maxOffHeapMemory*$(spark.memory.storageFraction) Off Heap storage memory|

UnifiedMemoryManager 在初始化时, 会指定 maxHeapMemory 与 onHeapStorageRegionSize, 其中

``` console
maxHeapMemory = (max_memory_of_JVM - 1.5*300M) * $(spark.memory.fraction)
其中 spark.memory.fraction 默认为 0.6
而 max_memory_of_JVM:
对于 driver 来说, 它值由 --driver-memory 或 spark.driver.memory 决定
对于 executor 来说, 它值由 --executor-memory 或 spark.executor.memory 决定


onHeapStorageRegionSize = maxMemory * $(spark.memory.storageFraction), 
其中spark.memory.storageFraction默认为 0.5

onHeapStorageRegionSize 表示初始化的 on Heap Storage pool size, 
但它并非一直不变, 它可以向Execution memory 借入或借出 memory, 反之亦然.
```

#### 从 Storage memory 中申请内存

![acquire-mem-from-storage](/docs/spark/blockmanager/bm/blockmanager-memoryStore-storage.svg)

上图是 putIteratorAsValues 的流程图, MemoryStorage 中的 `putIteratorAsValues/putIteratorAsBytes/putBytes` 都是
将 Block 存储在 Storage memory中.

从 Storage mem中申请内存时, 如果 Storage mem中可用内存小于 Block 大小, 此时 Storage mem 需要向 Execution mem 借
内存, 如果最后可用内存可以存储下 Block, 那直接 reserve 掉该内存. 如果最后的内存依然不足以存放下该 Block, 此时就需要
从 LRU 缓存中 drop 掉老的 Block, 如果老的 Block是可以落盘, 则落盘, 否则直接从 memory 移出掉.

### DiskStore

DiskStore 用于将 Block 存储在 Disk 中. 每个 Block 以一个文件形式保存到磁盘上

|DiskStore||
| ---- | --- |
|ConcurrentHashMap[BlockId, Long] blockSizes |BlockID -> Block Size 之间的 map|

**DiskBlockManager** 用于维护 block 与 block在磁盘上文件的映射关系, 每个 block 映射到磁盘上的一个文件, 文件名为 BlockId name.

| DiskBlockManager | |
| ---- | ---|
|Boolean deleteFilesOnStop | 是否在 DiskBlockManager 停掉的时候删除已经落盘的数据 |
|Long subDirsPerLocalDir| ${spark.diskStore.subDirectories}, 默认64, 表示在 ${spark.local.dir} 下文件夹的个数|
|Array[File] localDirs| | 非Yarn环境 SPARK_EXECUTOR_DIRS/SPARK_LOCAL_DIRS/${spark.local.dir} 指定的地方 |

BlockId 如何映射到磁盘文件 ?

``` java
val file = diskManager.getFile(blockId) // 对 BlockId.name算 hash
val dirId = hash % localDirs.length // 获得 local Dirs
val subDirId = (hash / localDirs.length) % subDirsPerLocalDir // 获得 sub dir

new File(subDir, blockId.name) //最后以 blockId name 生成最后的文件
```
