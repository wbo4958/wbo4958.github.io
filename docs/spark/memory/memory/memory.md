---
layout: page
title: 内存管理
nav_order: 8 
parent: Memory
grand_parent: Spark
---

# Spark内存管理 
{: .no_toc}

本文学习 Spark 的内存管理. 本文基于 Spark 3.1.1.

## 目录
{: .no_toc .text-delta}

1. TOC
{:toc}

## MemoryManager

Spark Memory 分为两种, 一种为 execution memory 另一种为storage memory. execution memory 通常是指那些涉及到**计算操作**的 Memory,
如 shuffles/joins/sorts/aggregations. 而 storage memory 通常指 caching 和跨集群的内部数据的传输的 memory.

Spark 通过 MemoryManager 来管理上面所述的两种内存. MemoryManager 是一个抽象类， UnifiedMemoryManager 是其唯一的实现类.
UnifiedMemoryManager 实现一些策略与算法来使得 execution memory 和 storage memory 可以相互共享.
比如从 storage memory 申请内存时, 如果 storage 可用内存不足时, 这时可以向 execution memory 中借一部分内存给
storage memory. storage memory pool 相应增加, 而对应的, execution memory pool 就会相应减小.

|UnifiedMemoryManager| |
| ---- | --- |
| onHeapStorageRegionSize | 初始化的onHeap Storage pool size |
| maxHeapMemory | 最大堆内存,包括 onHeapStorageMemory + onHeapExecutionMemory |
| onHeapStorageMemory | onHeap storage memory 大小|
| onHeapExecutionMemory | onHeap Execution memory 大小 `= maxHeapMemory - onHeapStorageMemory` |
| StorageMemoryPool onHeapStorageMemoryPool | OnHeap storage memory 使用情况bookmarking |
| StorageMemoryPool offHeapStorageMemoryPool | OffHeap storage memory 使用情况bookmarking |
| ExecutionMemoryPool onHeapExecutionMemoryPool | OnHeap execution memory 使用情况bookmarking |
| ExecutionMemoryPool offHeapExecutionMemoryPool | OffHeap execution memory 使用情况bookmarking |
| maxOffHeapMemory|$(spark.memory.offHeap.size) 最大的 Off Heap使用内存|
| offHeapStorageMemory|maxOffHeapMemory*$(spark.memory.storageFraction) Off Heap storage memory|

UnifiedMemoryManager 在初始化时, 会指定 maxHeapMemory 与 onHeapStorageRegionSize, 其中

``` console
maxHeapMemory = (max_memory_of_JVM - 1.5*300M) * $(spark.memory.fraction)
其中 spark.memory.fraction 默认为 0.6
而 max_memory_of_JVM:
对于 driver 来说, 它值由 --driver-memory 或 spark.driver.memory 决定
对于 executor 来说, 它值由 --executor-memory 或 spark.executor.memory 决定

onHeapStorageRegionSize = maxMemory * $(spark.memory.storageFraction),
其中spark.memory.storageFraction默认为 0.5

onHeapStorageRegionSize 表示初始化的 onHeap Storage pool size,
但它并非一直不变, 它可以向Execution memory 借入或借出 memory, 反之亦然.
```

UnifiedMemoryManager 实例是 Per-process 的, 它在 `SparkEnv` 的构造函数中实始化, 也就是 driver/executor 分别
只有一个 UnifiedMemoryManager 的实例.

``` scala
// 初始化 MemoryManager
val memoryManager: MemoryManager = UnifiedMemoryManager(conf, numUsableCores)
```

并且在 BlockManager 的构造函数中设置 MemoryStore. 用于 evict BlockManager 缓存的 Block, 释放内存.

``` scala
private[spark] val memoryStore =
  new MemoryStore(conf, blockInfoManager, serializerManager, memoryManager, this)
memoryManager.setMemoryStore(memoryStore)
```

MemoryManager 提供如下的 API 来对内存分配进行bookmark

- acquireStorageMemory

  从 Storage Mem Pool 中分配内存

  ![acquire-mem-from-storage](/docs/spark/memory/memory-acquire_storage.svg))

- acquireExecutionMemory
  
  从 Execution Mem Pool 中分配内存

  ![acquire-mem-from-execution](/docs/spark/memory/memory-acquire_execution.svg))

上述两个 API 都有可能触发 MemoryStore evict BlockManager 缓存的 Block 来释放 Storage Memory.

MemoryManager 本身并不直接的分配内存, 那真正分配内存来保存数据是 MemoryStore 和 TaskMemoryManager

## MemoryStore

MemoryStore 是 BlockManager 用于在内存中保存 Block 的类. 详见 [MemoryStore](../blockmanager/bm/bm.md#memorystore)

## TaskMemoryManager

TaskMemoryManager 管理 Task 的内存分配, 它是 Per-Task 级别的, 意味着每个 Task 都有自己的 TaskMemoryManager, 也就是可以
通过下面的代码来获得 TaskMemoryManager.

``` scala
val tmm = TaskContext.get().taskMemoryManager()
```

|TaskMemoryManager| |
| ---- | --- |
| MemoryBlock[] pageTable |  |
| BitSet allocatedPages | page 的 bitmap |
| MemoryMode tungstenMemoryMode | 默认是 OnHeap, 可由spark.memory.offHeap.enabled控制 |
| HashSet<MemoryConsumer> consumers | Task中的一组可以 spill 的 MemoryConsumer |

TaskMemoryManager 提供 getPage 来分配 `page`, 如下图所示,

![allocatePage](/docs/spark/memory/memory-allocatePage.svg)

allocatePage 返回的是 MemoryBlock,

| MemoryBlock ||
|---|----|
|Object obj | OnHeap 分配, obj为 long[]数组; OffHeap分配, obj=null|
|long offset| OnHeap, 相对于JVM对象的偏移; OffHeap, offset为Unsafe memory的地址|
|long length| 长度|

TaskMemoryManager 用于分配 page, 而 MemoryConsumer 用来消费分配的 Page.

