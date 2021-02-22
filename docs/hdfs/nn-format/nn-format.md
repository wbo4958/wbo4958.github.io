---
layout: page
title: NameNode 格式化
nav_order: 5
parent: HDFS
---

# NameNode 格式化
{: .no_toc }

本文通过学习 NameNode format 的整个流程来了解 NameNode 的目录结构以及相关文件内容. 本文基于 hadoop/branch-3.3.0

## 目录
{: .no_toc .text-delta}

1. TOC
{:toc}

## format overview

format 的命令行:

``` bash
hdfs namenode -format
```

![format-overview](/docs/hdfs/nn-format/hdfs-NN-format.svg)

format 的操作是比较危险的，它会将 hdfs 中保存到 NameNode 里的元信息全部删除, 那结果是所有的保存到 DataNode 中的数据将会失效. 因此为防止误操作， hdfs 提供 **dfs.namenode.support.allow.format** 配置来指明是否允许对 namenode 进行 format. 当该配置为 true 且用户执行 format 命令时会直接 throw exception.

format 默认是交互式，但也可以强制进行格式化，即

``` bash
hdfs namenode -format -force
```

## format 相关类

namenode format 并不会创建 NameNode 类，但它会创建很多相关辅助类比如 FSImage, NNStorage 等来达到 format 的目的.

![format-overview](/docs/hdfs/nn-format/hdfs-FSImage.svg)

- FSImage 负责 checkpointing 以及记录 namespace 的修改
- FSEditLog 维护着对 namespace 进行修改的日志文件
- NNStorage 管理着 namenode 所使用的文件夹
- StorageDirectory 表示一个具体的文件夹
- NNStorageRetentionManager 管理着 checkpoint 和 edit log 数量.

## namenode 相关目录的配置文件

---|---
**配置**|
dfs.namenode.name.dir  | fsimage保存在local storage的 dir地址
dfs.namenode.shared.edits.dir | primary/secondary 共享的 edit dir地址
dfs.namenode.edits.dir        | editlog 的目录

如果没有设置 `dfs.namenode.name.dir`, namenode目录地址默认为 **${hadoop.tmp.dir}/dfs/name**. 整个 edits 的地址由 **dfs.namenode.shared.edits.dir** + **dfs.namenode.edits.dir**, 如果这两个都没有设置, 则默认为 **dfs.namenode.name.dir** 所指定的地址.

## namenode format 步骤

1. 删除 ${dfs.namenode.name.dir}/current 下所有的文件
2. 创建 ${dfs.namenode.name.dir}/current 目录
3. 创建 ${dfs.namenode.name.dir}/current/VERSION 并写入 namespaceId/clusterId/blockpoolID/layout version 等
4. 创建 ${dfs.namenode.name.dir}/current/seen_txid 记录上一次 checkpoint 或 edit log roll 的 transaction id. 格式化后,该值为 0
5. 创建单独的线程将 FSImage 写入到 current/fsimage_${seen_txid}, 并生成该 fsimage 文件的md5校验码保存到fsimage_${seen_txid}.md5中

更多的文件介绍可以参考[这里](https://docs.cloudera.com/runtime/7.2.0/data-protection/topics/hdfs-namenodes.html)

## fsimage 中的内容

- saveNameSystemSection
  
  保存 NameSystem 相关信息

  ``` java
  NameSystemSection.Builder b = NameSystemSection.newBuilder()
    // 当前 Legacy block 生成的 戳记
    .setGenstampV1(blockIdManager.getLegacyGenerationStamp())
    // 第一个切换到 顺序 的 block id的  戳记
    .setGenstampV1Limit(blockIdManager.getLegacyGenerationStampLimit())
    // 全局生成戳记
    .setGenstampV2(blockIdManager.getGenerationStamp())
    // 按序分配的最后那个block 的 id
    .setLastAllocatedBlockId(blockIdManager.getLastAllocatedContiguousBlockId())
    // 按序分配的最后那个stripe block 的 id
    .setLastAsaveFSImageInAllDirsllocatedStripedBlockId(blockIdManager.getLastAllocatedStripedBlockId())
    .setTransactionId(context.getTxId());

  // namespace id
  b.setNamespaceId(fsn.unprotectedGetNamespaceInfo().getNamespaceID());
    if (fsn.isRollingUpgrade()) {
    // 如果是 rolling upgrad, 还需要 rolling upgrade 的开始时间 
    b.setRollingUpgradeStartTime(fsn.getRollingUpgradeInfo().getStartTime());
  }
  ```

- saveErasureCodingSection
  
  只有当 NameNode 支持 ERASURE_CODING 时，才保存 ErasureCoding 相关内容

  ``` java
  // 保存 erasure coding 的policy
  ErasureCodingSection section = ErasureCodingSection.newBuilder().
    addAllPolicies(ecPolicyProtoes).build();
  ```

- saveInodes
  
  保存 INode 的相关信息, format 后所有的  INode 信息都清空， 那 saveInodes 只会保存 lastInodeId: 16385, 以及 inode number=1 (root inode)

- saveSnapshots TOBEDONE
  
  保存 snapshotCounter 与 numSnapshots

- saveSecretManagerSection TOBEDONE
- saveCacheManagerSection TOBEDONE
- saveStringTableSection TOBEDONE
