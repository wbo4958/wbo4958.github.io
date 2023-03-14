---
layout: page
title: RandomForest
nav_order: 5
parent: Spark-ML
---

# Spark ML RandomForest 算法
{: .no_toc}

RandomForest 相关的概念可以参考 [RandomForest wiki](RandomForest). 本文主要是通过训练 iris 数据集来学习
Spark RandomForest 的实现. 

## 目录
{: .no_toc .text-delta}

1. TOC
{:toc}

## 测试代码

``` scala
val df = spark.read.parquet("~/data/iris/parquet").repartition(2)

val trainDf = new VectorAssembler()
  .setInputCols(Array("sepal_length", "sepal_width", "petal_length", "petal_width"))
  .setOutputCol("features").transform(df)
  .select("features", "class")

val rf = new RandomForestClassifier()
  .setLabelCol("class")
  .setFeaturesCol("features")
  .setNumTrees(2)
  .setSeed(12)

val model = rf.fit(trainDf)
```

`repartition(2)` 将会force 只有两个 task来实现 RandomForest 算法

## Train

![rf-train](/docs/spark-ml/random-forest/random-forest-flow.drawio.svg)

整个训练的流程如上所示. Spark 版本的 RandomForest 分布式训练并不是只有一个 Spark Job, 
相反包含多个 Job, 其中可以将 Job 分为两种, 第一种是获得所有 feature 的 所有 splits (只有一个Job).
第二种 Job 是计算 LearningNode 的 split 点, 直到不可再分为止.

### 数据流

整个数据流如下所示,

![dataflow](/docs/spark-ml/random-forest/random-forest-dataflow.drawio.svg)

`RDD[[Vector, Label]] -> RDD[Instance] -> RDD[TreePoint] -> RDD[BaggedPoint]`

- Instance

``` scala
case class Instance(label: Double, weight: Double, features: Vector)
```

- TreePoint

``` scala
class TreePoint(
    val label: Double,
    val binnedFeatures: Array[Int],
    val weight: Double)
```

binnedFeatures 是一个int数组, 它的长度等于 feature 个数, 其中每个元素的值表示它在该 feature 的哪个 bin 中.

如, `TreePoint(1.0, 15,8,19,10, 1.0)`, 该instance被分到第一个feature的15号bin, 被分到第二个feature的8号bin,
依次类推.

- BaggedPoint

``` scala
class BaggedPoint[Datum](
    val datum: Datum,
    val subsampleCounts: Array[Int],
    val sampleWeight: Double = 1.0)
```

BaggedPoint 是 [Bagging](https://www.ibm.com/topics/bagging) 的概念, datum 指向真正的 TreePoint,
subsampleCounts 是一个 int 数组, 它的长度由 number of trees 决定的, 每一个元素表示该 TreePoint 在 build
对应 tree 的 dataset 中的个数, 如,`BaggedPoint{TreePoint(1.0, 15,8,19,10, 1.0) 0,2}`, 表示该 
`TreePoint(1.0, 15,8,19,10, 1.0)` 在 build tree0 时没有, 而在 build tree1 时有 2 个相同的.

### 计算 Feature Split 信息.

RandomForest 对每个 feature 采用 HistGram 方法将所有样本放入到对应的 bins
(默认最大只有32个 bin), 比如 iris 有 150 个样本, 则需要将该 150 个样本放到 32 个 bin中.
注意, 具有相同 value 的 sample 必须放在同一个 bin里. 每一个 split 由 ContinuousSplit(如果是Categorical数据, 则由CategoricalSplit)表示,

``` console
ContinuousSplit
  - featureIndex    基于哪个 Feature
  - threshold       split 值
```

![findSplits](/docs/spark-ml/random-forest/random-forest-findSplits.drawio.svg)

如图所示, 在计算 ContinuousSplit 时, 会通过 aggregateByKey 获得每个 value以及它的count, 然后对 value 进行 sort, 最后将 sort 后的 value
划分到对应的 bins. 

最后将每个 feature 的 ContinuousSplit 收集回 driver 端, 为后面查找 best split 使用


### 计算LearningNode的最好分裂点

``` scala
class LearningNode(
    var id: Int, // 在 tree 中的 ID
    var leftChild: Option[LearningNode],
    var rightChild: Option[LearningNode],
    var split: Option[Split], // 保存分裂点
    var isLeaf: Boolean, //是否是叶子节点
    var stats: ImpurityStats) // 该节点的 stats 信息
```

LearningNode 是 Train 过程中的中间变量, 它记录着分裂点信息以及树节点的相关信息.

**Driver 端** 

1. 根据 numTrees 生成对应的 Root LearningNode (比如 numTrees=2, 则生成 2 个 LearningNode, 
    此时 LearningNode 代表不同 Tree 的 Root Node). 并将生成的 LearningNode 加入到 nodeStack 中.
1. 依次遍历 nodeStack, 并根据 mem 相关信息选择当前需要分裂的 LearningNode 作为一个 group 同时处理.

**Executor 端**

- 获得 Feature 的 stats 信息

![stats](/docs/spark-ml/random-forest/random-forest-stats.drawio.svg)

计算 LearningNode 的 split 的依据是 stats 信息, 对于 RandomForestClassifier 默认采用的 GiniAggregator, 对于 Gini
可以参考[Gini-impurity](https://victorzhou.com/blog/gini-impurity/)

Spark 用 DTStatsAggregator 来记录 subsampled features 所有 bin 的 stat 信息.

``` scala
class DTStatsAggregator {
  featureSubset: Option[Array[Int]] # 只处理部分 features (feature subsampling)
  allStats: Array[Double] # 所有 subsampled features 所有 bin 的 stats
  numBins: Array[Int] # subsampled feature所对应的 bins
}
```

那 Stats 值表示什么意思? 来看下 GiniAggregator

``` scala
class GiniAggrator {

  def update(
      allStats: Array[Double],
      offset: Int,
      label: Double,
      numSamples: Int,
      sampleWeight: Double): Unit = {
    if (label >= numClasses) {
      throw new IllegalArgumentException(s"GiniAggregator given label $label" + s" but requires label < numClasses (= ${numClasses}).")
    }
    if (label < 0) {
      throw new IllegalArgumentException(s"GiniAggregator given label $label" + s"but requires label to be non-negative.")
    }
    allStats(offset + label.toInt) += numSamples * sampleWeight
    allStats(offset + statsSize - 1) += numSamples
  }
}
```

其中 `offset` 是不同 feature 不同 label 的 offset, allStats 也就是 DTStatsAggregator中的 allStats. 对于 Iris 第一个 sample

``` console
Instance(1.0,1.0,[6.1,2.9,4.7,1.4]) 
    -> TreePoint(1.0, 15,8,19,10, 1.0) 
    -> BaggedPoint{TreePoint(1.0, 15,8,19,10, 1.0) 0,1}


Label = 1

Feature 0 对应的 bin = 15
Feature 1 对应的 bin = 8
...
```

则 DTStatsAggregator 中 allStats 如下所示.

|bin number| label 0 | label 1 | label 2| num_samples|
| :----: | :----: | :----: | :----: | :----: |
|0 | 0 | 0 | 0 | 0 |
|1 | 0 | 0 | 0 | 0 |
| ... |... | ... | ... | ... |
|8 | 0 | 1 | 0 | 1 |
| ... |... | ... | ... | ... |
|15 | 0 | 1 | 0 | 1 |
| ... |... | ... | ... | ... |

可以看到 bin8 和 bin15 的 label1 已经被更新了,且 num_samples 也更新了.

- 找到最好的分裂点

- 首先将 stats 逐行累加 (mergeForFeature), 如 stats 所示. 
- 然后依次遍历每个 feature 每个bin, 计算出如果按当前 bin 划分后的 gain.
- 最后获得最大收益时的 split feature, 以及 ContinuousSplit 等信息返回给 Driver 端

如下所示

``` console
在 Partition 0 中 (每个 task 通过 reduceByKey 从 2 个 feature 中找到一个 best split)

依次遍历 feature index = 0 获得的 stat, 找到 best gain.

splitIdx=1,  left={3,0,0}, right=total-left={40,56,60}, parent={43, 56, 159}
   leftImpurity=1-(3/3)*(3/3)-(0/3)*(0/3)-(0/3)*(0/3) = 1
   rightImpurity=1-(40/156)*(40/156)-(56/156)*(56/156)-(60/156)*(60/156)=0.6574621959237343
   parentImpurity = 1-(43/159)*(43/159)-(56/156)*(56/156)-(60/156)*(60/156)=0.6605169138879
   
   leftWeight=leftCount/totalCount=3/159=0.01886792
   rightWeight=rightCount/totalCount=156/159=0.98113

gain = parentImpurtity - leftWeight*leftImpurity - rightWeight*rightImpurity = 0.01535966505706631

splitIdx=2 时, gain=0.04792215497804686 依次类推

最后找到 
featureIndex: 0, bestFeatureSplitIndex: 9, 
bestFeatureGainStats: gain = 0.1819334767348158, impurity = 0.6604169138879, 
        left impurity = 0.43222354340071356, right impurity = 0.5050485246544457

同理依次遍历 feature index=1 获得的 stat, 找到 best gain
featureIndex: 1, bestFeatureSplitIndex: 12, 
bestFeatureGainStats: gain = 0.15506503646975472, impurity = 0.6604169138879, 
        left impurity = 0.5832507105558861, right impurity = 0.2391975308641974

最后在 feature index0和 feature index1 中再进行比较选择  best split
```

因此 Partition 0/Partition 1 返回给 Driver 的 best split 如下所示,

![collect-best-split](/docs/spark-ml/random-forest/random-forest-collectBestSplit.drawio.svg)

### 构造 LearningNode 树

Driver 端在 collect best split 后, 开始构造 Learning Tree.

如图所示

![construct-learning-node-tree](/docs/spark-ml/random-forest/random-forest-construct-learning-tree.drawio.svg)

- 根据treeIndex 找到第一棵树当前正在进行分裂的 LearningNode 判断该节点是否需要再进行分裂, 1. 是否达到 maxDepth, 2. gain <= 0,  如果不能再分裂,则设置该节点为 leaf
- 如果可以继续分裂, 创建LearningNode 作为 left child, 继续判断 left child是否可以进行 split, 判断依据 child 是否达到 maxDepth, leftImpurity 是否足够小
- 创建LearningNode 作为 right child,  继续判断 right child是否可以进行 split, 判断依据 child 是否达到 maxDepth, rightImpurity是否足够小
- 最后将新创建的 left/right LearningNode 加入到 while loop 中继续寻找分裂点.

注意每个 LearningNode 都会 sampling feature, 因为即使在同一个树中 每个 LearningNode split的feature可能是不一样.

## 模型

当所有的 LearningNode 都不能再分时, 那开始构造真正的决策树, 

![rf-model](/docs/spark-ml/random-forest/random-forest-trees.drawio.svg)

主要是从 Root LearningNode 调用 toNode 递归的判断该 LearningNode 是作为 LeafNode 还是 InternalNode.

判断依据是

1. 如果 left.prediction == right.prediction, 则该 LearningNode 作为 LeafNode, 否则作为 InternalNode
2. 如果该 LearningNode 没有 left/right, 则作为 LeafNode.

### 什么是 prediction

上面所说的 prediction 对于  RandomForestClassifier 来说也就是该节点 predict 后的 label. 对于 GiniAgg 来说

``` scala
  def predict: Double = if (count == 0) {
    0
  } else {
    indexOfLargestArrayElement(stats) //返回 stats 中最大的所在的 index 也就是 label.
  }
```

如有个 stats 是 [0, 44, 2] 则 predict 为 1, 即 label 1.


### raw prediction

TODO

### probability prediction

TODO

### feature importance

TODO
