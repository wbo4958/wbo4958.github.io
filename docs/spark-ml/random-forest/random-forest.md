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

## 流程图

### 训练过程

![rf-train](/docs/spark-ml/random-forest/random-forest-flow.drawio.svg)

整个数据流过程如下所示,

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
`TreePoint(1.0, 15,8,19,10, 1.0)` 在 build tree0 时没有, 而在 build tree1 时有2个相同的.


Spark 版本的 RandomForest 实现并不是只有一个 Spark Job, 相反包含多个 Job, 其中可以将 Job 分为
两种, 第一种是获得所有 feature 的 所有 splits (只有一个Job). 第二种 Job 是计算 LearningNode
的 split 点.

1. 获得 Split

RandomForest 对于每个 feature 采用 HistGram 来将所有的 sample 放入到 对应的 bins
(默认最大只有32个 bin), 比如 iris 有 150 个样本, 则需要将该 150 个样本放到 32 个 bin中.
注意, 具有相同 value 的 sample 必须放在同一个 bin里. 每一个 split 由 ContinuousSplit 表示,

``` console
ContinuousSplit
  - featureIndex    基于哪个 Feature
  - threshold       split 值
```

2. LearningNode





### 模型

![rf-model](/docs/spark-ml/random-forest/random-forest-trees.drawio.svg)




