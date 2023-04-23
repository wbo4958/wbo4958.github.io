---
layout: page
title: PCA
nav_order: 20 
parent: Spark-ML
---

# SparkML PCA
{: .no_toc}

LogisticRegression 是一个分类算法, 可以用于 Binary Regression 和 Multinomial Regression.

PCA 是一个降维算法. 详细过程可以参考 [Step-By-Step PCA Guide](https://www.turing.com/kb/guide-to-principal-component-analysis)

## 目录
{: .no_toc .text-delta}

1. TOC
{:toc}

## 测试代码

``` scala
val data = Array(
  Vectors.dense(1, 5, 3, 1),
  Vectors.dense(4, 2, 6, 3),
  Vectors.dense(1, 4, 3, 2),
  Vectors.dense(4, 4, 1, 1),
  Vectors.dense(5, 5, 2, 3),
)

val df = spark.createDataFrame(data.map(Tuple1.apply)).toD("features")
val pca = new PCA().setK(2).setInputCol("features")
val model = pca.fit(df)
```

## train flow

![train](/docs/spark-ml/pca/spark-pca-train.drawio.svg)

Spark 版本的 PCA 实现会根据 feature 数量的不同, 实现会有所不同, 如果 feature < 65535, Spark 计算出协方差, 然后进行奇异值分解, 最后获得 eigenvectors. 但是如果 feature > 65535, 计算协方差会 overflow, 因此采用 arpack 进行奇异值分解.

## 数据流

![data](/docs/spark-ml/pca/spark-pca-data.drawio.svg)

Spark 在计算协方差是通过 treeAggregate 进行分布式计算的, 如上图所示. 但是奇异值分解是在 driver 端做的. 所以PCA会在 driver 端也会有大量计算.

**有一点需要注意:** Spark 在计算协方差时, 并没有对数据进行 标准化, 这点与 [Step-By-Step PCA Guide](https://www.turing.com/kb/guide-to-principal-component-analysis) 是不同的.
