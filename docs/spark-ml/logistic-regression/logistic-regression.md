---
layout: page
title: LogisticRegression
nav_order: 10
parent: Spark-ML
---

# Spark ML LogisticRegression 算法
{: .no_toc}

LogisticRegression 是一个分类算法, 可以用于 Binary Regression 和 Multi-classes Regression.

## 目录
{: .no_toc .text-delta}

1. TOC
{:toc}

## 测试代码

``` scala
val df = Seq(
      (Vectors.dense(46), 0),
      (Vectors.dense(69), 1),
      (Vectors.dense(32), 0),
      (Vectors.dense(60), 1),
      (Vectors.dense(52), 1),
      (Vectors.dense(41), 0)).toDF("value", "label")

val rf = new LogisticRegression()
      .setLabelCol("label")
      .setFeaturesCol("value")

val model = rf.fit(df)
```

## Train

![lr-train](/docs/spark-ml/logistic-regression/logistic_regression-flow.drawio.svg)

整个训练的流程如上所示. LogisticRegression 作为入口函数, 在进入 iteration 之前先 train 一次
获得当前 State 信息, 根据此 State 信息进入 iteration,  每一次 iteration 都会计算相应的梯度, loss
等相关信息, 计算完成后会判断是否需要进行下一步的 iteration. 最后返回结果.

### 计算 Mean/Std/Count

LogisticRegression 首先通过一个 Job 计算出 dataset 中每个 feature 的  Mean/Standard Deviation 以及所有的sample个数.

计算 std, spark 采用了 [Welford's online algorithm](https://en.m.wikipedia.org/wiki/Algorithms_for_calculating_variance)
一次性就计算出 mean/std.

### 创建优化器

优化器是对 cost function (objective function) 查找最小值的方法. 一般常用的 [gradient descent](https://en.wikipedia.org/wiki/Gradient_descent) (一阶
迭待优化算法) 但是 gradient descent 收敛慢. 于是出现了 newton's method, newton's method 收敛更快, 
但是需要进行大量 Hessian 矩阵计算. 于是出现了 BGFS/LBGFS 是一种类牛顿方法, 但计算一个和 Hessian 近似的矩阵. 参考[Gradient Descent vs L-BFGS-B](https://gbhat.com/machine_learning/sgd_vs_lbfgsb.html).

Spark 对于 Lasso/Ridge/ElasticNet 采用不同的优化器, 如 BreezeLBFGSB, BreezeLBFGS 和 BreezeOWLQN.

本例中使用的是 BreezeLBFGSB

### 创建初始化的系数.

根据 spark 的注释. 对于 Binary Classification, 当初始化 coefficients = 0, 

``` 
P(0) = 1 / (1 + exp(b)), and
P(1) = exp(b) / (1 + exp(b))

intercept = log{P(1) / P(0)} = log{count_1 / count_0}
```

时,收敛更快. 这是什么原理?