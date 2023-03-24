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

整个训练过程如下所示,

![lr-train](/docs/spark-ml/logistic-regression/logistic_regression-flow.drawio.svg)

## Train Preparation

### 计算 Mean/Std/Count

LogisticRegression 首先通过一个 Job 计算出 dataset 中每个 feature 的  Mean/Standard Deviation 以及所有的sample个数.

计算 std, spark 采用了 [Welford's online algorithm](https://en.m.wikipedia.org/wiki/Algorithms_for_calculating_variance)
一次性就计算出 mean 和 std.

### 创建优化器

优化器是对 cost function (objective function) 查找最小值的方法. 一般常用的 [gradient descent](https://en.wikipedia.org/wiki/Gradient_descent) (一阶
迭待优化算法) 但是 gradient descent 收敛慢. 于是出现了 newton's method, newton's method 收敛更快, 
但是需要进行大量 Hessian 矩阵计算. 于是出现了 BGFS/LBGFS 是一种类牛顿方法, 但只计算一个和 Hessian 近似的矩阵. 参考[Gradient Descent vs L-BFGS-B](https://gbhat.com/machine_learning/sgd_vs_lbfgsb.html).

同时Spark 对于 L1/L2 采用不同的优化器, 如 BreezeLBFGSB, BreezeLBFGS 和 BreezeOWLQN.

本例中使用的是 BreezeLBFGSB

### 创建初始化的系数.

根据 spark 的注释. 对于 Binary Classification, 当初始化 coefficients = 0 时,收敛更快.

``` 
P(0) = 1 / (1 + exp(b)), and
P(1) = exp(b) / (1 + exp(b))

intercept = log{P(1) / P(0)} = log{count_1 / count_0}
```

## Train

有了上面的准备, 就可以开始 train 了.

LogisticRegression 作为入口函数, 在进入 infinite Iterations 之前先 train 一次 获得初始 State 信息,
然后进入 iteration. 

每一次 iteration 都会调用 rdd.treeAggregate 计算相应的梯度, loss 等相关信息, 且将计算结果收集回 driver
端保存在 state 中, 并判断是否收敛.

### 计算什么?

每一次 iteration 其实是一个 Spark Job, 通过 RDD.treeAggregate 计算每个 partition 的 loss sum, 以及 gradient 值.

如 RDDLossFunction 所示

``` scala
  override def calculate(coefficients: BDV[Double]): (Double, BDV[Double]) = {
    // cofficients 是上一次 train 出来的系数 vector.
    val bcCoefficients = instances.context.broadcast(Vectors.fromBreeze(coefficients))
    val thisAgg = getAggregator(bcCoefficients)
    val seqOp = (agg: Agg, x: T) => agg.add(x)
    val combOp = (agg1: Agg, agg2: Agg) => agg1.merge(agg2)
    val newAgg = instances.treeAggregate(thisAgg)(seqOp, combOp, aggregationDepth)

    // 获得 gradient
    val gradient = newAgg.gradient

    // regulation loss
    val regLoss = regularization.map { regFun =>
      val (regLoss, regGradient) = regFun.calculate(Vectors.fromBreeze(coefficients))
      BLAS.axpy(1.0, regGradient, gradient)
      regLoss
    }.getOrElse(0.0)
    bcCoefficients.destroy()
    // 返回 total loss 与 gradient
    (newAgg.loss + regLoss, gradient.asBreeze.toDenseVector)
  }
```

所以可以看出来, treeAggregate 是计算 loss 与 gradient 的.

#### BinaryLogisticBlockAggregator

对于 LogisticRegression Binary classification, 采用 BinaryLogisticBlockAggregator aggregator 根据 [loss-function](https://spark.apache.org/docs/latest/mllib-linear-methods.html#loss-functions) 计算
loss 与 gradient. 

``` scala
  // 处理当前 block
  def add(block: InstanceBlock): this.type = {
    // 如果每个 instance 的 weight 为0, 则直接返回
    if (block.weightIter.forall(_ == 0)) return this
    val size = block.size

    // arr here represents margins
    // 注意这个 margin 并不是与 label 之间的 margin, 而是 h(x)= Sum(Wi * Xi) 的值
    val arr = Array.ofDim[Double](size)
    if (fitIntercept) { 
      // 是否在训练时也训练出 intercept 值
      // 是否需要 scale, 如果是的, 则 offset= cofficients - cofficients*scaledMean
      //                如果不是, 则直接返回最后一组 cofficients
      val offset = if (fitWithMean) marginOffset else coefficientsArray.last
      java.util.Arrays.fill(arr, offset)
    }

    // arr = A * coefficients + arr  获得 prediction value + margins
    BLAS.gemv(1.0, block.matrix, coefficientsArray, 1.0, arr)

    // in-place convert margins to multiplier
    // then, arr represents multiplier
    var localLossSum = 0.0
    var localWeightSum = 0.0
    var multiplierSum = 0.0
    var i = 0
    while (i < size) {
      val weight = block.getWeight(i)
      localWeightSum += weight
      if (weight > 0) {
        val label = block.getLabel(i) //真实值
        val margin = arr(i)
        if (label > 0) { // 先计算该 instance 的 loss, 最后累加到 localLossSum
          // The following is equivalent to log(1 + exp(-margin)) but more numerically stable.
          localLossSum += weight * Utils.log1pExp(-margin)
        } else {
          localLossSum += weight * (Utils.log1pExp(-margin) + margin)
        }

        // multiplier 也是 margin, 只不过是与 label 之间的差距
        val multiplier = weight * (1.0 / (1.0 + math.exp(-margin)) - label)
        arr(i) = multiplier // 更新到 arr 中
        multiplierSum += multiplier
      } else { arr(i) = 0.0 }
      i += 1
    }
    lossSum += localLossSum //所有 block 的 loss sum
    weightSum += localWeightSum  //所有 instance 的 weights

    // predictions are all correct, no gradient signal
    if (arr.forall(_ == 0)) return this // 如果 margins = 0, 表示 prediction 全正确 

    // update the linear part of gradientSumArray
    // A * multiplier 表示当前 block 的梯度.
    // gradientSumArray = gradientSumArray + A * multiplier 累加所有 block 的梯度
    BLAS.gemv(1.0, block.matrix.transpose, arr, 1.0, gradientSumArray)

    if (fitWithMean) {
      // above update of the linear part of gradientSumArray does NOT take the centering
      // into account, here we need to adjust this part.
      BLAS.javaBLAS.daxpy(numFeatures, -multiplierSum, bcScaledMean.value, 1,
        gradientSumArray, 1)
    }

    if (fitIntercept) {
      // update the intercept part of gradientSumArray
      gradientSumArray(numFeatures) += multiplierSum
    }

    this
  }
```

上面的过程很简单,

- 1. 根据 coefficients 计算出 h(x)= Sum(Wi * Xi) 的margin
- 2. arr = A * coefficients + arr  获得 prediction value + margins, 计算出关于h(x)的prediction值并加上 margins
- 3. 依次计算出每个 sample 基于 arr 的 loss, 以及关于 label 的 multiplier
- 4. 根据 multiplier 计算出梯度.

整个迭待过程如下所示.

在计算 loss 和 gradient 之前, 会对 sample 进行 scale. 具体是每个 sample * (inversed std)
本例的 samples 只有一个 feature,

```
samples: [46 69 32 60 52 41]

mean = 50
std = 13.311649
inversed std = 1/13.311649 = 0.075122
scaledMean = mean * (inversed std) = 50 * 0.075122 = 3.75611

samples = samples * (inversed std) = [3.45561995, 5.1834299, 2.4039095, 4.5073303, 3.906353, 3.080009]
```

- Iteration 1