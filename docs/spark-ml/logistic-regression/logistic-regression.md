---
layout: page
title: LogisticRegression
nav_order: 10
parent: Spark-ML
---

# Spark ML LogisticRegression 算法
{: .no_toc}

LogisticRegression 是一个分类算法, 可以用于 Binary Regression 和 Multi-classes Regression.

对于 Binary Regression, 主要是

- 线性方程

![theta](/docs/spark-ml/logistic-regression/thelta_x.png)

- prediction 函数

![sigmod](/docs/spark-ml/logistic-regression/sigmod.png)

那最终的目的是找出所有的 theta, 使得 train samples 的 prediction 与 label 一致.

至于怎么找 theta, 怎么计算, 计算什么 请参考下面的学习笔记. 

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

### 到底计算什么?

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

### BinaryLogisticBlockAggregator

对于 LogisticRegression Binary classification, 采用 BinaryLogisticBlockAggregator aggregator 根据 [loss-function](https://spark.apache.org/docs/latest/mllib-linear-methods.html#loss-functions) 计算
loss 与 gradient. 

``` scala
  // 处理当前 block
  def add(block: InstanceBlock): this.type = {
    // 如果每个 instance 的 weight 为0, 则直接返回
    if (block.weightIter.forall(_ == 0)) return this
    val size = block.size

    // arr here represents margins
    // 注意这个 margin 并不是与 label 之间的 margin, 而是 Theta(x)= Sum(Wi * Xi) 的值
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

- 1 根据 coefficients 计算出 Theta(x)= Sum(Wi * Xi) 的margin
- 2 arr = A * coefficients + arr  获得 Theta 函数 prediction value + margins, 计算出关于Theta(x)的prediction值并加上 margins
- 3 依次计算出每个 sample 基于 arr 的 loss, 以及关于 label 的 multiplier
- 4 根据 multiplier 计算出梯度.

在计算 loss 和 gradient 之前, 会对 sample 进行 scale. 具体是每个 `sample * (inversed std)`
注: 本例的 samples 只有一个 feature,

``` console
samples: [46 69 32 60 52 41]

mean = 50
std = 13.311649
inversed std = 1/13.311649 = 0.075122
scaledMean = mean * (inversed std) = 50 * 0.075122 = 3.75611

samples = samples * (inversed std) = [3.45561995, 5.1834299, 2.4039095, 4.5073303, 3.906353, 3.080009]
```

整个迭待过程如下所示.

- **Iteration 1**

initial coefficients = [0, 0]

**Executor side**

``` console
coefficients = [0, 0]
scaled samples = [3.45561995, 5.1834299, 2.4039095, 4.5073303, 3.906353, 3.080009]

marginOffset = coefficients.last - coefficientsArray.T*scaledMean = 0 - 0
Theta(x) initial margins = [0, 0, 0, 0, 0, 0]
Theta(x) margins = initial margins + coefficients*(scaled samples) = [0, 0, 0, 0, 0, 0]
sum of local loss = Sum( Utils.log1pExp(-margin) or (Utils.log1pExp(-margin) + margin) ) = 4.1588883

multiplier (margin with H_theta(Theta(x))) = [0.5, -0.5, 0.5, -0.5, -0.5, 0.5]
multiplierSum = 0

gradients = X.T * multiplier = [-2.328787, 0]
gradients = gradients + (-multiplierSum)*scaledMean = [-2.328787, 0] if fitWithMean
gradients[numFeatures] += multiplierSum 
=> gradients = [-2.328787, 0]
```

**driver side**

最后得出 loss = sum of local loss / weightedSamples, gradient = gradient / weightedSamples

`coefficients = [0, 0], loss = 4.1588883/6 = 0.6931471805599453, gradient=[−0.388131167, 0]`

生成 State
``` console
State {
  x = coefficients = [0, 0],
  value = loss = 0.6931471805599453
  grad = [−0.388131167, 0]
  adjustedValue = value + regulation = 0.6931471805599453
  adjustedGradient = grad + regulation' = [−0.388131167, 0]
  iter = 0 第一轮
  initialAdjVal = 0.6931471805599453  # 上一轮的 state.value 用于判断是否 converge, 如果是第一轮,则等 于 value
}
```

判断是否 converge

- **Iteration 2**

``` console
在 chooseDescentDirection 找出梯度下降的方向 `state.history * state.grad` 得到 dir=[0.38813122653205856, -0.0]
在 determineStepSize 根据 dir 中找出下一个 coefficients = (1/Norm(dir)) * dir = (1/|0.388|) * [0.38813122653205856, -0.0] = [0.999999999, 0]
```

coefficients = [0.999999999, 0]

**Executor side**

``` console
coefficients = [0.999999999, 0]
scaled samples = [3.45561995, 5.1834299, 2.4039095, 4.5073303, 3.906353, 3.080009]

marginOffset = coefficients.last - coefficientsArray.T*scaledMean = 0 - 3.7561086
Theta(x) initial margins = [-3.7561086, -3.7561086, -3.7561086, -3.7561086, -3.7561086, -3.7561086]
Theta(x) margins = initial margins + coefficients*(scaled samples) = [-0.3004887, 1.427321285, -1.352199, 0.75122, 0.150244, -0.6760996]
sum of local loss = Sum( Utils.log1pExp(-margin) or (Utils.log1pExp(-margin) + margin) ) = 2.4177785

multiplier (margin with H_theta(Theta(x))) = [0.5, -0.5, 0.5, -0.5, -0.5, 0.5]
multiplierSum = -0.008499468

gradients = X.T * multiplier = [-1.2520986969938845, 0.0]
gradients = gradients + (-multiplierSum)*scaledMean = [-1.220173771567439, 0.0] if fitWithMean
gradients[numFeatures] += multiplierSum 
=> gradients = [-1.220173771567439, -0.008499468054163961]
```

**driver side**

最后得出 loss = sum of local loss / weightedSamples, gradient = gradient / weightedSamples

`coefficients = [0.999999999, 0], loss = 2.4177785/6 = 0.4029630838653115, gradient=[-0.20336229526123983,-0.0014165780090273268]`

生成 State
``` console
State {
  x = coefficients = [0.999999999, 0],
  value = loss = 0.4029630838653115
  grad = [-0.20336229526123983,-0.0014165780090273268]
  adjustedValue = value + regulation = 0.4029630838653115
  adjustedGradient = grad + regulation' = [-0.20336229526123983,-0.0014165780090273268]
  iter = 1 第一轮
  initialAdjVal = 0.6931471805599453  # 上一轮的 state.value 用于判断是否 converge, 如果是第一轮,则等 于 value
}
```

判断是否 converge

依此类推

- Converge 判断

对于 LBFGS, 默认的收敛条件判断如下所示

``` scala
maxIterationsReached[T](maxIter) ||  // 是否达到最大迭待次数 与 maxIter 相关
functionValuesConverged(tolerance, relative, fvalMemory) || // loss 是否没什么变化, 与 tolerance
gradientConverged[T](tolerance, relative) ||  // 梯度的范数是否足够小, 与 tolerance 相关
searchFailed //在计算时出错
```

## Model

经过上面的计算, 最终获得的 Theta 也就是 coefficients 和 intercept 分别为 

``` console
coefficients = [5.511520037332782] # 本例中只有一个 feature, 因此只有一个 theta
intercept = -270.3529400636783
```

并创建 LogisticRegressionModel, 其中会初始化下面两个变量用于 prediction.

``` scala
  /** Margin (rawPrediction) for class label 1.  For binary classification only. */
  private val margin: Vector => Double = (features) => {
    BLAS.dot(features, _coefficients) + _intercept
  }

  /** Score (probability) for class label 1.  For binary classification only. */
  private val score: Vector => Double = (features) => {
    val m = margin(features)
    1.0 / (1.0 + math.exp(-m))
  }
```

margin 函数用于求得 theta(x) 的值, score 函数用于获得最终的 sigmod(theta(x)) 的 prediction 值 (0, 1)


### predict

``` scala
  override def predict(features: Vector): Double = if (isMultinomial) {
    super.predict(features)
  } else {
    // Note: We should use _threshold instead of $(threshold) since getThreshold is overridden.
    if (score(features) > _threshold) 1 else 0 # _threshold 默认为0, 如果 sigmod 值 > 0, 则 predict 出来为 1
  }
```

### predictRaw

``` scala
  override def predictRaw(features: Vector): Vector = {
    if (isMultinomial) {
      margins(features)
    } else {
      val m = margin(features)
      Vectors.dense(-m, m)
    }
  }
```

predictRaw 获得 theta(x) 的值.

## 参考
- [LogisticRegression](https://github.com/endymecy/spark-ml-source-analysis/blob/master/%E5%88%86%E7%B1%BB%E5%92%8C%E5%9B%9E%E5%BD%92/%E7%BA%BF%E6%80%A7%E6%A8%A1%E5%9E%8B/%E9%80%BB%E8%BE%91%E5%9B%9E%E5%BD%92/logic-regression.md)
- [spark logistic regression](https://spark.apache.org/docs/latest/mllib-linear-methods.html#logistic-regression)
- [loss function](https://spark.apache.org/docs/latest/mllib-linear-methods.html#loss-functions)
- [How to Calculate Standard Deviation](https://www.scribbr.com/statistics/standard-deviation/)
