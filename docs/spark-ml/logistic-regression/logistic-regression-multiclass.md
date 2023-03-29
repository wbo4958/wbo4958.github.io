---
layout: page
title: LogisticRegression Multinomial Classification
nav_order: 15
parent: Spark-ML
---

# SparkML LogisticRegression Multinomial classification 算法
{: .no_toc}

[Logistic Regression Binary classification](logistic-regression.md) 已经介绍了 LogisticRegression 的
Binary classification 原理. 而对于 Multinomial 分类, 不同在于 loss/gradient 的计算.

对于 multi class classification, 主要是

## 目录
{: .no_toc .text-delta}

1. TOC
{:toc}

## 测试代码

``` scala
val df = spark.read.schema(
  "sepal_length float,sepal_width float, petal_length float, petal_width float, label float")
  .csv("iris-pruned.csv")
df.show()

val trainDf = new VectorAssembler()
  .setInputCols(Array("sepal_length", "sepal_width", "petal_length", "petal_width"))
  .setOutputCol("features").transform(df)
  .select("features", "label")
val rf = new LogisticRegression()
  .setLabelCol("label")
  .setFeaturesCol("features")
val model = rf.fit(trainDf)
model.transform(trainDf).collect()
```

``` console
+------------+-----------+------------+-----------+-----+
|sepal_length|sepal_width|petal_length|petal_width|label|
+------------+-----------+------------+-----------+-----+
|         5.0|        3.5|         1.6|        0.6|  0.0|
|         5.1|        3.8|         1.9|        0.4|  0.0|
|         5.6|        2.7|         4.2|        1.3|  1.0|
|         5.7|        2.8|         4.1|        1.3|  1.0|
|         6.8|        3.2|         5.9|        2.3|  2.0|
|         6.5|        3.0|         5.2|        2.0|  2.0|
+------------+-----------+------------+-----------+-----+
```

本例只选择如上的 iris sample 进行 train, 总共 6 个 sample

## Train Preparation

### 计算 Mean/Std/Count

Iris 数据集有 4 个 feature, 1 个 label.

``` console
mean: [5.7833333015441895, 3.166666666666667, 3.816666603088379, 1.3166666477918625]
std:  [0.7305250054931269, 0.4226897837838385, 1.7359915315704273, 0.7467708078966974]
```

### 创建优化器

与 Binary classification 相同, 对于本例用的是 BreezeLBFGSB

### 创建初始化的系数.

Binary classification 只有一组 coefficients + intercept, 个数为 number features + 1.

而与 Binary classification 不同, Multinomial classification 中每一个 label 都有一组 `coefficients + intercept`,
而每一组的 coefficients 的个数等于 features 的个数, 因此 Multinomial 的系数为一个矩阵, shape 为 `(num class) x (num features + 1)`

## Train

Train 的流程与 Binary classification 一样, 只是计算不同.

### 到底计算什么?

Mutlinomial 使用 MultinomialLogisticBlockAggregator 作为 cost function 的实现.

### MultinomialLogisticBlockAggregator

https://spark.apache.org/docs/3.3.2/ml-classification-regression.html#multinomial-logistic-regression

``` scala
  // 每次处理一个 block
  def add(block: InstanceBlock): this.type = {
    // 如果每个 item 的 weight 都为0, 则直接返回
    if (block.weightIter.forall(_ == 0)) return this
    val size = block.size // 多少个 sample, 本例只有 6 个 sample

    // mat/arr here represents margins, shape: S X C 总共 6*3,
    // mat/arr 表示 theta(x) 的 margin
    val mat = DenseMatrix.zeros(size, numClasses) //每个 sample 都有 num Class个 margin
    val arr = mat.values
    if (fitIntercept) {
      // 获得 margin
      val offset = if (fitWithMean) marginOffset else intercept
      var j = 0
      while (j < numClasses) {
        // 初始化 margin. 前面6个是 label0的 margin , 中间6个是 label1 的margin, 后面6个是 label2的 margin
        if (offset(j) != 0) java.util.Arrays.fill(arr, j * size, (j + 1) * size, offset(j))
        j += 1
      }
    }
    // mat = mat + X*coefficients 即 (6*4) * (4*3) = 每个 sample 分别乘以 每个 label 的 coefficients
    BLAS.gemm(1.0, block.matrix, linear.transpose, 1.0, mat)

    // in-place convert margins to multipliers
    // then, mat/arr represents multipliers
    var localLossSum = 0.0
    var localWeightSum = 0.0
    var i = 0
    while (i < size) {
      val weight = block.getWeight(i)
      localWeightSum += weight
      if (weight > 0) {
        val labelIndex = i + block.getLabel(i).toInt * size
        Utils.softmax(arr, numClasses, i, size, arr) // prob
        localLossSum -= weight * math.log(arr(labelIndex))
        if (weight != 1) BLAS.javaBLAS.dscal(numClasses, weight, arr, i, size)
        arr(labelIndex) -= weight
      } else {
        BLAS.javaBLAS.dscal(numClasses, 0, arr, i, size)
      }
      i += 1
    }
    lossSum += localLossSum
    weightSum += localWeightSum

    // mat (multipliers):             S X C, dense                                N
    // mat.transpose (multipliers):   C X S, dense                                T
    // block.matrix (data):           S X F, unknown type                         T
    // gradSumMat (gradientSumArray): C X FPI (numFeaturesPlusIntercept), dense   N
    block.matrix match {
      case dm: DenseMatrix =>
        // gradientSumArray[0 : F X C] += mat.T X dm
        BLAS.nativeBLAS.dgemm("T", "T", numClasses, numFeatures, size, 1.0,
          mat.values, size, dm.values, numFeatures, 1.0, gradientSumArray, numClasses)

      case sm: SparseMatrix =>
        // TODO: convert Coefficients to row major order to simplify BLAS operations?
        // linearGradSumMat = sm.T X mat
        // existing BLAS.gemm requires linearGradSumMat is NOT Transposed.
        val linearGradSumMat = DenseMatrix.zeros(numFeatures, numClasses)
        BLAS.gemm(1.0, sm.transpose, mat, 0.0, linearGradSumMat)
        linearGradSumMat.foreachActive { (i, j, v) => gradientSumArray(i * numClasses + j) += v }
    }

    if (fitIntercept) {
      val multiplierSum = Array.ofDim[Double](numClasses)
      var j = 0
      while (j < numClasses) {
        var i = j * size
        val end = i + size
        while (i < end) { multiplierSum(j) += arr(i); i += 1 }
        j += 1
      }

      if (fitWithMean) {
        // above update of the linear part of gradientSumArray does NOT take the centering
        // into account, here we need to adjust this part.
        // following BLAS.dger operation equals to: gradientSumArray[0 : F X C] -= mat.T X _mm_,
        // where _mm_ is a matrix of size S X F with each row equals to array ScaledMean.
        BLAS.nativeBLAS.dger(numClasses, numFeatures, -1.0, multiplierSum, 1,
          bcScaledMean.value, 1, gradientSumArray, numClasses)
      }

      BLAS.javaBLAS.daxpy(numClasses, 1.0, multiplierSum, 0, 1,
        gradientSumArray, numClasses * numFeatures, 1)
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
