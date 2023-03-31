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

- 线性方程

![theta](/docs/spark-ml/logistic-regression/thelta_x.png)

- prediction 函数 (softmax)

![softmax](/docs/spark-ml/logistic-regression/softmax.svg)

- loss function

![loss](/docs/spark-ml/logistic-regression/loss.svg)

参考 [softmax loss gradient](http://bigstuffgoingon.com/blog/posts/softmax-loss-gradient/) [一文详解Softmax函数
](https://zhuanlan.zhihu.com/p/105722023)


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

        //计算出 prediction
        Utils.softmax(arr, numClasses, i, size, arr) // prob

        // 计算出 loss
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
      // 计算梯度
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

在计算 loss 和 gradient 之前, 会对 sample 进行 scale. 具体是每个 `sample * (inversed std)`
注: 本例的 samples 只有一个 feature,

``` console
samples: 
5.0, 3.5, 1.6, 0.6
5.1, 3.8, 1.9, 0.4
5.6, 2.7, 4.2, 1.3
5.7, 2.8, 4.1, 1.3
6.8, 3.2, 5.9, 2.3
6.5, 3.0, 5.2, 2.0

mean: [5.7833333015441895, 3.166666666666667, 3.816666603088379, 1.3166666477918625]
std:  [0.7305250054931269, 0.4226897837838385, 1.7359915315704273, 0.7467708078966974]

inversed std = [1.3688785359578064, 2.3658012054329545, 0.5760396763545106, 1.339098943645816]
scaledMean = mean * (inversed std) = [7.916680822773837, 7.491703817204357, 2.198551394796099, 1.7631469171917609]

samples = samples * (inversed std) = 
6.844392679789032   8.28030421901534   0.9216634959010731  0.8034593981140964  
6.981280402838382   8.990044467835034  1.094475371339714   0.5356395854399781  
7.6657196708172854  6.38766336747917   2.4193665308180954  1.7408285628863474  
7.802607393866636   6.62424326240208   2.361762618118069   1.7408285628863474  
9.308374305605945   7.570563970195646  3.398634145427037   3.0799275065321634  
8.897710483725742   7.097403616298863  2.995406207172606   2.678197887291632
```

整个迭待过程与 LogisticRegression Binary classification 类似.

## Model

经过上面的计算, 最终获得的 Theta 也就是 coefficients 和 intercept 分别为 

- coefficient

``` console
-4.125986206819423  15.652431537565903  -3.0244748666738954  -3.695660456730089  
-7.369270983747656  -28.21306513402949  0.7202419665333993   -5.540050255560649  
11.495257190567079  12.560633596463585  2.304232900140496    9.235710712290738 
```

- intercept

```
[-9.364161124333789, 137.13105330440678, -127.76689218007299]
```

并创建 LogisticRegressionModel, 其中会初始化下面两个变量用于 prediction.

``` scala
  /** Margin (rawPrediction) for each class label. */
  private val margins: Vector => Vector = (features) => {
    val m = _interceptVector.copy
    BLAS.gemv(1.0, coefficientMatrix, features, 1.0, m)
    m
  }
```

margins 函数用于求得 theta(x)*coefficients + intercept 的值

### predictRaw

``` scala
  override def predictRaw(features: Vector): Vector = {
    if (isMultinomial) {
      margins(features)
    } else {
      ...
    }
  }
```

求得 intercept + Coefficients * Input 作为 Raw value.

predictRaw 获得 Theta(x) 的值.

### predictProbability

首先 predict raw value, 然后通过 softmax 算得每个 label 的概率

### predict

``` scala
  override def predict(features: Vector): Double = if (isMultinomial) {
    super.predict(features)
  } else {
    ...
  }
```

首先 predict raw value, 然后找到最大值时 label value.
