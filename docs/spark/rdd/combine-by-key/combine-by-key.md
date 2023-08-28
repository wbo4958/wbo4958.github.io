---
layout: page
title: combineByKey
nav_order:  3
parent: rdd 
grand_parent: spark
---

# Spark RDD combineByKey

{: .no_toc}

本文基于 spark 3.4 学习 combineByKey


## 目录
{: .no_toc .text-delta}

1. TOC
{:toc}


## 测试代码

``` scala
val rdd = sc.parallelize(
  Seq(
    (1, 2),
    (1, 4),
    (2, 5),
    (2, 3),
    (1, 3),
    (3, 2),
    (3, 3),
    (2, 4),
    (1, 4),
    (2, 2),
  ), 3
)

val result = rdd.combineByKey(
  (value: Int) => (value, 1),
  (accumulator: (Int, Int), value: Int) => {
    (accumulator._1 * value, accumulator._2 + 1)
  },
  (accumulator1: (Int, Int), accumulator2: (Int, Int)) => {
    (accumulator1._1 * accumulator2._1, accumulator1._2 + accumulator2._2)
  },
  2
)

result.collect().foreach(println)
```

该sample是计算相同key的乘积与count

输出结果:

``` console
(2,(120,4))
(1,(96,4))
(3,(6,2))
```

## spark 实现

``` scala
  def combineByKey[C](
      createCombiner: V => C,
      mergeValue: (C, V) => C,
      mergeCombiners: (C, C) => C,
      numPartitions: Int): RDD[(K, C)]
```

由 API 定义可以看出该 API 需要自定义 combiner, 返回 (K, 与 Combiner)
只支持 (K, V) 数据格式, 如果是 (K, V, O) 会提示错误. 那意思是第一列将会
作为 key, 第二列的 value 进行 combine.

![combine-by-key](/docs/spark/rdd/combine-by-key/spark-ml-combineByKey.drawio.svg)

该 Job 被分为两个 stage, 第一个 shuffle stage (共3个 task) 分别进行 shuffle write (输出的结果是进行 hash partitioned的),
且在写入文件之前会进行 Combine.

第二个stage进行 shuffle read, 且在读取时将具有相同 key 的再次进行 combine. 最后将最终结果返回driver.
