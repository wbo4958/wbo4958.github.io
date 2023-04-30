---
layout: page
title: reduceByKey
nav_order:  1
parent: rdd 
grand_parent: spark
---

# Spark RDD reduceByKey

{: .no_toc}

rdd.reduceByKey 被广泛应用到 Spark ML library 中, 本文通过一个 sample 来了解
该  API 背后的工作原理


## 目录
{: .no_toc .text-delta}

1. TOC
{:toc}


## 测试代码

``` scala
val data = Seq(("Apple", 2),
  ("Pear", 1),
  ("Kiwi", 1),
  ("Apple", 1),
  ("Strawberry", 1),
  ("Banana", 1),
  ("Orange", 1),
  ("Apple", 1),
  ("Pear", 1),
  ("Strawberry", 1),
  ("Banana", 1),
  ("Banana", 1),
)
val rdd = sc.parallelize(data, 3)
val rdd2 = rdd.reduceByKey(_ + _)
rdd2.foreach(println)
```

输出结果:

``` console
(Orange,1)
(Strawberry,2)
(Banana,3)
(Apple,4)
(Kiwi,1)
(Pear,2)
```

## spark 实现

``` scala
def reduceByKey(func: (V, V) => V): RDD[(K, V)] = self.withScope {
    reduceByKey(defaultPartitioner(self), func)
}
```

由 API 定义可以看出该 API 只支持 (K, V) 数据格式, 如果是 (K, V, O) 会提示错误. 那意思是第一列将会
作为 key, 第二列的 value 进行 reduce.

![reduce-by-key](/docs/spark/rdd/reduce-by-key/spark-ml-reduceByKey.drawio.svg)

该 Job 被分为两个 stage, 第一个 shuffle stage (共3个 task) 分别进行 shuffle write (输出的结果是进行 hash partitioned的),
且在写入文件之前会进行 Combine, 如 partition 0 所示, 在写入到 local file 之前, Apple 已经被 combine 了.

第二个stage进行 shuffle read, 且在读取时将具有相同 key 的再次进行 combine. 最后将最终结果返回driver.
