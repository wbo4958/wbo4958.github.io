---
layout: page
title: RDD-aggregate/treeAggregate
nav_order:  10000000
parent: Spark 
---

# Spark RDD aggregate/treeAggregate

{: .no_toc}

rdd.aggregate 和 rdd.treeAggregate 被广泛应用到 Spark ML library 中, 本文通过一个 sample 来了解
这两个 API 背后的工作原理


## 目录
{: .no_toc .text-delta}

1. TOC
{:toc}

## aggregate

### 测试代码

``` scala
// executor中 task 执行的方法，直接加和
def seqOp(s1: Int, s2: Int): Int = {
  val partitionId = TaskContext.get().partitionId()
  val out = s1 + s2
  println(s"partitionId: ${partitionId} seq: input $s1, $s2. out => ${out}")
  out
}

// 在driver或executor端汇总
def combOp(c1: Int, c2: Int): Int = {
  val out = c1 + c2
  if (TaskContext.get() == null) {
    println(s"driver comb: input $c1, $c2 out => ${out}")
  } else {
    val partitionId = TaskContext.get().partitionId()
    println(s"partitionId: ${partitionId} comb: input $c1, $c2 out => ${out}")
  }
  out
}

val spark = SparkSession.builder().master("local[1]").appName("tf-idf").getOrCreate()
spark.sparkContext.setLogLevel("WARN")
// 创建rdd，并分成6个分区
val rdd = spark.sparkContext.parallelize(1 to 12, 6)
// 输出每个分区的内容
println("-----------------")

rdd.mapPartitionsWithIndex((index: Int, it: Iterator[Int]) => {
  Array((s" $index : ${it.toList.mkString(",")}")).toIterator
}).foreach(println)

println("-----------------")

// 执行agg
val res1 = rdd.aggregate(0)(seqOp, combOp)
println("-----------------")
println(res1)
```

输出结果:

``` console
-----------------
 0 : 1,2
 1 : 3,4
 2 : 5,6
 3 : 7,8
 4 : 9,10
 5 : 11,12
-----------------
partitionId: 0 seq: input 0, 1. out => 1
partitionId: 0 seq: input 1, 2. out => 3
driver comb: input 0, 3. out => 3              // 当某个task做完 seqOp 后, driver端收到其结果,并进行 combOp
partitionId: 1 seq: input 0, 3. out => 3
partitionId: 1 seq: input 3, 4. out => 7
driver comb: input 3, 7. out => 10
partitionId: 2 seq: input 0, 5. out => 5
partitionId: 2 seq: input 5, 6. out => 11
driver comb: input 10, 11. out => 21
partitionId: 3 seq: input 0, 7. out => 7
partitionId: 3 seq: input 7, 8. out => 15
driver comb: input 21, 15. out => 36
partitionId: 4 seq: input 0, 9. out => 9
partitionId: 4 seq: input 9, 10. out => 19
driver comb: input 36, 19. out => 55
partitionId: 5 seq: input 0, 11. out => 11
partitionId: 5 seq: input 11, 12. out => 23
driver comb: input 55, 23. out => 78
-----------------
78
```

### spark 实现

测试代码在 driver 中生成数据, 并强制分为 6 个 partitions. 这样每个 partition的数据为2个, 且设置 spark local core=1, 表示每时只有一个 task 在执行, 这样可以避免 log overlap.
当执行 aggregate 时, 将 partition 里的数据进行 seqOp 操作, 即求合, 然后将每个分区执行后的结果传回给 driver 进行 combOp, 即在 driver 端进行汇总.

``` scala
def aggregate[U: ClassTag](zeroValue: U)(seqOp: (U, T) => U, combOp: (U, U) => U): U = withScope {
  // Clone the zero value since we will also be serializing it as part of tasks
  var jobResult = Utils.clone(zeroValue, sc.env.serializer.newInstance())
  val cleanSeqOp = sc.clean(seqOp)
  val cleanCombOp = sc.clean(combOp)
  val aggregatePartition = (it: Iterator[T]) => it.aggregate(zeroValue)(cleanSeqOp, cleanCombOp)
  val mergeResult = (_: Int, taskResult: U) => jobResult = combOp(jobResult, taskResult)
  sc.runJob(this, aggregatePartition, mergeResult)
  jobResult
}
```

实现很简单, 将 seqOp 和 combOp 序列化, aggregatePartition 会 capture 这些方法, 最终 Task 在 Executor 端执行 `it.aggregate(zeroValue)(cleanSeqOp, cleanCombOp)`,
也就是 `scala iterator.aggregate()`. 最后在 Driver 端执行 `combOp`.


![agg](/docs/spark/rdd-agg/spark-ml-aggregate.drawio.svg)

## treeAggregate

将上面的例子换成 treeAggregate 得到的结果如下

``` console
-----------------
 0 : 1,2
 1 : 3,4
 2 : 5,6
 3 : 7,8
 4 : 9,10
 5 : 11,12
-----------------
partitionId: 0 seq: input 0, 1. out => 1
partitionId: 0 seq: input 1, 2. out => 3
partitionId: 0 comb: input 0, 3 out => 3  // foldByKey 中进行 combOp
partitionId: 1 seq: input 0, 3. out => 3
partitionId: 1 seq: input 3, 4. out => 7
partitionId: 1 comb: input 0, 7 out => 7
partitionId: 2 seq: input 0, 5. out => 5
partitionId: 2 seq: input 5, 6. out => 11
partitionId: 2 comb: input 0, 11 out => 11
partitionId: 3 seq: input 0, 7. out => 7
partitionId: 3 seq: input 7, 8. out => 15
partitionId: 3 comb: input 0, 15 out => 15
partitionId: 4 seq: input 0, 9. out => 9
partitionId: 4 seq: input 9, 10. out => 19
partitionId: 4 comb: input 0, 19 out => 19
partitionId: 5 seq: input 0, 11. out => 11
partitionId: 5 seq: input 11, 12. out => 23
partitionId: 5 comb: input 0, 23 out => 23

// 下面开始在 shuffle stage task 0 中进行 comb
partitionId: 0 comb: input 3, 11 out => 14
partitionId: 0 comb: input 14, 19 out => 33
partitionId: 0 comb: input 0, 33 out => 33
driver comb: input 0, 33 out => 33  // driver 端进行 combOp

// 下面开始在 shuffle stage task 1 中进行 comb
partitionId: 1 comb: input 7, 15 out => 22
partitionId: 1 comb: input 22, 23 out => 45
partitionId: 1 comb: input 0, 45 out => 45
driver comb: input 33, 45 out => 78 // driver 端进行 combOp
-----------------
78

```

``` scala
def treeAggregate[U: ClassTag](zeroValue: U)(
    seqOp: (U, T) => U,
    combOp: (U, U) => U,
    depth: Int = 2): U = withScope {
  require(depth >= 1, s"Depth must be greater than or equal to 1 but got $depth.")
  if (partitions.length == 0) {
    Utils.clone(zeroValue, context.env.closureSerializer.newInstance())
  } else {
    // 第一步, 对每个 partition 进行 seqOp 操作
    val cleanSeqOp = context.clean(seqOp)
    val cleanCombOp = context.clean(combOp)
    val aggregatePartition =
      (it: Iterator[T]) => it.aggregate(zeroValue)(cleanSeqOp, cleanCombOp)
    var partiallyAggregated: RDD[U] = mapPartitions(it => Iterator(aggregatePartition(it)))

    var numPartitions = partiallyAggregated.partitions.length
    val scale = math.max(math.ceil(math.pow(numPartitions, 1.0 / depth)).toInt, 2)
    // If creating an extra level doesn't help reduce
    // the wall-clock time, we stop tree aggregation.
    // Don't trigger TreeAggregation when it doesn't save wall-clock time

    // 第二步, 加一个或多个 shuffle stage 对上一个 seqOp 的结果 进行 combOp
    while (numPartitions > scale + math.ceil(numPartitions.toDouble / scale)) {
      numPartitions /= scale
      val curNumPartitions = numPartitions

      // 引入 shuffle stage, 对具有相同 hash 值的上一个 stage 中的 task 的 seqOp 结果进行 combOp 操作
      partiallyAggregated = partiallyAggregated.mapPartitionsWithIndex {
        (i, iter) => iter.map((i % curNumPartitions, _))
      }.foldByKey(zeroValue, new HashPartitioner(curNumPartitions))(cleanCombOp).values
    }
    val copiedZeroValue = Utils.clone(zeroValue, sc.env.closureSerializer.newInstance())
    // 最后在 driver 端再次进行 combOp
    partiallyAggregated.fold(copiedZeroValue)(cleanCombOp)
  }
}
```

相对于 aggregate, treeAggregate 引入中间一个或多个 shuffle stage 将具有相同 hash value 的 上个 stage 的 task seqOp 的结果进行 combOp.
最后再在 driver 端将 所有的结果进行 combOp. 通过引入中间 shuffle stage 可以避免 aggregate 将所有结果拉回 driver 计算时出现 OOM.

![treeAgg](/docs/spark/rdd-agg/spark-ml-treeAggregate.drawio.svg)

refer to [https://www.cnblogs.com/xing901022/p/9285898.html](https://www.cnblogs.com/xing901022/p/9285898.html)
