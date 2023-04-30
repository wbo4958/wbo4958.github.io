---
layout: page
title: window 实现
nav_order: 10
parent: sql
grand_parent: spark 
---

# Spark SQL window
{: .no_toc}

本文通过代码学习 Spark 中 window 函数的实现. 本文基于 Spark 3.1.1.

## 目录
{: .no_toc .text-delta}

1. TOC
{:toc}

## Sample code

``` scala
def testUnboundedWindowFunctionFrame(spark: SparkSession) = {
  import spark.sqlContext.implicits._
  // 创建 WindowSpec 指定 partition 为 depName 列
  val overCategory = Window.partitionBy('depName)

    // 测试数据
  val empsalary = Seq(
    // depName, empNo, salary
    Salary("AAA", 1, 5000),
    Salary("CCC", 7, 4200),
    Salary("BBB", 2, 3900),
    Salary("CCC", 8, 6000),
    Salary("CCC", 9, 4500),
    Salary("AAA", 3, 4800),
    Salary("CCC", 10, 5200),
    Salary("BBB", 5, 3500),
    Salary("AAA", 4, 4800),
    Salary("CCC", 11, 5200)).toDS

  // over 指定新增的列的 window 列
  val df = empsalary
    .withColumn("total_salary", sum('salary) over overCategory)
  df.explain(true)
}
```

`df.show()` 的结果如下

``` console
    +-------+-----+------+------------+
    |depName|empNo|salary|total_salary|
    +-------+-----+------+------------+
    |AAA    |1    |5000  |14600       |
    |AAA    |3    |4800  |14600       |
    |AAA    |4    |4800  |14600       |
    |BBB    |2    |3900  |7400        |
    |BBB    |5    |3500  |7400        |
    |CCC    |7    |4200  |25100       |
    |CCC    |8    |6000  |25100       |
    |CCC    |9    |4500  |25100       |
    |CCC    |10   |5200  |25100       |
    |CCC    |11   |5200  |25100       |
    +-------+-----+------+------------+
```

## QueryPlans

![window query plans](/docs/spark/window/window-queryExecution.svg)

- Logical Plan
  
  所有的 window 相关的表达式都记录在 **Project** 新增加的列的表达式中, 即 Alias 中.

- Analyzed Plan
  
  Logical Plan 经过 Analyzer 分析最后获得 Analyzed plan. Analyzer 中主要用到下面的 rules.
  
  - ResolveWindowOrder

    ``` scala
    object ResolveWindowOrder extends Rule[LogicalPlan] {
      def apply(plan: LogicalPlan): LogicalPlan = plan resolveExpressions {
        // window function对于计算的列必须是排序好的
        case WindowExpression(wf: WindowFunction, spec) if spec.orderSpec.isEmpty =>
          failAnalysis(s"Window function $wf requires window to be ordered, please add ORDER BY " +
            s"clause. For example SELECT $wf(value_expr) OVER (PARTITION BY window_partition " +
            s"ORDER BY window_ordering) from table")
        case WindowExpression(rank: RankLike, spec) if spec.resolved =>
          // 加入 order 到 Rank相关的 window functions
          val order = spec.orderSpec.map(_.child)
          WindowExpression(rank.withOrder(order), spec)
      }
    }
    ```

    - ResolveWindowFrame

      解析 UnspecifiedFrame 生成 SpecifiedWindowFrame.

      ``` scala
      object ResolveWindowFrame extends Rule[LogicalPlan] {
        def apply(plan: LogicalPlan): LogicalPlan = plan resolveExpressions {
          // 错误检查
          case WindowExpression(wf: FrameLessOffsetWindowFunction,
            WindowSpecDefinition(_, _, f: SpecifiedWindowFrame)) if wf.frame != f =>
            failAnalysis(s"Cannot specify window frame for ${wf.prettyName} function")
          case WindowExpression(wf: WindowFunction, WindowSpecDefinition(_, _, f: SpecifiedWindowFrame))
              if wf.frame != UnspecifiedFrame && wf.frame != f =>
            failAnalysis(s"Window Frame $f must match the required frame ${wf.frame}")
          case WindowExpression(wf: WindowFunction, s @ WindowSpecDefinition(_, _, UnspecifiedFrame))
              if wf.frame != UnspecifiedFrame =>
            WindowExpression(wf, s.copy(frameSpecification = wf.frame))
          // 解析 UnspecifiedFrame  
          case we @ WindowExpression(e, s @ WindowSpecDefinition(_, o, UnspecifiedFrame))
              if e.resolved =>
            val frame = if (o.nonEmpty) {
              SpecifiedWindowFrame(RangeFrame, UnboundedPreceding, CurrentRow)
            } else {
              SpecifiedWindowFrame(RowFrame, UnboundedPreceding, UnboundedFollowing)
            }
            we.copy(windowSpec = s.copy(frameSpecification = frame))
        }
      }    
      ```

  - ExtractWindowExpressions

    如果有 window 列，则加入 Window plan, window 列的标准是 是否存在 WindowExpression

    ``` scala
    def apply(plan: LogicalPlan): LogicalPlan = plan resolveOperatorsDown {
      // ...
      // We only extract Window Expressions after all expressions of the Project
      // have been resolved.
      case p @ Project(projectList, child)
        // 如果有存在 WindowExpression, 则加入 Window plan
        if hasWindowFunction(projectList) && !p.expressions.exists(!_.resolved) =>
        // 抽取出 window 列 和 非 window 列
        val (windowExpressions, regularExpressions) = extract(projectList)
        // We add a project to get all needed expressions for window expressions from the child
        // of the original Project operator.
        val withProject = Project(regularExpressions, child)
        // Add Window operators.
        val withWindow = addWindow(windowExpressions, withProject)

        // Finally, generate output columns according to the original projectList.
        val finalProjectList = projectList.map(_.toAttribute)
        Project(finalProjectList, withWindow)
    }    
    ```

- Optimized plan
  
  Analyzed paln 出现多个 project, 而本例中 project 生成的列与 LocalRelation 还是 window plan 最后输出的列是一致的，所以 Project 被优化掉了.

- Spark Plan
  
  在 SparkStrategies 的 Window rule下， 如果 window function 是 SQL， 将 Window 替换成 WindowExec, 如果是 Python, 将 Window 替换成 WindowInPandasExec.

- Executed Plan
  
  这里的 Executed Plan 并没有加入 WholeStage, 在 SparkPlan 中, WindowExec 的 partitionSpec 不为空时, requiredChildDistribution 为ClusteredDistribution, 而 WindowExec 的 child 为 LocalTableScanExec 的输出并不符合 distribution 要求， 因此需要加入 ShuffleExchangeExec, 同理 requiredChildOrdering 也不符合要示， 也需要加入 SortExec, 这里是 local sort, 并不需要 global 的sort, 也就是 partition 内要求排序, partition 间不需要进行排序.

  **ClusteredDistribution** 会创建 HashPartitioning, 而需要进行 hash 计算的即是 WindowExec中的 partitionSpec列.

## Window operator 的数据驱动

Window operator 在 child 的 rdd 上做了一次 mapPartitions 操作. 从上面的 executed plan 可以看出, WindowExec 的输入也就是 child (SortExec) 的输出是 partition 间排序的, 如下所示

``` console
+-------+-----+------+
|depName|empNo|salary|
+-------+-----+------+
|AAA    |4    |4800  |
|AAA    |3    |4800  |
|AAA    |1    |5000  |
|BBB    |5    |3500  |
|BBB    |2    |3900  |
|CCC    |11   |5200  |
|CCC    |10   |5200  |
|CCC    |9    |4500  |
|CCC    |8    |6000  |
|CCC    |7    |4200  |
+-------+-----+------+
```

那我们来看下 window operator 是怎么进行数据驱动的.

![data-driven](/docs/spark/window/window-data-driven.svg)

其中

- buffer: ExternalAppendOnlyUnsafeRowArray
- bufferIterator: buffer.generateIterator()
- windowFunctionResult: InternalRow 表示 window 函数在 window frame 中的计算结果
- frame: 一个 WindowFunctionFrame 用于计算 window frame 里的多个 window 函数的值.

大致工作流程如下:

next 函数一次将同一个 group 或 partition 的数据通过 child 的 iterator 读取出来放到 buffer 中. 如图中所示， window operator 处理第一个 group (AAA), 会将所有 AAA 的行读到 buffer 中，此时 buffer 中的数据为 `AAA,1,5000`, `AAA,3,4800`, `AAA,4,4800`. 然后将 buffer 扔给 frame.prepare， 然后通过 `bufferIterator=buffer.generateIterator()` 生成 buffer 的 iterator.

next 函数通过 bufferIterator 获得当前需要被处理的行, 然后将该行的数据扔给 frame.write 去计算， frame计算的结果保存到 windowFunctionResult 中.

最后将 当前处理的行与windowFunctionResult join 起来成为最终的结果.

## WindowFunctionFrame 的种类

Spark 3.1.1 中实现了 7 种不同的 WindowFunctionFrame 用于优化不同的场景.

![window function frame](/docs/spark/window/window-WindowFuncFrame.svg)

如图所示, 根据 window 函数的类型, 以及 WindowFrame 的 lower/upper 来决定最终选择哪个 WindowFunctionFrame 来实现.Window 函数分为两种，一种是利用 Aggregation 的函数如 Sum/Max/Min, 另一种是自定义的 Window Function, 如下所示.

![window function](/docs/spark/window/window-windowfunction.svg)

WindowFunctionFrame 的实现，以 UnboundedWindowFunctionFrame 为例，该 UnboundedWindowFunctionFrame 中 lower/upper 都是无界，也就是说 window frame 是无界的，也就是每行的 window frame都一行，包含所有的数据。所以只需要计算一次，没必要为每行都计算.

下面来看下 SlidingWindowFunctionFrame 的实现方式.

![Slide window function frame](/docs/spark/window/window_frame.gif)

## Window Frame 类型

目前 Window Frame 分类 RowFrame 与 RangeFrame, 它们不同的是 lower 和 upper 表示的内容不同.

- RowFrame

  lower/upper 表示相对于 当前行的行偏移. RowFrame 没有要求 OrderBy 列, 但是一些特殊的 lag/lead/nth_value 要求 OrderBy

- RangeFrame

  是针对当前行中 OrderBy 那列的值的偏移. RangeFrame 要求 WindowSpec 指定 OrderBy 列.

## 参考

- [window function](https://spark.apache.org/docs/latest/sql-ref-syntax-qry-select-window.html)
- [spark-window-function](https://knockdata.github.io/spark-window-function/)
