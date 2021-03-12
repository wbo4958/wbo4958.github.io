---
layout: page
title: window 实现
nav_order: 10
parent: Spark 
---

# Spark SQL window
{: .no_toc}

本文通过代码学习 Spark 中 window 函数的实现. 本文基于 Spark 3.1.1.

[window function](https://spark.apache.org/docs/latest/sql-ref-syntax-qry-select-window.html)

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
  
  所有的 window 相关的表达都记录在 **Project** 新增加的列.

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
  
  这里的 Executed Plan 并没有加入 WholeStage, 在 SparkPlan 中, WindowExec 的 requiredChildDistributiono为ClusteredDistribution, 而 WindowExec 的 child 为 LocalTableScanExec 的输出并不符合 distribution 要求， 因此需要加入 ShuffleExchangeExec, 同理 requiredChildOrdering 也不符合要示， 也需要加入 SortExec, 这里是 local sort, 并不需要 global 的sort.

  **ClusteredDistribution** 会创建 HashPartitioning, 而需要进行 hash 计算的即是 WindowExec中的 partitionSpec列.