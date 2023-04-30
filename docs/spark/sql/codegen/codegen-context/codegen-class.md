---
layout: page
title: Codegen工具类
nav_order: 5002
parent: sql
grand_parent: spark
---

# Spark codegen 工具类
{: .no_toc}

本文主要是学习常用的 CodeGenerator 是怎样生成代码的, 本文基于 Spark 3.1.1.

## 目录
{: .no_toc .text-delta}

1. TOC
{:toc}

## CodeGenerator

CodeGenerator 是一个抽象类，它主要是用来根据输入的 expression 生成代码用于表达式计算.

![](/docs/spark/sql/codegen/codegen-context/codegen-codegenerator.svg)

其中

``` scala
def generate(expressions: InType)
```

用于接收已经绑定的 expression， 而

``` scala
  /** Generates the requested evaluator binding the given expression(s) to the inputSchema. */
  def generate(expressions: InType, inputSchema: Seq[Attribute]): OutType =
    generate(bind(expressions, inputSchema))
```

用于接收还没有绑定的表达示， 它也是先绑定再调用第一个 generate.

## GenerateOrdering

``` scala
val c1 = AttributeReference("c1", IntegerType)()
val c2 = AttributeReference("c2", IntegerType)()
val c3 = AttributeReference("c3", IntegerType)()
val input = Seq(c1, c2, c3)
val order = Seq(SortOrder(c1, Ascending), SortOrder(c2, Descending))
val ordering = RowOrdering.create(order, input)
```

RowOrdering.create 最终调用到 GenerateOrdering.generate 函数生成一个 BaseOrdering的子类. GenerateOrdering 用于生成代码比较两个 InternalRow 在给定的列数据的大小. 返回值为正数，表示大于,负数表示小于，0表示等于

![](/docs/spark/sql/codegen/codegen-context/codegen-order.svg)

## GeneratePredicate

``` scala
val c1 = AttributeReference("c1", IntegerType)()
val condition = GreaterThan(c1, Literal(3, IntegerType))
val input = Seq(c1, AttributeReference("c2", IntegerType)())
val predicate = Predicate.create(condition, input)
```

Predicate.create 最终调用 GeneratePredicate.generate 函数生成一个 BasePredicate 的子类. GeneratePredicate 用于生成代码来判断表达示在 InternalRow 上运算的是否为 true 或 false.

![](/docs/spark/sql/codegen/codegen-context/codegen-predicate.svg)

## Projection

![](/docs/spark/sql/codegen/codegen-context/codegen-projection-class.svg)

Projection 意为投影的意思. 即将 表达示 apply到输入的 InternalRow 上的运算结果投影到另一个 InternalRow 上. 根据apply输出 InternalRow 的类型可以将Projection 分为 safe projection (SpecificInternalRow), 和 unsafe projection (UnsafeRow).

### GenerateSafeProjection

``` scala
val schema1 = StructType((1 to N).map(i => StructField("", IntegerType)))
val safeProj = SafeProjection.create(schema1)
```

SafeProjection.create 最终会调用到 GenerateSafeProjection.generate 函数生成一个 BaseProjection 的子类. 用于将输入的 InternalRow 转换为另一个 SpecificInternalRow.

![](/docs/spark/sql/codegen/codegen-context/codegen-SafeProjection.svg)

### GenerateUnsafeProjection

``` scala
val schema1 = StructType((1 to N).map(i => StructField("", IntegerType)))
val unsafeProj = UnsafeProjection.create(schema1)
```

UnsafeProjection.create 最终会调用到 GenerateUnsafeProjection.create 函数生成一个 UnsafeProjection 的子类. 用于将输入的 InternalRow 转换为另一个 UnsafeRow.

![](/docs/spark/sql/codegen/codegen-context/codegen-UnsafeProjection.svg)

### GenerateMutableProjection

``` scala
val mutableProj = MutableProjection.create(Seq(BoundReference(0, IntegerType, true)))
```

MutableProjection.create 最终会调用到 GenerateMutableProjection.generate 函数生成一个 BaseMutableProjection 的子类用于将输入的 InternalRow 转换为另一个 InternalRow. 该类允许用户设定 target InternalRow, 如果没有指定， 则默认使用GenericInternalRow.

![](/docs/spark/sql/codegen/codegen-context/codegen-MutableProjection.svg)
