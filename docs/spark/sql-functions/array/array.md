---
layout: page
title: array
nav_order: 5
parent: Sql functions
grand_parent: Spark
---

# array
{: .no_toc}

本文主要是学习 sql array 函数. 本文基于 Spark 3.1.1.

## 目录
{: .no_toc .text-delta}

1. TOC
{:toc}

## array

### array 的数据类型

Spark 底层使用 ArrayData 表示一个 array. ArrayData 有三个子例分别为

![functions](/docs/spark/sql-functions/array/functions.svg)

- GenericArrayData 数据保存在 JVM 堆中
- UnsafeArrayData 数据保存在 Unsafe Memory 非 JVM 堆
- ColumnarArray 是 ColumnVector 的 Wrapper, 数据保存在 ColumnVector中

### 创建 array

- 创建一维 array

``` scala
df = df.withColumn("arr_of_num_class", array("number", "class")) //arr_of_num_class包含 number, class 列
```

对应的 Catalyst 实现为

``` scala
CreateArray(Seq(BoundReference(0, IntegerType, true), BoundReference(1, IntegerType, true)))
```

Codegen 代码为

``` java
ArrayData arrayData_1 = ArrayData.allocateArrayData(4, 2L, " createArray failed."); //生成 ArrayData
boolean isNull_4 = i.isNullAt(0); // i 为 InternalRow
int value_4 = isNull_4 ?  -1 : (i.getInt(0));
if (isNull_4) { arrayData_1.setNullAt(0);
} else { arrayData_1.setInt(0, value_4); }
boolean isNull_5 = i.isNullAt(1);
int value_5 = isNull_5 ?  -1 : (i.getInt(1));
if (isNull_5) {  arrayData_1.setNullAt(1);
} else { arrayData_1.setInt(1, value_5);
}
```

- 创建二维 array

``` scala
df = df.withColumn("arr_of_num_class", array("number", "class"))
  .withColumn("arr_of_class", array("class"))
  .withColumn("arr_of_arr", array("arr_of_num_class", "arr_of_class")) //创建二维数组
```

对应的 Catalyst 实现为

``` scala
val expr1 = CreateArray(Seq(BoundReference(0, IntegerType, true)))
val expr2 = CreateArray(Seq(BoundReference(1, IntegerType, true)))
val arrayOfArray = CreateArray(Seq(expr1, expr2))
```

Codegen 代码为

``` java
ArrayData arrayData_2 = ArrayData.allocateArrayData(-1, 2L, " createArray failed.");// 二维数组

ArrayData arrayData_3 = ArrayData.allocateArrayData(4, 1L, " createArray failed."); // InternalRow 第一列数据生成一维数组
boolean isNull_7 = i.isNullAt(0);
int value_7 = isNull_7 ?  -1 : (i.getInt(0));
if (isNull_7) {  arrayData_3.setNullAt(0); } else { arrayData_3.setInt(0, value_7);  }
arrayData_2.update(0, arrayData_3); //更新二维数组第一列

ArrayData arrayData_4 = ArrayData.allocateArrayData(
    4, 1L, " createArray failed.");
boolean isNull_9 = i.isNullAt(1);
int value_9 = isNull_9 ?  -1 : (i.getInt(1));
if (isNull_9) {  arrayData_4.setNullAt(0); } else {  arrayData_4.setInt(0, value_9);  }

arrayData_2.update(1, arrayData_4); //更新二维数组第二列
```

## array_contains
## array_max/array_min
## array_distinct
## array_sort
## array_remove
## array_intersect
## array_union
## array_except
## array_position
## array_join