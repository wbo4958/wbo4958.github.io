---
layout: page
title: func-array
nav_order: 10000
parent: sql
grand_parent: spark
---

# array
{: .no_toc}

本文主要是学习 sql array 函数. 本文基于 Spark 3.1.1.

## 目录
{: .no_toc .text-delta}

1. TOC
{:toc}

## array

array 的数据类型

Spark 底层使用 ArrayData 表示一个 array. ArrayData 有三个子类分别为

![functions](/docs/spark/sql/functions/array/functions.svg)

- GenericArrayData 数据保存在 JVM 堆中
- UnsafeArrayData 数据保存在 Unsafe Memory 非 JVM 堆
- ColumnarArray 是 ColumnVector 的 Wrapper, 数据保存在 ColumnVector中

### 创建 array

- **创建一维 array**

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

- **创建二维 array**

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

### array_contains

检查 Array 中是否包含 value

- 一维数组

  ``` scala
  df = df.withColumn("arr_of_num_class", array("number", "class"))
          .withColumn("arr_of_class_contains_2", array_contains('arr_of_num_class, 2))
  ```

  catalyst

  ``` scala
  val expr = CreateArray(Seq(Literal(1), Literal(2)))
  var contains = ArrayContains(expr, 9)
  ```

  codegen

  ``` java
  for (int i_1 = 0; i_1 < arrayData_0.numElements(); i_1 ++) {
    if (9 == arrayData_0.getInt(i_1)) { // 依次遍历 arrayData 数据，查找是否有 1
      value_0 = true;
      break;
    }
  }
  ```

- 二维数组

  ``` scala
  df = df.withColumn("arr_of_num_class", array("number", "class"))
          .withColumn("arr_of_class", array("class"))
          .withColumn("arr_of_arr", array("arr_of_num_class", "arr_of_class"))
          .withColumn("arr_of_arr_num_contains", array_contains('arr_of_arr, Array(1001, 2)))
  ```

  catalyst

  ``` scala
  val expr1 = CreateArray(Seq(BoundReference(0, IntegerType, true)))
  val expr2 = CreateArray(Seq(BoundReference(1, IntegerType, true)))
  val arrayOfArray = CreateArray(Seq(expr1, expr2))
  var arrOfArrContains = ArrayContains(arrayOfArray, Literal.create(Array(9)))
  ```

  codegen 代码

  ``` java
  for (int i_1 = 0; i_1 < arrayData_0.numElements(); i_1 ++) {
    if (compareArray_0(((ArrayData) references[0] /* literal */), arrayData_0.getArray(i_1)) == 0) {
      value_0 = true;
      break;
    }
  }
  ```

  references[0] 是传入的需要比较的数组 Array(9). compareArray_0 函数请参考 [genComp](/docs/spark/sql/codegen/codegen-context/codegencontext2.html#gencomp)

### array_max/array_min

- 一维数组

``` java
boolean isNull_0 = true; //保证 value_0 第一次被 assign 给 arrayData_0.getInt(0)
int value_0 = -1;
for (int i_1 = 0; i_1 < arrayData_0.numElements(); i_1 ++) {
  if (!(arrayData_0.isNullAt(i_1)) && (isNull_0 ||
    (arrayData_0.getInt(i_1)) > value_0)) {
    isNull_0 = false;
    value_0 = (arrayData_0.getInt(i_1));
  }
}
```

- 二维数组

``` java
boolean isNull_0 = true;  //保证 value_0 第一次被 assign 给 arrayData_0.getInt(0)
ArrayData value_0 = null; //保存最大值
for (int i_1 = 0; i_1 < arrayData_0.numElements(); i_1 ++) {
  if (!(arrayData_0.isNullAt(i_1)) && (isNull_0 ||
    (compareArray_0((arrayData_0.getArray(i_1)), value_0)) > 0)) {
    isNull_0 = false;
    value_0 = (arrayData_0.getArray(i_1)); 
  }
}
```

其它的 Array 操作也是基于 ArrayData 数据结构, 百变不离其中.