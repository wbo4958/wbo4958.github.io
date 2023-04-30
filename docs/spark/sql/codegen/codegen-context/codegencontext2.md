---
layout: page
title: CodegenContext相关函数
nav_order: 10
parent: Codegen
grand_parent: Spark
---

# Spark CodeGen Context 辅助函数
{: .no_toc}

本文学习 CodegenContext 用于生成代码的几个函数。 本文基于 Spark 3.1.1.

## 目录
{: .no_toc .text-delta}

1. TOC
{:toc}

## genEqual

生成 equal expression 的代码

``` scala
def genEqual(dataType: DataType, c1: String, c2: String) // 
```

genEqual 根据 datatype 不同，生成的 "equal" 代码也不相同

| data type | generated code |
|---|---|
|genEqual(BooleanType, "true", "false") | true == false
|genEqual(FloatType, "1", "2")          | ((java.lang.Float.isNaN(1) && java.lang.Float.isNaN(2)) \|\| 1 == 2)  
|genEqual(DoubleType, "1", "2")         | ((java.lang.Double.isNaN(1) && java.lang.Double.isNaN(2)) \|\| 1 == 2)
|genEqual(IntegerType, "1", "2")        | 1 == 2
|genEqual(BinaryType, "c1", "c2")       | java.util.Arrays.equals(c1, c2)
|genEqual(StringType, "c1", "c2")       | c1.equals(c2)
|genEqual(NullType, "c1", "c2")         | false

对于 StructType 和 ArrayType 调用 getComp 生成代码.

## genComp

``` scala
def genComp(dataType: DataType, c1: String, c2: String): String
```

生成两个 expression 比较的代码, 如果返回值为 0， 表示值相同, 返回值 < 0, 表示 c1 < c2, 如果返回值 > 0, 表示 c1 > c2

| data type | generated code |
|----|----|
|genComp(BooleanType, "true", "false")) | (true == false ? 0 : (true ? 1 : -1))
|genComp(FloatType, "1", "2"))          | org.apache.spark.sql.catalyst.util.SQLOrderingUtil.compareFloats(1, 2)
|genComp(DoubleType, "1", "2"))         | org.apache.spark.sql.catalyst.util.SQLOrderingUtil.compareDoubles(1, 2)
|genComp(IntegerType, "1", "2"))        | (1 > 2 ? 1 : 1 < 2 ? -1 : 0)
|genComp(BinaryType, "c1", "c2"))       | org.apache.spark.sql.catalyst.util.TypeUtils.compareBinary(c1, c2)
|genComp(StringType, "c1", "c2"))       | c1.compare(c2)
|genComp(NullType, "c1", "c2"))         | 0

1. 对于 StructType

    ``` scala
    // 生成代码比较 s1, s2 InternalRow 里的 col1 和 col2 列
    val schema = StructType(Seq(StructField("col1", IntegerType), StructField("col2", FloatType)))
    genComp(schema, "s1", "s2")
    ```

    生成的代码如下

    ``` java
    compareStruct_0(s1, s2)
    public int compareStruct_0(InternalRow a, InternalRow b) {
      // when comparing unsafe rows, try equals first as it compares the binary directly
      // which is very fast.
      if (a instanceof UnsafeRow && b instanceof UnsafeRow && a.equals(b)) {
        return 0;
      }
      // 先比较第一列
      boolean isNull_0 = a.isNullAt(0);
      int value_0 = isNull_0 ? -1 : (a.getInt(0));
      boolean isNull_2 = b.isNullAt(0);
      int value_2 = isNull_2 ?  -1 : (b.getInt(0));
      if (isNull_0 && isNull_2) {
        // Nothing
      } else if (isNull_0) {
        return -1;
      } else if (isNull_2) {
        return 1;
      } else {
        int comp = (value_0 > value_2 ? 1 : value_0 < value_2 ? -1 : 0);
        if (comp != 0) {
          return comp;
        }
      }
      // 再比较第二列
      boolean isNull_1 = a.isNullAt(1);
      float value_1 = isNull_1 ?  -1.0f : (a.getFloat(1));
      boolean isNull_3 = b.isNullAt(1);
      float value_3 = isNull_3 ?  -1.0f : (b.getFloat(1));
      if (isNull_1 && isNull_3) {
        // Nothing
      } else if (isNull_1) {
        return -1;
      } else if (isNull_3) {
        return 1;
      } else {
        int comp = org.apache.spark.sql.catalyst.util.SQLOrderingUtil.compareFloats(value_1, value_3);
        if (comp != 0) {
          return comp;
        }
      }
      return 0;
    }
    ```

2. 对于 ArrayType

    生成数据比较两个 ArrayData

    ``` scala
    compareArray_0(a1, a2)

    public int compareArray_0(ArrayData a, ArrayData b) {
      // when comparing unsafe arrays, try equals first as it compares the binary directly
      // which is very fast.
      if (a instanceof UnsafeArrayData && b instanceof UnsafeArrayData && a.equals(b)) {
        return 0;
      }
      int lengthA = a.numElements();
      int lengthB = b.numElements();
      int minLength_0 = (lengthA > lengthB) ? lengthB : lengthA;
      for (int i = 0; i < minLength_0; i++) {
        boolean isNullA_0 = a.isNullAt(i);
        boolean isNullB_0 = b.isNullAt(i);
        if (isNullA_0 && isNullB_0) {
          // Nothing
        } else if (isNullA_0) {
          return -1;
        } else if (isNullB_0) {
          return 1;
        } else {
          int elementA_0 = a.getInt(i);
          int elementB_0 = b.getInt(i);
          int comp = (elementA_0 > elementB_0 ? 1 : elementA_0 < elementB_0 ? -1 : 0);
          if (comp != 0) {
            return comp;
          }
        }
      }
      if (lengthA < lengthB) {
        return -1;
      } else if (lengthA > lengthB) {
        return 1;
      }
      return 0;
    }
    ```

## genCreater

``` scala
def genGreater(dataType: DataType, c1: String, c2: String)
```

生成数据大于比较的代码

- 对于 byte/short/int/long, 生成代码 `c1 > c2`
- 对于其它类型, 调用 genComp 生成相关代码

## genExpressions

``` scala
def generateExpressions(
    expressions: Seq[Expression],
    doSubexpressionElimination: Boolean = false): Seq[ExprCode] {}
  if (doSubexpressionElimination) subexpressionElimination(expressions)
  expressions.map(e => e.genCode(this))
```

generateExpressions 函数为 Expressions 生成代码. doSubexpressionElimination用于控制是否查找相同的表达式， 然后为相同的表达式生成函数，这样可以避免生成重复的代码，且减少运行时计算量. ubexpressionElimination 函数主要是通过 EquivalentExpressions.addExprTree 来查找相同满足条件的相同的表达式，并保存到 equivalenceMap 中.

``` scala
def addExprTree(
    expr: Expression,
    addFunc: Expression => Boolean = addExpr): Unit = {
  // skip 掉 LeafExpression/LambdaVariable 以及 PlanExpression 等
  val skip = expr.isInstanceOf[LeafExpression] ||
    // `LambdaVariable` is usually used as a loop variable, which can't be evaluated ahead of the
    // loop. So we can't evaluate sub-expressions containing `LambdaVariable` at the beginning.
    expr.find(_.isInstanceOf[LambdaVariable]).isDefined ||
    // `PlanExpression` wraps query plan. To compare query plans of `PlanExpression` on executor,
    // can cause error like NPE.
    (expr.isInstanceOf[PlanExpression[_]] && TaskContext.get != null)
  if (!skip && !addFunc(expr)) {
    childrenToRecurse(expr).foreach(addExprTree(_, addFunc))
    commonChildrenToRecurse(expr).filter(_.nonEmpty).foreach(addCommonExprs(_, addFunc))
  }
}
```

紧接着 subexpressionElimination 为找到的 common expression 生成Expression代码并生成相关的成员函数代码.
下面的例子

``` scala
val ref = BoundReference(0, IntegerType, true)
val ref1 = BoundReference(1, IntegerType, true)
val add1 = Add(ref, ref1) // 第一列与第二列相加
val add2 = Add(add1, ref1) // 第一列与第二列相加后再与第二列相加
val ctx = new CodegenContext
ctx.generateExpressions(Seq(add2, add1), doSubexpressionElimination = true)
```

对于上面的代码， add2 包括了 add1, 如果 doSubexpressionElimination = true 时, add1 将会被生成函数，最终的值保存到类的成员变量中

1. doSubexpressionElimination = true 时

   生成如下代码

   - 生成 成员变量

    ``` java
    // 生成类中的 成员变量
    private boolean subExprIsNull_0; // add1 的结果是否为 null
    private int subExprValue_0; // 保存 add1 的结果
    ```

   - 生成计算 add1 的代码

    ``` java
    // 生成计算 add1 的成员函数
    private void subExpr_0(InternalRow i) {
      boolean isNull_0 = true;
      int value_0 = -1;
      boolean isNull_1 = i.isNullAt(0); 
      int value_1 = isNull_1 ? -1 : (i.getInt(0)); // 获取第一列数据
      if (!isNull_1) {
        boolean isNull_2 = i.isNullAt(1);
        int value_2 = isNull_2 ? -1 : (i.getInt(1)); // 获取第二列数据
        if (!isNull_2) {
          isNull_0 = false; // resultCode could change nullability.
          value_0 = value_1 + value_2; //第一列加上第二列的结果
        }
      }
      subExprIsNull_0 = isNull_0;
      subExprValue_0 = value_0; //第一列加上第二列的结果保存到 subExprValue_0
    }
    ```

   - 生成 add2 的代码

    ``` java
    boolean isNull_3 = true;
    int value_3 = -1;
    if (!subExprIsNull_0) { // add1 结果是否为 null
      boolean isNull_4 = i.isNullAt(1);
      int value_4 = isNull_4 ? -1 : (i.getInt(1)); //获取第二列的数据
      if (!isNull_4) {
        isNull_3 = false; // resultCode could change nullability.
        value_3 = subExprValue_0 + value_4; // add1 的结果 + 第二列的数据 最终为 add2的结果.
      }
    }
    ```

2. doSubexpressionElimination = false

   生成的代码如下

   ``` java
    boolean isNull_0 = true;
    int value_0 = -1;
    boolean isNull_1 = true;
    int value_1 = -1;
    boolean isNull_2 = i.isNullAt(0);
    int value_2 = isNull_2 ? -1 : (i.getInt(0));
    if (!isNull_2) {
      boolean isNull_3 = i.isNullAt(1);
      int value_3 = isNull_3 ?  -1 : (i.getInt(1));
      if (!isNull_3) {
        isNull_1 = false; // resultCode could change nullability.
        value_1 = value_2 + value_3;
      }
    }
    if (!isNull_1) {
      boolean isNull_4 = i.isNullAt(1);
      int value_4 = isNull_4 ?  -1 : (i.getInt(1));
      if (!isNull_4) {
        isNull_0 = false; // resultCode could change nullability.
        value_0 = value_1 + value_4; // 获得 add1 的计算结果(第一列+第二列+第三列)
      }
    }

    boolean isNull_5 = true;
    int value_5 = -1;
    boolean isNull_6 = i.isNullAt(0);
    int value_6 = isNull_6 ? -1 : (i.getInt(0));
    if (!isNull_6) {
      boolean isNull_7 = i.isNullAt(1);
      int value_7 = isNull_7 ? -1 : (i.getInt(1));
      if (!isNull_7) {
        isNull_5 = false; // resultCode could change nullability.
        value_5 = value_6 + value_7; //获得 add1的计算结果
      }
    }
   ```
