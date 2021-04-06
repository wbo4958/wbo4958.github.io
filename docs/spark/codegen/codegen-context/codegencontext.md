---
layout: page
title: Codegen生成类元素
nav_order: 5
parent: Codegen
grand_parent: Spark
---

# Spark CodeGen Context
{: .no_toc}

本文主要是学习 CodegenContext 在生成代码的过程中，所涉及到的相关变量以及意义, 本文基于 Spark 3.1.1.

## 目录
{: .no_toc .text-delta}

1. TOC
{:toc}

## Background

- ExprValue

  表示一个 java 表达式， 变量名与变量的 java 类型.

  ![codegen-exprvalue](/docs/spark/codegen/codegen-context/codegen-ExprValue.svg)

- ExprCode
  
  ``` scala
  case class ExprCode(var code: Block, var isNull: ExprValue, var value: ExprValue)
  ```

  ExprCode 用于给定 InternalRow 时 计算 Expression 的Java源代码
  - code: 表示生成的代码用于计算 expression
  - isNull: expression 计算是否为null
  - value: expression 的计算结果

- CodeBlock

  ``` scala
  case class CodeBlock(codeParts: Seq[String], blockInputs: Seq[JavaCode]) 
    extends Block
  ```

  CodeBlock 表示一个生成的 java 代码块.

  如,

  ``` scala
  val isNull = JavaCode.isNullVariable("expr1_isNull") // VariableValue
  val stringLiteral = "false"
  code"boolean $isNull = $stringLiteral;" //生成 CodeBlock 代码
  ```

  **code"boolean $isNull = $stringLiteral;"** 会被重新写为

  ``` scala
  new BlockHelper(new StringContext("boolean", "=", ";"))
    .code(isNull, stringLiteral)
  ```

  最后生成 CodeBlock

  CodeBlock | |
  -----|---|
  codeParts: Seq[String] | 表示要生成代码的 code 模板. 如["boolean", "= false;"] |
  blockInputs | 生成代码的变量, 如 [VariableValue("expr1_isNull, javaType="Boolean")]

  CodeBlock 的最终代码生成如下.

  ``` scala
  override lazy val code: String = {
    val strings = codeParts.iterator
    val inputs = blockInputs.iterator
    val buf = new StringBuilder(Block.CODE_BLOCK_BUFFER_LENGTH)
    buf.append(strings.next)
    while (strings.hasNext) { //交替的组装 codePart 与 blockInput
      buf.append(inputs.next)
      buf.append(strings.next)
    }
    buf.toString
  }
  ```

  最终生成的结果为 `boolean expr1_isNull = false;`
  
## CodegenContext

CodegenContext 是 codegen 的上下文, 用于保存生成的类的成员变量，函数，内部类等等.

| 相关变量 | |
---|---|
**INPUT_ROW**  | 保存当前 operator input row 的变量名称, 和 BoundReference 一起生成相关代码. 默认名称为 "i"
**currentVars**: Seq[ExprCode] | 一组生成的列作为当前operator的输入. 当 currentVars 不为空时, BoundReference和 currentVars使用，反之 BoundReference和 INPUT_ROW连用

- 生成变量名

  为了防止出现变量名相同的情况, CodegenContext 使用下面的变量来记录

  | 相关变量 | |
  ---|---|
  **freshNameIds**: mutable.HashMap[String, Int] | 保存相同变量名与其对应的个数
  **freshNamePrefix** | 变量名前缀

  ``` scala
  def freshName(name: String): String = synchronized {
    val fullName = if (freshNamePrefix == "") {
      name
    } else {
      s"${freshNamePrefix}_$name"
    }
    val id = freshNameIds.getOrElse(fullName, 0)
    freshNameIds(fullName) = id + 1 
    s"${fullName}_$id" // 变量 + id, 则不会出现同名的了变量名了
  }
  ```

- freshVariable
  
  ``` scala
  def freshVariable(name: String, dt: DataType): VariableValue =
    JavaCode.variable(freshName(name), dt)

  case class VariableValue(variableName: String, javaType: Class[_]) extends ExprValue {
    override def code: String = variableName
  }  
  ```
  
  创建一个变量名唯一的 local VariableValue. VariableValue 保存变量名与它对应的 java 类型. 如

  ``` scala
  val x = VariableValue("count", IntegerType)
  println(s"$x") //输出  count_0
  ```

### 类的构造函数参数

| 相关变量 | |
---|---|
**references**: mutable.ArrayBuffer[Any] | 生成的类中的构造函数中的参数. 引用到外部的对象

CodegenContext 通过 addReferenceObj 将外部的对象加入到 references 中. 然后通过 `clazz.generate(ctx.references.toArray)` 传递给生成类的构造函数.

### 生成的类的成员变量

- addMutableState

往 class 中加入private成员变量, 对于 primitive 类型，直接单独写成一行 (inline?, 这样性能更好?), 对于其它的类型， 定义成数组类型.

| 相关变量 | |
---|---|
**inlinedMutableStates**: mutable.ArrayBuffer[(String, String)] | 保存类中的private字段(类型，成员变量名), 如 ("int", "count"), 将会生成 private int count;
**arrayCompactedMutableStates**: mutable.Map[String, MutableStateArrays] | 相同类型的成员变量定义放到了一个数组中. key 为 java type, MutableStateArrays为成员变量定义
**mutableStateNames**: mutable.HashSet[String] | class 中的成员变量名, 保证命名正确
**mutableStateInitCode**: mutable.ArrayBuffer[String] | class 成员变量的初始化代码
**immutableStates**: mutable.Map[String, (String, String)] |

如

``` scala
// primitive type 加入到 inlinedMutableStates
ctx.addMutableState(CodeGenerator.JAVA_INT, "count", v => s"$v = 10")
ctx.addMutableState(CodeGenerator.JAVA_BOOLEAN, "isApple", v => s"$v = true")
// 非 primitive 加入到 arrayCompactedMutableStates
ctx.addMutableState("Foo", "foo", v => s"$v = new Foo()")
ctx.addMutableState("Foo", "foo_1", v => s"$v = new Foo()")
ctx.addMutableState("Bar", "bar", v => s"$v = new Bar()")
```

- declareMutableStates

ctx.declareMutableStates 打印声明的成员变量

``` java
private int count_0;
private boolean isApple_0;
private Foo[] mutableStateArray_0 = new Foo[2];
private Bar[] mutableStateArray_1 = new Bar[1];
```

- initMutableStates

ctx.initMutableStates 打印成员变量初始化代码

``` java
count_0 = 10
isApple_0 = true
mutableStateArray_0[0] = new Foo()
mutableStateArray_0[1] = new Foo()
mutableStateArray_1[0] = new Bar()
```

### 处理 partition id 代码

| 相关变量 | |
---|---|
partitionInitializationStatements: mutable.ArrayBuffer[String] | 保存对 partition id操作的初始化的代码.

有些 Expression 需要 partitionId 进行一些操作。 如 MonotonicallyIncreasingID, Rand 等.

``` scala
ctx.addPartitionInitializationStatement(
    s"partitionMaskTerm = ((long) partitionIndex) << 33;")
```

- initPartition

ctx.initPartition 生成 partitionIdx 相关的代码

``` java
partitionMaskTerm = ((long) partitionIndex) << 33;
```

### 生成 inner class

- addInnerClass

| 相关变量 | |
---|---|
extraClasses: mutable.ListBuffer[String] | 保存 inner class代码

``` scala
ctx.addInnerClass(
  s"""
     |public class DummyClass {
     |  public dummy() {}
     |}
       """.stripMargin)
```

- emitExtraCode

生成 inner class 代码

``` java
public class HelloWorld {
  public dummy() {}
}
```

### 生成类的函数

| 相关变量 | |
---|---|
**classes**: mutable.ListBuffer[(String, String)] = mutable.ListBuffer[(String, String)](outerClassName -> null) | 保存所有的的类的信息, key为class name. value为对应的 class 实例. 初始化为 "OuterClass" -> null. 表示最外层的类, 它包含其它所有的内部子类
**classSize**: mutable.Map[String, Int] = mutable.Map[String, Int](outerClassName -> 0) | class name -> class size, 默认 "OuterClass" -> 0
**classFunctions**: mutable.Map[String, mutable.Map[String, String]] = mutable.Map(outerClassName -> mutable.Map.empty[String, String]) | 保存 class 与 class中的函数信息. Map[ClassName, Map[Function Name, Function code]]
**currClassSize**() | 最新增加的 class 的size
**currClass**(): (String, String) | 最新加的 class name 与 class instance

- addNewFunction

GENERATED_CLASS_SIZE_THRESHOLD 默认为 1000000, 当 class size 大于 该值时，会生成 NestedClass, 如下所示，当把 GENERATED_CLASS_SIZE_THRESHOLD 设置为 1 时,

``` scala
ctx.addNewFunction("sayHello1",
  """void sayHello1() {
    | System.out.println("say hello")
    |}
    |""".stripMargin)
ctx.addNewFunction("sayHello2",
  """void sayHello2() {
    | System.out.println("say hello");
    |}
    |""".stripMargin)
ctx.addNewFunction("sayHello3",
  """void sayHello3() {
    | System.out.println("say hello");
    |}
    |""".stripMargin)
```

- declareAddedFunctions

生成如下代码

``` java
// 最外层的 function
void sayHello1() {
 System.out.println("say hello")
}

private NestedClass_1 nestedClassInstance_1 = new NestedClass_1();
private NestedClass_0 nestedClassInstance_0 = new NestedClass_0();
// inner class
private class NestedClass_1 {
  void sayHello3() {
    System.out.println("say hello")
  }
}
// inner class
private class NestedClass_0 {
  void sayHello2() {
    System.out.println("say hello")
  }
}
```
