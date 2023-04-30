---
layout: page
title: Encoder
nav_order: 700 
parent: sql
grand_parent: spark 
---

# Spark Encoder
{: .no_toc}

本文通过代码学习 Spark 中 Encoder. 本文基于 Spark 3.1.1.

## 目录
{: .no_toc .text-delta}

1. TOC
{:toc}

## Sample code

``` scala
case class Person(name: String, age: Int)

def test = {
    ...
    val data = Seq(Person("Michael", 29), Person("Andy", 30), Person("Justin", 23))
    val personEncoder = Encoders.product[Person]
    val ds = spark.createDataset(data)(personEncoder)
    val persons = ds.collect()
    persons.map(p => println("name:" + p.name + " age:" + p.age))
}
```

上面的例子很简单，生成一组 Person JVM对象并放入 Seq 序列中，然后生成 Dataset. Spark Catalyst 中的
Expressions 以及各种 PhysicalPlans 处理的对象是 InternalRow, 而如何能将 JVM 数据转换为 InternalRow
呢？ 当 Spark 计算完后又是如何将 InternalRow 数据转换为 JVM 对象供后续操作呢?

## Encoder

Spark 在 1.6.0 中加入了 Encoder 用于将 JVM 对象转换为 InternalRow, 以及将 InternalRow 转换为 JVM
对象. Spark 将常用的 Encoder 隐式的声明在 SQLImplicits 中，用户甚至不用显示的调用 Encoder 函数.

本例通过 `val personEncoder = Encoders.product[Person]` 显示的初始化一个 Encoder, 用于了解
Encoder的内部工作原理.

``` scala
def product[T <: Product : TypeTag]: Encoder[T] = ExpressionEncoder()

def apply[T : TypeTag](): ExpressionEncoder[T] = {
  val mirror = ScalaReflection.mirror //获得 Mirror 用于runtime 反射
  val tpe = typeTag[T].in(mirror).tpe //通过 typeTag 获得 T 的类型信息, 

  val cls = mirror.runtimeClass(tpe) //获得 T 的 runtime class 类型
  val serializer = ScalaReflection.serializerForType(tpe) // Expression JVM->InternalRow
  val deserializer = ScalaReflection.deserializerForType(tpe) // Expression InternalRow->JVM

  new ExpressionEncoder[T]( //生成 T 的 ExpressionEncoder
    serializer,
    deserializer,
    ClassTag[T](cls))
} 
```

得到的 ExpressionEncoder 如下图所示,

![ExpressionEncoder](/docs/spark/sql/encoder/encoder.svg)

### objSerializer

objSerializer 用于将 JVM 对象转换为 InternalRow. 它是一个 IF 表达示,

判断条件是如果 Person 为 NULL, 则返回一个 Literal(null), 类型为
`StructType(StructField("name", StringType), StructField("age", IntegerType))`, 

如果 Person 不为 NULL, 则创建一个 `CreateNameStruct`

CreateNamedStruct 的 eval 函数如下，本质上将 valExprs 计算的值保存到 InternalRow 对应的列中.

``` scala
  override def eval(input: InternalRow): Any = {
    InternalRow(valExprs.map(_.eval(input)): _*)
  }
```

所以 objSerializer 的目的是将 JVM 对象中的各个成员变量抽取出来依次放入到 InternalRow 对应的列中.

### objDeserializer

objDeserializer 与 objSerializer 作用相反，用于将 InternalRow 转换为 JVM 对象，它也是一个 IF表达示,
objDeserializer 与 objSerializer 不同的是 objSerializer 不需要解析，而 objDeserializer 需要先解析
然后与 Dataset 的output Attributes 进行绑定.

objDeserializer 订是获得 InternalRow(0) 也就是 String 与 InternalRow(1) 也就是 Integer, 然后生成
Person 对象.

## 转换JVM对象到InternalRow

在创建Dataset时会将 JVM 的数据先转换为 InternalRow 的数据.

``` scala
def createDataset[T : Encoder](data: Seq[T]): Dataset[T] = {
  val enc = encoderFor[T] //获得 T 类型的 Encoder
  val toRow = enc.createSerializer() // 创建高阶函数用于将T的JVM对象转换为InternalRow
  val attributes = enc.schema.toAttributes // 获得 schema
  val encoded = data.map(d => toRow(d).copy()) //将 JVM 对象转换为 InternalRow
  val plan = new LocalRelation(attributes, encoded)
  Dataset[T](self, plan)
}
```

``` scala
def createSerializer(): Serializer[T] = new Serializer[T](optimizedSerializer)

class Serializer[T](private val expressions: Seq[Expression])
  extends (T => InternalRow) with Serializable {
  @transient
  private[this] var inputRow: GenericInternalRow = _
  @transient
  private[this] var extractProjection: UnsafeProjection = _
  override def apply(t: T): InternalRow = try {
    if (extractProjection == null) {
      inputRow = new GenericInternalRow(1)
      extractProjection = GenerateUnsafeProjection.generate(expressions)
    }
    inputRow(0) = t // 将 Person对象保存到 InternalRow(0) 中
    extractProjection(inputRow) // 将 Persion 字段抽取出来放到 InternalRow 对应的列中
  } catch {
      ...
  }
}
```

createSerializer 会通过 `GenerateUnsafeProjection.generate` 生成代码用于将 Person 字段
抽取出来保存到 InternalRow, 生成的代码如下所示,

``` java
class SpecificUnsafeProjection extends org.apache.spark.sql.catalyst.expressions.UnsafeProjection {
  private Object[] references;
  private boolean resultIsNull_0;
  private boolean globalIsNull_0;
  private java.lang.String[] mutableStateArray_0 = new java.lang.String[1];
  private org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter[] mutableStateArray_1 =
      new org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter[1];

  public SpecificUnsafeProjection(Object[] references) {
    this.references = references;
    mutableStateArray_1[0] = new org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter(2, 32);
  }

  public void initialize(int partitionIndex) {
  }

  // Scala.Function1 need this
  public java.lang.Object apply(java.lang.Object row) {
    return apply((InternalRow) row);
  }

  public UnsafeRow apply(InternalRow i) {
    mutableStateArray_1[0].reset();
    mutableStateArray_1[0].zeroOutNullBytes();

    UTF8String value_5 = StaticInvoke_0(i); //获得 name 对应的值
    if (globalIsNull_0) {
      mutableStateArray_1[0].setNullAt(0);
    } else {
      mutableStateArray_1[0].write(0, value_5); //将name写入到 UnsafeRow(0)
    }

    boolean isNull_8 = i.isNullAt(0);
    learning.Person value_9 = isNull_8 ? null : ((learning.Person)i.get(0, null)); //获得Person对象
    if (isNull_8) { //看来 Person 对象不能为null
      throw new NullPointerException(((java.lang.String) references[1] /* errMsg */));
    }
    boolean isNull_5 = true;
    int value_6 = -1;
    if (!false) {
      isNull_5 = false;
      if (!isNull_5) {
        value_6 = value_9.age(); //获得 age
      }
    }
    mutableStateArray_1[0].write(1, value_6); //写入age到UnsafeRow(1)
    return (mutableStateArray_1[0].getRow());
  }

  // 获得 Persion.name 的值
  private UTF8String StaticInvoke_0(InternalRow i) {
    resultIsNull_0 = false;
    if (!resultIsNull_0) {
      boolean isNull_4 = i.isNullAt(0); //检测 Persion 是否为 null
      learning.Person value_4 = isNull_4 ?  null : ((learning.Person)i.get(0, null));
      if (isNull_4) {
        throw new NullPointerException(((java.lang.String) references[0] /* errMsg */));
      }
      boolean isNull_1 = true;
      java.lang.String value_1 = null;
      if (!false) {
        isNull_1 = false;
        if (!isNull_1) {
          Object funcResult_0 = null;
          funcResult_0 = value_4.name();
          if (funcResult_0 != null) {
            value_1 = (java.lang.String) funcResult_0;
          } else {
            isNull_1 = true;
          }
        }
      }
      resultIsNull_0 = isNull_1;
      mutableStateArray_0[0] = value_1;
    }

    boolean isNull_0 = resultIsNull_0;
    UTF8String value_0 = null;
    if (!resultIsNull_0) {
      // 通过 UTF8String.fromString 获得name的值
      value_0 = org.apache.spark.unsafe.types.UTF8String.fromString(mutableStateArray_0[0]);
    }
    globalIsNull_0 = isNull_0;
    return value_0;
  }
}
```

## 将 InternalRow 转换为 JVM 对象

``` scala
private lazy val resolvedEnc = {
  // 解析与绑定 ExpressionEncoder, 主要是解析与绑定 objDeserializer
  exprEnc.resolveAndBind(logicalPlan.output, sparkSession.sessionState.analyzer)
}
  
def collect(): Array[T] = withAction("collect", queryExecution)(collectFromPlan)

private def collectFromPlan(plan: SparkPlan): Array[T] = {
  val fromRow = resolvedEnc.createDeserializer() // 创建function: InternalRow -> JVM 对象
  plan.executeCollect().map(fromRow) // 对collect回来的 InternalRow 转换为 JVM 对象
}
```

Dataset 将 encoder 进行解析并与 output 的 Attribute 进行绑定, 最后生成的代码如下所示

``` java
class SpecificSafeProjection extends org.apache.spark.sql.catalyst.expressions.codegen.BaseProjection {

  private Object[] references;
  private InternalRow mutableRow;
  private int argValue_0;
  private java.lang.String[] mutableStateArray_0 = new java.lang.String[1];

  public SpecificSafeProjection(Object[] references) {
    this.references = references;
    mutableRow = (InternalRow) references[references.length - 1];
  }

  public void initialize(int partitionIndex) {
  }

  public java.lang.Object apply(java.lang.Object _i) {
    InternalRow i = (InternalRow) _i;
    boolean isNull_2 = i.isNullAt(0);
    UTF8String value_2 = isNull_2 ? null : (i.getUTF8String(0)); //获得第一列数据
    boolean isNull_1 = true;
    java.lang.String value_1 = null;
    if (!isNull_2) {
      isNull_1 = false;
      if (!isNull_1) {
        Object funcResult_0 = null;
        funcResult_0 = value_2.toString();
        value_1 = (java.lang.String) funcResult_0;
      }
    }
    mutableStateArray_0[0] = value_1;
    int value_4 = i.getInt(1); //获得 age
    argValue_0 = value_4;

    // 生成 Person 对象
    final learning.Person value_0 = false ? null : new learning.Person(mutableStateArray_0[0], argValue_0);
    mutableRow.update(0, value_0); //保存在 InternalRow(0) 中
    return mutableRow;
  }
}
```
