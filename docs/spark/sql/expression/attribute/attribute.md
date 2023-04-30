---
layout: page
title: Attribute
nav_order: 5
parent: Expression
grand_parent: Spark
---

# Attribute
{: .no_toc}

本文主要是学习 Attribute. 本文基于 Spark 3.1.1.

## 目录
{: .no_toc .text-delta}

1. TOC
{:toc}

## Attribute

Spark plan operator 有输入和输出之分. Spark plan 继承于 TreeNode, 本质上是一棵树, 因此处于树中的某个节点有 Child operators, Parent operator 之分. Child operators 的输出会作为 Current operator 的输入, 而 Current operator 的输出又会作为 Parent operator 的输入, 依此类推. Attribute 扮演的角色就是将 operator 之间的输入与输出进行关联起来.

一个 Spark operator 对应的输入和输出的 Attribute 数量并不总是一一对应的, 有些情况下会不同. 比如有些 operator 可能会减少 attribute 如 ProjectExec, 而有些 operator 会增加 attribute 如 WindowExec. Attribute 另一个作用是将 operator 中 Expression 的输出绑定到 operator 的输入.

[`Attribute is the Catalyst name for an input column from a child operator.`](http://mail-archives.apache.org/mod_mbox/spark-user/201508.mbox/%3CCAAswR-59+2Fz1HNfVAUmM1Oc_uWaHki24=vrGuB3oh7x_cUG7A@mail.gmail.com%3E) 这句话其实不太准确, Attribute 大多数会绑定到 table 的 field 或 column, 但也会绑定到 expression 的输出.

### Attribute 实现类

``` scala
abstract class Attribute extends LeafExpression with NamedExpression with NullIntolerant
```

Attribute 是一个抽象类, 因为继承于 LeafExpression, 所以没有 children 输入, Attribute 继承于 NamedExpression, 因此每个 Attribute 都会有如下属性

``` scala
def name: String
def exprId: ExprId // expression id
def qualifier: Seq[String] // optional, 包含一些限定信息如 catalog, database, table
def metadata: Metadata
```

- UnresolvedAttribute 继承于 Attribute 和 Unevaluable
  
  保存着尚未解析的属性的名称. 因为没有解析,它也就没有已知的 exprId, dataType 等等. 由于 UnresolvedAttribute 继承于 Unevaluable, 所以 UnresolvedAttribute 是不可计算的.

- AttributeReference 继承于 Attribute 和 Unevaluable

  UnresolvedAttribute 解析过后变成了 AttributeReference, 它表示引用到 operator 输出的某个属性. 既然它已经是被解析过的,那它就有确定的 dataType, nullable, exprId 等. 由于 AttributeReference 继承于 Unevaluable, 所以 AttributeReference 是不可计算的.

### AttributeSeq

AttributeSeq 是一个隐式类, 它会将 Seq[Attribute] 转换为 AttributeSeq.

``` scala
val input: AttributeSeq = Seq('a.int, 'b.int)
```

| AttributeSeq | |
|----| ---- |
| val attrs: Seq[Attribute] | |
| def toStructType: StructType | Attributes 转换为 StructType |
| val exprIdToOrdinal | Attribute exprId 与 Attribute 所在的 Seq 里的位置的映射 |
| def indexOf(exprId: ExprId): Int | 根据 exprId 找到 对应的 位置 |
| val direct: Map[String, Seq[Attribute]] | name 与 Attribute 的映射 |
| val qualified: Map[(String, String), Seq[Attribute]] | 两个限定符与 Attribute 的映射 |
| val qualified3Part: Map[(String, String, String), Seq[Attribute]] | 三个限定符与 Attribute 的映射 |
| val qualified4Part: Map[(String, String, String, String), Seq[Attribute]] |四个限定符与 Attribute 的映射|

### AttributeSet

AttributeSet 通过 LinkedHashSet 保存 AttributeEquals, 而 AttributeEquals 对 Attribute 进行 wrap 一层.

``` scala
protected class AttributeEquals(val a: Attribute) {
  //两个 Attribute 相同的,比较的是 exprId
  override def equals(other: Any): Boolean = (a, other.asInstanceOf[AttributeEquals].a) match {
    case (a1: AttributeReference, a2: AttributeReference) => a1.exprId == a2.exprId
    case (a1, a2) => a1 == a2
  }
}
```

所以可以看出 AttributeSet 其实就是 Attribute 的一个集合表达式

### Expression 中的 Attribute

Expression 中定义了 references, 表示该 expression 所引用到的 所有的 Attribute, 默认为所有的 children 的 attribute

``` scala
/**
 * Workaround scala compiler so that we can call super on lazy vals
 */
@transient
private lazy val _references: AttributeSet =
  AttributeSet.fromAttributeSets(children.map(_.references))

def references: AttributeSet = _references
```

### QueryPlan 中的 Attribute

QueryPlan 是 LogicalPlan 与 SparkPlan 的基类,它定义了与 Attribute 相关的方法如下,

|Attribute 相关方法||
|----|----|
| def output: Seq[Attribute] | Node的输出属性 |
| lazy val outputSet: AttributeSet | output 的 AttributeSet, 默认为 AttributeSet(output) |
| def inputSet: AttributeSet | Node 的 input set, 默认为 AttributeSet(children.flatMap(_.asInstanceOf[QueryPlan[PlanType]].output)) |
| def producedAttributes: AttributeSet | 该 Node 生成的 Attribute, 和 output 什么关系???? 默认为 AttributeSet.empty|
| val references: AttributeSet | Node 的 expression 中所出现的所有的 attribute. 默认为AttributeSet.fromAttributeSets(expressions.map(_.references)) -- producedAttributes |
| def missingInput: AttributeSet | 非 child 中的 attribute, 默认为references -- inputSet|

### schema 与 Attribute

Spark SQL 通过定义 schema 来处理结构化和半结构化的数据. schema 定义数据的字段、类型等. Spark SQL 通过 StructType 表示 schema. 参考[这里](https://www.waitingforcode.com/apache-spark-sql/schemas/read)

StructType 由一组 StructField 组成,每个 StructField 描述一个字段的信息

- name 字段名,列名
- dataType 字段的数据类型
- nullable 字段是否可以接收 null 型的数据
- metadata 字段的 meta 信息, metadata是一个Map[String, Any]类型, 描述字段的附加信息. 如字段的Comments

  ``` scala
  /**
   * Return the comment of this StructField.
   */
  def getComment(): Option[String] = {
    if (metadata.contains("comment")) Option(metadata.getString("comment")) else None
  }
  ```

StructType 通过 toAttribute 方法可以将 AttributeReference (而非 UnresolvedAttribute. 因为schema 的数据类型、nullable都是已知) 引用到 schema 的 field (column) 上.

``` scala
protected[sql] def toAttributes: Seq[AttributeReference] =
  map(f => AttributeReference(f.name, f.dataType, f.nullable, f.metadata)())
```

## Attribute 的解析与绑定

1. LogicalPlan 引用到数据对应的列

    ``` scala
    val df = spark.read.format("parquet").load(filePath)
    ```

    上面的代码在 v1 data reader 时通过 SparkSession.baseRelationToDataFrame 先创建一个 LogicalRelation, 然后再创建了一个 DataFrame.

    ``` scala
    def baseRelationToDataFrame(baseRelation: BaseRelation): DataFrame = {
      Dataset.ofRows(self, LogicalRelation(baseRelation))
    }
    ```

    ``` scala
    object LogicalRelation {
      def apply(relation: BaseRelation, isStreaming: Boolean = false): LogicalRelation = {
        // The v1 source may return schema containing char/varchar type. We replace char/varchar
        // with "annotated" string type here as the query engine doesn't support char/varchar yet.
        val schema = CharVarcharUtils.replaceCharVarcharWithStringInSchema(relation.schema)
        LogicalRelation(relation, schema.toAttributes, None, isStreaming)
      }
    }
    ```

    LogicalRelation 是一个 LeafNode, 它没有输入, LogicalRelation 表示的是底层数据信息, 它的 `output: Seq[AttributeReference]` 已经引用到了数据的 field.

    ![logical relation](/docs/spark/expression/attribute/expression-logicalrelation.svg)

2. 创建 UnresolvedAttribute

    ``` scala
    val df2 = df.select("name")
    ```

    上面的代码目的是从 df 中选择 name 列的数据. name 是一个字符串的列名. select 函数首先 将 "name" 转换成 Column. 如下所示

    ``` scala
    class Column(val expr: Expression) extends Logging {

      def this(name: String) = this(name match {
        case "*" => UnresolvedStar(None)
        case _ if name.endsWith(".*") =>
          val parts = UnresolvedAttribute.parseAttributeName(name.substring(0, name.length - 2))
          UnresolvedStar(Some(parts))
        case _ => UnresolvedAttribute.quotedString(name)
      })
      ...
    }
    ```

    对于 String 中有 * 相关的会创建 UnresolvedStar, 否则创建 UnresolvedAttribute. 注意, 该 expression 是 Column 的 expr变量.

    接着 select 函数创建 Project.projectList 用于"裁剪" 只选择 "name" 列. Project 的输入为上面的 LogicalRelation, 输出为 `projectList.map(_.toAttribute)`

    ``` scala
    def select(cols: Column*): DataFrame = withPlan {
      val untypedCols = cols.map {
        case typedCol: TypedColumn[_, _] =>
          // ...
        case other => other
      }
      Project(untypedCols.map(_.named), logicalPlan)
    }
    ```

    此时还没有对 LogicalPlan 进行解析, 所以 spark 并不知道关于 "name" 列的任何信息(数据类型,nullable等),以及 "name" 是否有效也不知道. 当通过 `untypedCols.map(_.named)` 把 Column 转换为 Project 的 projectList 时, 依然是 UnresolvedAttribute , 只不过多加了一个 UnresovedAlias.

    ![project](/docs/spark/expression/attribute/expression-project.svg)

3. 解析 UnresolvedAttribute

   最后 withPlan 触发 LogicalPlan 的解析. 对于 UnresolvedAttribute 的解析是在 Analyzer 里的 ResolveReference rule下进行的. 代码如下,

   ``` scala
   def apply(plan: LogicalPlan): LogicalPlan = plan.resolveOperatorsUp {
      // ...
      case q: LogicalPlan =>
        q.mapExpressions(resolveExpressionTopDown(_, q))
   }

   private def resolveExpressionTopDown(
    e: Expression,
    q: LogicalPlan,
    trimAlias: Boolean = false): Expression = {

      def innerResolve(e: Expression, isTopLevel: Boolean): Expression = {
        if (e.resolved) return e
        e match {
          case u @ UnresolvedAttribute(nameParts) =>
            // Leave unchanged if resolution fails. Hopefully will be resolved next round.
            val resolved =
              withPosition(u) {
                // 调用 LogicalPlan的resolveChildren
                q.resolveChildren(nameParts, resolver)
                  .orElse(resolveLiteralFunction(nameParts, u, q))
                  .getOrElse(u)
              }
            val result = resolved match {
              case Alias(s: GetStructField, _) if trimAlias && !isTopLevel => s
              case others => others
            }
            logDebug(s"Resolving $u to $result")
            result

          // 第一次进来的是 UnresolvedAlias, 递归调用 innerResolve
          case _ => e.mapChildren(innerResolve(_, isTopLevel = false))
        }
      }
      innerResolve(e, isTopLevel = true)
    }
   ```

   ``` scala
   private[this] lazy val childAttributes =
       AttributeSeq(children.flatMap(c => c.output ++ c.metadataOutput))

   def resolveChildren(
       nameParts: Seq[String],
       resolver: Resolver): Option[NamedExpression] =
     childAttributes.resolve(nameParts, resolver)
   ```

   LogicalPlan 的 resolveChildren 首先获得 child 输出的 Attribute, 在本例中最终会找到 `AttributeReference("name", StringType)` 与 `AttributeReference("age", IntegerType)` 然后 resolve 的本质就是查找是否有 name="name"的AttributeReference. 如果找到则返回该 AttributeReference. 在本例中是可以找到的. 即 Project.projectList 引用到了 "name" 列. 如果没有找到的话, projectList 依然保存着 UnresolvedAttribute, 最后会报如下错误

   ``` console
   org.apache.spark.sql.AnalysisException: cannot resolve '`namex`' given input columns: [class, english, history, math, name, number];
   'Project ['namex]
   +- Relation[name#0,number#1,english#2,math#3,history#4,class#5] parquet
   ```

   最终的 LogicalPlan 如下

   ![final logical plan](/docs/spark/expression/attribute/expression-project-resolved.svg)

4. BoundReference

   Attribute 是一个属性,它的实现类不管是 UnresolvedAttribute 或是 AttributeReference 都是 Unevaluable 的,是不能参与计算和 Codegen. 但是有些包含Attribute的expression是需要计算和codegen的, 如大部分 agg function, SortOrder等需要在对应的列上进行计算, 而 Attribute 里引用着相关列的信息. 那如何才能让 Attribute 参与到计算中呢. 答案是通过 BoundReference.

   ``` scala
   case class BoundReference(ordinal: Int, dataType: DataType, nullable: Boolean) extends LeafExpression {
   ```

   - ordinal BoundReference与输入的 InternalRow 的哪个 slot 进行绑定
   - dataType 绑定slot的输入数据类型
   - nullable 绑定slot是否可null

   Spark 通过 BindReferences.bindReference 将一个 Expression 与输入进行绑定, 其本质上是将 Expression中 的 AttributeReference 与输入进行绑定

   ``` scala
   def bindReference[A <: Expression](
      expression: A, // 需要被绑定的 expression
      input: AttributeSeq,  //输入的属性
      allowFailures: Boolean = false): A = {
    expression.transform { case a: AttributeReference => // transform 获得 AttributeReference
      attachTree(a, "Binding attribute") {
        val ordinal = input.indexOf(a.exprId) // 获得 AttributeReference 在 input 中的位置
        if (ordinal == -1) {
          if (allowFailures) {
            a
          } else {
            sys.error(s"Couldn't find $a in ${input.attrs.mkString("[", ",", "]")}")
          }
        } else {
          BoundReference(ordinal, a.dataType, input(ordinal).nullable) //生成 BoundReference
        }
      }
    }.asInstanceOf[A]
   ```

   通过下面的代码来看下 BoundReference 是如何参与到计算或 codegen 中的

   ``` scala
    val colInfo = 'b.int
    val inputSchema: AttributeSeq = new AttributeSeq(Seq('a.int, colInfo))
    val input = new GenericInternalRow(Array[Any](1, 20))

    val absExpr = Abs(colInfo) //对 b 列求绝对值

    // absExpr.eval(input) //报错,因为 Abs的child为AttributeReference是不能直接计算的
    val ctx = new CodegenContext
    // absExpr.genCode(ctx) //报错, 因为 Abs的child为AttributeReference是不能参与codegen

    val expr = BindReferences.bindReference(absExpr, inputSchema)

    expr.eval(input) // worked
    expr.genCode(ctx) // worked
   ```

   BoundReference 如何 eval ?

   ``` scala
   case class BoundReference(ordinal: Int, dataType: DataType, nullable: Boolean)
      extends LeafExpression {
     // 最终调用 input.getInt(oridinal) //获得第 oridinal 列的数据
     private val accessor: (InternalRow, Int) => Any = InternalRow.getAccessor(dataType, nullable)

     // Use special getter for primitive types (for UnsafeRow)
     override def eval(input: InternalRow): Any = {
       accessor(input, ordinal)
     }
   ```

   BoundReference 最后了生成什么样的代码片断呢？

   ``` java
    boolean isNull_1 = i.isNullAt(1);
    int value_1 = isNull_1 ? -1 : (i.getInt(1)); // BoundReference 生成的代码,从 InputRow 里取第1列的值

    boolean isNull_0 = isNull_1;
    int value_0 = -1;

    if (!isNull_1) {
      value_0 = (int)(java.lang.Math.abs(value_1));
    }
   ```
