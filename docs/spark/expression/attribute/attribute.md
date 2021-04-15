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

Spark plan operator 的输入和输出之分. 上一个 operator 的输出会作为当前 operator 的输入, 而当前 operator 的输出又会作为下一个 operator 的输入, 依此类推. Attribute 扮演的角色就是将 operator 之间的输入与输出进行关联起来. 一个 Spark operator 对应的输入和输出的 Attribute 数量并不总是一一对应的, 大多数时候会不同. 有些 operator 可能会减少 attribute 如 ProjectExec, 而有些 operator 会增加 attribute 如 WindowExec. Attribute 另一个作用是将 operator 中 Expression 的输出绑定到 operator 的输出中.

[`Attribute is the Catalyst name for an input column from a child operator.`](http://mail-archives.apache.org/mod_mbox/spark-user/201508.mbox/%3CCAAswR-59+2Fz1HNfVAUmM1Oc_uWaHki24=vrGuB3oh7x_cUG7A@mail.gmail.com%3E) 这句话其实不太准确， Attribute 大多数会绑定到 table 的 field 或 column, 但也会绑定到 expression 的输出.

### Attribute 实现类

``` scala
abstract class Attribute extends LeafExpression with NamedExpression with NullIntolerant
```

Attribute 是一个抽象类, 没有 children 输入, 是一个 命名 Expression, 因此每个 Attribute 都会有

``` scala
def name: String
def exprId: ExprId
def qualifier: Seq[String] // optional, 包含一些限定信息如 catalog, database, table
def metadata: Metadata
```

- UnresolvedAttribute
  
  保存着尚未解析的属性的名称. 因为没有解析，它也就没有 exprId, dataType 等等

- AttributeReference

  UnresolvedAttribute 解析过后变成了 AttributeReference, 它表示绑定到某个 operator 输出的某个属性, 既然它已经是被解析过的，那它就有确定的 dataType, nullable, exprId 等

### AttributeSet

AttributeSet 通过 LinkedHashSet 保存 AttributeEquals, 而 AttributeEquals 对 Attribute 进行 wrap 一层.

``` scala
protected class AttributeEquals(val a: Attribute) {
  //两个 Attribute 相同的，比较的是 exprId
  override def equals(other: Any): Boolean = (a, other.asInstanceOf[AttributeEquals].a) match {
    case (a1: AttributeReference, a2: AttributeReference) => a1.exprId == a2.exprId
    case (a1, a2) => a1 == a2
  }
}
```

所以可以看出 AttributeSet 其实就是 Attribute 的一个集合表达式

### Expression 中的 Attribute

Expression 中定义了 references, 表示 expression 中所有的 Attribute, 默认为所有的 children 的 attribute

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

QueryPlan 是 LogicalPlan 与 SparkPlan 的基类，它定义了与 Attribute 相关的方法.

|QueryPlan|Attribute 相关|
|----|----|
| def output: Seq[Attribute] | Node的输出属性  |
| lazy val outputSet: AttributeSet | output 的 AttributeSet, 默认为 AttributeSet(output) |
| def inputSet: AttributeSet | node 的 input set, 默认为 AttributeSet(children.flatMap(_.asInstanceOf[QueryPlan[PlanType]].output)) |
| def producedAttributes: AttributeSet | 该 node 生成的 Attribute, 和 output 什么关系???? 默认为 AttributeSet.empty|
| val references: AttributeSet | node 的 expression 中所出现的所有的 attribute. 默认为AttributeSet.fromAttributeSets(expressions.map(_.references)) -- producedAttributes |
| def missingInput: AttributeSet | 非 child 中的 attribute, 默认为references -- inputSet|

### schema 与 Attribute

Spark SQL 通过定义 schema 来处理结构化和半结构化的数据. schema 定义数据的字段，类型等. Spark SQL 通过 StructType 来表示 schema. 参考[这里](https://www.waitingforcode.com/apache-spark-sql/schemas/read)

StructType 由一组 StructField 组成，每个 StructField 描述一个字段的信息

- name 字段名，列名
- dataType 字段的数据类型
- nullable 字段是否可以接收 null 型的数据
- metadata 字段的 meta 信息，metadata是一个Map[String, Any]类型, 描述字段的附加信息. 如字段的Comments

  ``` scala
  /**
   * Return the comment of this StructField.
   */
  def getComment(): Option[String] = {
    if (metadata.contains("comment")) Option(metadata.getString("comment")) else None
  }
  ```

StructType 中通过 toAttribute 可以将 Attribute 绑定到 schema 的 field (column) 上. 因为 schema 的数据类型, nullable都是已知的，所以并不需要再次解析. 由 AttributeReference 表示.

``` scala
protected[sql] def toAttributes: Seq[AttributeReference] =
  map(f => AttributeReference(f.name, f.dataType, f.nullable, f.metadata)())
```

## Attribute 的解析与绑定

1. LogicalPlan 绑定到数据对应的列

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

    LogicalRelation 是一个 LeafNode, 它没有输入, LogicalRelation 表示的是底层数据信息， 它的 `output: Seq[AttributeReference]` 已经绑定到了数据的 field.

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

    对于 String 中有 * 相关的会创建 UnresolvedStar, 否则创建 UnresolvedAttribute, 注意, 这些 expression 是 Column 的 expr变量.

    接着 select 函数创建 Project 的 projectList 用于"裁剪" 只选择 "name" 列. Project 的输入为上面的 LogicalRelation, 输出为 `projectList.map(_.toAttribute)`

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

    此时还并没有对 LogicalPlan 进行解析， 所以 spark 并不知道关于 "name" 列的任何信息(数据类型，nullable等)，包括 "name" 是否有效也不知道. 当通过 `untypedCols.map(_.named)` 把 Column 转换为 Project 的 projectList 时， 依然使用 UnresolvedAttribute , 只不过多加了一个 UnresovedAlias.

    ![project](/docs/spark/expression/attribute/expression-project.svg)

3. 解析 UnresolvedAttribute

   最后 withPlan 时触发 LogicalPlan 的解析. 对于 UnresolvedAttribute 的解析是在 Analyzer 里的 ResolveReference rule下进行的. 代码如下,

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

   LogicalPlan 的 resolveChildren 首先获得 child 输出的 Attribute, 在本例中最终会找到 `AttributeReference("name", StringType)` 与 `AttributeReference("age", IntegerType)` 然后 resolve 的本质就是查找是否有 name="name"的AttributeReference. 如果找到则返回该 AttributeReference. 在本例中是可以找到的. 即 Project 的 projectList 与 "name" 列进行绑定了. 如果没有找到的话, projectList 依然保存着 UnresolvedAttribute, 最后会报如下错误

   ``` console
   org.apache.spark.sql.AnalysisException: cannot resolve '`namex`' given input columns: [class, english, history, math, name, number];
   'Project ['namex]
   +- Relation[name#0,number#1,english#2,math#3,history#4,class#5] parquet
   ```

   最终的 LogicalPlan 如下

   ![final logical plan](/docs/spark/expression/attribute/expression-project-resolved.svg)