---
layout: page
title: Schema
nav_order: 5
parent: Spark 
---

# Spark schema
{: .no_toc}


## 目录
{: .no_toc .text-delta}

1. TOC
{:toc}

## xxx

- 没有 user defined schema, 默认输出 Parquet 文件所有的列.

``` scala
scala> var df = spark.read.parquet("/home/bobwang/data/student/student-parquet/class=1")
df: org.apache.spark.sql.DataFrame = [name: string, number: int ... 3 more fields]

scala> df.printSchema //Dataset 的 schema, parquet 文件中所有的列
root
 |-- name: string (nullable = true)
 |-- number: integer (nullable = true)
 |-- english: float (nullable = true)
 |-- math: integer (nullable = true)
 |-- history: float (nullable = true)


scala> df.explain(true) //没有任何 ProjectExec
== Parsed Logical Plan ==
Relation [name#0,number#1,english#2,math#3,history#4] parquet

== Analyzed Logical Plan ==
name: string, number: int, english: float, math: int, history: float
Relation [name#0,number#1,english#2,math#3,history#4] parquet

== Optimized Logical Plan ==
Relation [name#0,number#1,english#2,math#3,history#4] parquet

== Physical Plan ==
*(1) ColumnarToRow
+- FileScan parquet [name#0,number#1,english#2,math#3,history#4] Batched: true, DataFilters: [], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/home/bobwang/data/student/student-parquet/class=1], PartitionFilters: [], PushedFilters: [], ReadSchema: struct<name:string,number:int,english:float,math:int,history:float>

```

- 用户指定 schema, 但是该　schema "TESTING_NOT_EXITING" 并没在 FileScan 的找到 reference.

``` scala
scala> val schema = StructType(Seq(StructField("TESTING_NOT_EXITING", IntegerType)))                                                      
schema: org.apache.spark.sql.types.StructType = StructType(StructField(TESTING_NOT_EXITING,IntegerType,true))

scala> var df = spark.read.schema(schema).parquet("/home/bobwang/data/student/student-parquet/class=1")
df: org.apache.spark.sql.DataFrame = [TESTING_NOT_EXITING: int]

scala> df.printSchema
root
 |-- TESTING_NOT_EXITING: integer (nullable = true)


scala> df.explain(true)
== Parsed Logical Plan ==
Relation [TESTING_NOT_EXITING#39] parquet

== Analyzed Logical Plan ==
TESTING_NOT_EXITING: int
Relation [TESTING_NOT_EXITING#39] parquet

== Optimized Logical Plan ==
Relation [TESTING_NOT_EXITING#39] parquet

== Physical Plan ==
*(1) ColumnarToRow
+- FileScan parquet [TESTING_NOT_EXITING#39] Batched: true, DataFilters: [], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/home/bobwang/data/student/student-parquet/class=1], PartitionFilters: [], PushedFilters: [], ReadSchema: struct<TESTING_NOT_EXITING:int>


scala> df.show(2) //读出来全是 null
+-------------------+
|TESTING_NOT_EXITING|
+-------------------+
|               null|
|               null|
+-------------------+
only showing top 2 rows

```

``` scala
scala> var df = spark.read.parquet("/home/bobwang/data/student/student-parquet/class=1")
df: org.apache.spark.sql.DataFrame = [name: string, number: int ... 3 more fields]

scala> df.printSchema
root
 |-- name: string (nullable = true)
 |-- number: integer (nullable = true)
 |-- english: float (nullable = true)
 |-- math: integer (nullable = true)
 |-- history: float (nullable = true)


scala> df = df.select("NAME")
df: org.apache.spark.sql.DataFrame = [NAME: string]

scala> df.printSchema
root
 |-- NAME: string (nullable = true)


scala> df.explain(true)
== Parsed Logical Plan ==
'Project ['NAME]
+- Relation [name#11,number#12,english#13,math#14,history#15] parquet

== Analyzed Logical Plan ==
NAME: string
Project [NAME#11]
+- Relation [name#11,number#12,english#13,math#14,history#15] parquet

== Optimized Logical Plan ==
Project [NAME#11]
+- Relation [name#11,number#12,english#13,math#14,history#15] parquet

== Physical Plan ==
*(1) ColumnarToRow
+- FileScan parquet [name#11] Batched: true, DataFilters: [], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/home/bobwang/data/student/student-parquet/class=1], PartitionFilters: [], PushedFilters: [], ReadSchema: struct<name:string>


scala> df.show(2)
+-----+
| NAME|
+-----+
| andy|
|yijun|
+-----+
only showing top 2 rows

```