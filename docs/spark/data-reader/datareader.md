---
layout: page
title: Data Reader
nav_order: 5
parent: Spark 
---

# Spark Data Reader
{: .no_toc}

Spark 定义了 Data Source API 去处理各种各样的数据存储系统. 目前在 Spark 3.1.1 中存在两种Data Source API, 分别为 V1 和 V2. 至于 V2 引入的时间和原因, 可以参考这个 video [Apache Spark Data Source V2](https://databricks.com/session/apache-spark-data-source-v2)

本文通过一段 spark 读Parquet文件代码来学习 Spark 3.1.1 中 Data Reader 的过程.

## 目录
{: .no_toc .text-delta}

1. TOC
{:toc}

## Sample code

``` scala
def testDataReaderParquet = {
  val resource1 = "file:/data/student-parquet"
  val spark = SparkSession.builder().master("local[1]").appName("test data reader").getOrCreate()
  //    spark.conf.set("spark.sql.sources.useV1SourceList", "avro")
  val schema = StructType(Array(
    StructField("name", StringType),
    StructField("number", IntegerType),
    StructField("class", IntegerType),
    StructField("math", IntegerType),
  ))
  val df = spark.read.format("parquet").schema(schema).load(resource1).toDF()
  df.explain(true)
  spark.close()
}
```

`/data/student-parquet` 是一个以 class (value=1 2 3)列进行分区的文件夹, 它的源数据取自于
[这里](https://github.com/wbo4958/wbo4958.github.io/blob/master/data/student-score.csv)

``` console
'./class=1':
part-00000-420b3c0a-508e-42d8-bb5d-fbdf645e7206.c000.snappy.parquet
'./class=2':
part-00000-420b3c0a-508e-42d8-bb5d-fbdf645e7206.c000.snappy.parquet
'./class=3':
part-00000-420b3c0a-508e-42d8-bb5d-fbdf645e7206.c000.snappy.parquet
```

parquet文件的meta信息如下所示

``` console
name:        OPTIONAL BINARY L:STRING R:0 D:1
number:      OPTIONAL INT32 R:0 D:1
english:     OPTIONAL FLOAT R:0 D:1
math:        OPTIONAL INT32 R:0 D:1
history:     OPTIONAL FLOAT R:0 D:1
```

示例代码只读取其中 name, number, class, match 列数据.

## DataFrameReader到LogicalPlan

**spark.read** 返回一个 DataFrameReader 用于加载外部存储数据到 Dataset. DataFrameReader有三个成员变量分别为

``` scala
// 指定输入数据的 source format
private var source: String = sparkSession.sessionState.conf.defaultDataSourceName
// 用户是否提供了自定义的 schema
private var userSpecifiedSchema: Option[StructType] = None
// 每个source format或许会要求一些其它的参数, 而这些参数放在 extraOptions map中
private val extraOptions = new scala.collection.mutable.HashMap[String, String]
```

本示例中指定 "source=parquet", 然后自定义了需要读取的 schema. 如果没有自定义, 那默认读取所有列.

首先看下 data source 的类图结构. 几乎所有的 data source 都直接或间接实现了 DataSourceRegister, 而 v1 data source 实现了 FileFormat, v2 data source 实现 TableProvider 并继承于 FileDataSourceV2.

![data source](/docs/spark/data-reader/datareader-data-source.svg)

接下来看下 Spark 怎么 load 数据到 Dataset, 最后是怎么生成 RDD的, 如下图所示

![data reader](/docs/spark/data-reader/datareader-overview.svg)

从图中可以看出,

1. 首先确定是使用 v1 还是 v2 data source

   DataFrameReader 通过 ServiceLoader 加载 DataSourceRegister 且 [已经声明]((https://github.com/apache/spark/blob/v3.1.1-rc1/sql/core/src/main/resources/META-INF/services/org.apache.spark.sql.sources.DataSourceRegister#L6)) 的实现类, 以及 `spark.sql.sources.useV1SourceList` 查找是否使用 data source v1 的实现. 目前在 spark 3.1.1 中, useV1SourceList 默认为 **"avro,csv,json,kafka,orc,parquet,text"**.

2. 然后创建 InMemoryFileIndex 并获得 dataSchema, partitionSchema 等.

3. 最后创建 LogicalPlan

     - 对于 v1, 生成 HadoopFsRelation, 并创建 LogicalPlan.

        HadoopFsRelation字段 ||
        ------------ | -------------
        location: FileIndex | 一个接口用来枚举出所有的源文件path,以及分区
        partitionSchema: StructType | 用于分区的列 schema
        dataSchema: StructType | 需要读取的列 schema
        bucketSpec: Option[BucketSpec] | 描述是否是 bucketing ?
        fileFormat: FileFormat | V1 的 FileFormat 用于读写文件
        options: Map[String, String] | 用来读写数据的配置项,也就是 DataFrameReader 里的 extraOptions

     - 对于 v2, 通过 getTable 获得 Table, 并创建 DataSourceV2Relation.

        几乎所有的 Table间接实现类都是继承于 FileTable.

        FileTable字段 ||
        ------------ | -------------
        **fileIndex: PartitioningAwareFileIndex** | 也主是可以识别分区的FileIndex
        dataSchema: StructType| 需要读取的列 schema
        schema: StructType | 整个 Table 的 schema
        String name() | table的名字
        Transform[] partitioning() | fileIndex.partitionSchema.names.toSeq.asTransforms
        Map<String, String> properties() | Table的属性, options.asCaseSensitiveMap

### InMemoryFileIndex

v1 和 v2 最后生成的 LogicalPlan 都间接包含 FileIndex, 在这里其实都是 InMemoryFileIndex 这个实现类, InMemoryFileIndex 主要是根据输入的文件或文件夹递归的查找叶子文件, 并推断出分区信息.

InMemoryFileIndex 在构造函数中会扫描输入的文件. 如图所示 ![InMemoryFileIndex](/docs/spark/data-reader/datareader-InMemoryFileIndex.svg)

整个过程很简单, 通过 FileSystem.listStatus 递归的查找 child 文件或文件夹. 如果当 child 的数量大于某个值由 `spark.sql.sources.parallelPartitionDiscovery.parallelism` 指定 (默认32), 会将查询 job 提交到 Spark cluster执行, 这样查询速度会更快.

InMemoryFileIndex 在获得叶子文件后, 通过 FileSystem.getFileBlockLocations 获得该文件的 BlockLocation --- 这个信息对于 Spark Locality 来说非常重要. 最后 InMemoryFileIndex 将查询到的文件信息缓存到变量中.

InMemoryFileIndex 字段 ||
------------| -----------
rootPathsSpecified: Seq[Path] | root path, 用于 scan
parameters: Map[String, String]| 同 DataFrameReader 里的 extraOptions
userSpecifiedSchema: Option[StructType]| 用户指定的 schema
userSpecifiedPartitionSpec: Option[PartitionSpec]| 用户指定的 PartitionSepc
cachedLeafFiles: mutable.LinkedHashMap[Path, FileStatus]| 扫描后缓存到 leaf 文件信息 
cachedLeafDirToChildrenFiles: Map[Path, Array[FileStatus]]| dir -> leafs_of_dir
cachedPartitionSpec: PartitionSpec| 如果用户没有指定, infer PartitionSpec

- 推断 PartitionSpec

当用户没有指定 userSpecifiedPartitionSpec 时, InMemoryFileIndex 根据 leafDirs 推断中 PartitionSpec, 具体实现可以参考 [这里](https://github.com/apache/spark/blob/v3.1.1-rc1/sql/core/src/main/scala/org/apache/spark/sql/execution/datasources/PartitioningUtils.scala#L95).

``` console
PartitionSpec(
  partitionColumns = StructType(
    StructField(name = "class", dataType = IntegerType, nullable = true),
  partitions = Seq(
    Partition(
      values = Row(1),
      path = "file:/data/student-parquet/class=1"),
    Partition(
      values = Row(2),
      path = "file:/data/student-parquet/class=2"),
    Partition(
      values = Row(3),
      path = "file:/data/student-parquet/class=3"),      
    )))
```

推断 column dataType 时, 如果用户指定的 schema 包含了该 column, 就不再推断 dataType, 直接使用用户指定的 dataType, 反之也需要推断出 dataType, 推断也比较简单, 各种 Try.

``` scala
Try(Literal.create(Integer.parseInt(raw), IntegerType))
  .orElse(Try(Literal.create(JLong.parseLong(raw), LongType)))
  .orElse(decimalTry)
  // Then falls back to fractional types
  .orElse(Try(Literal.create(JDouble.parseDouble(raw), DoubleType)))
  // Then falls back to date/timestamp types
  .orElse(timestampTry)
  .orElse(dateTry)
  // Then falls back to string
  .getOrElse {
    if (raw == DEFAULT_PARTITION_NAME) {
      Literal.create(null, NullType)
    } else {
      Literal.create(unescapePathName(raw), StringType)
    }
  }
```

### Schema

不管是 v1 还是 v2, 都有很多种类型的 schema, 那这些 schema 是什么呢?

- **userSpecifiedSchema**
  
  用户在读数据时指定的 schema, 如果没有设置, 则为 empty. 本例中的为

  ``` scala
  StructType(Array(
    StructField("name", StringType),
    StructField("number", IntegerType),
    StructField("class", IntegerType),
    StructField("math", IntegerType),
  ))
  ```

- **partitionSchema**
  
  用来分区的 schema, 本例为

  ``` scala
  StructType(
    StructField("class", IntegerType)
  )
  ```

- **dataSchema**

  非 partition schema. 如果用户指定了 userSpecifiedSchema, dataSchema=(userSpecifiedSchema - partitionSchema), 如果用户没有指定 userSpecifiedSchema, dataSchema=inferSchema(), 需要自行推断出. 本例为

  ``` scala
  StructType(
    StructField("name", StringType),
    StructField("number", IntegerType),
    StructField("math", IntegerType),
  )
  ```

下面这张图描述了怎么样推断出 Parquet 的 schema. ![parquet-infer-schema](/docs/spark/data-reader/datareader-parquet-infer-schema.svg)

## LogicalPlan 到 PhysicalPlan

- v1

  对于 v1. LogicalPlan(HadoopFsRelation) 在 [FileSourceStrategy](https://github.com/apache/spark/blob/v3.1.1-rc1/sql/core/src/main/scala/org/apache/spark/sql/execution/datasources/FileSourceStrategy.scala#L145) 中被替换成 FileSourceScanExec.

- v2

  对于 v2. 在 Optimization的 rule [V2ScanRelationPushDown](https://github.com/apache/spark/blob/v3.1.1-rc1/sql/core/src/main/scala/org/apache/spark/sql/execution/datasources/v2/V2ScanRelationPushDown.scala#L32) 将 DataSourceV2Relation 替换成DataSourceV2ScanRelation, 且 FileTable 通过 newScanBuilder 生成 Scan, 如 Parquet/CSV/OrcTable -> Parquet/CSV/OrcScan等. 紧接着在 [DataSourceV2Strategy](https://github.com/apache/spark/blob/v3.1.1-rc1/sql/core/src/main/scala/org/apache/spark/sql/execution/datasources/v2/DataSourceV2Strategy.scala#L108) 中, DataSourceV2ScanRelation 被替换成 BatchScanExec.

## PhysicalPlan 到 RDD

PhysicalPlan 生成最后的 RDD. 对于 Row-wised 的 PhysicalPlan 通过 doExecute() 触发, 而对于 Columnar-wised 通过 doExecuteColumnar 来触发.

![physicalplan rdd](/docs/spark/data-reader/datareader-physicalplan-rdd.svg)

- v1

  对于 v1. readFunction 也就是 FileScanRDD 的计算函数, filePartitions 是 readFunction 计算数据(这里并不是真实的文件内的数据, 相反只是文件信息)

- v2

  对于 v2. PartitionReaderFactory 通过 createColumnarReader 或 createReader 创建计算函数.

## RDD read

不管是 v1 最后生成的 FileSourceRDD 还是 v2 生成的 DataSourceRDD, 当 `spark.sql.parquet.enableVectorizedReader` 打开时 (默认为true), 则创建 VectorizedParquetRecordReader 读取 Parquet 文件返回 ColumnBatch, 反之创建 ParquetRecordReader 返回 InternalRow.

![rdd-read](/docs/spark/data-reader/datareader-rdd-read.svg)

## Filters PushDown (谓词下推)

谓词下推在不影响结果的情况下，尽量将过滤条件下移到离数据源最近的地方，这样可以过滤掉很多不用的数据.

file| math meta
----|----
class=1/xxx.parquet|`math: INT32 SNAPPY DO:0 FPO:267 SZ:81/79/0.98 VC:7 ENC:PLAIN,BIT_PACKED,RLE ST:[min: 65, max: 99, num_nulls: 0]`
class=2/xxx.parquet|`math: INT32 SNAPPY DO:0 FPO:332 SZ:98/96/0.98 VC:12 ENC:PLAIN,BIT_PACKED,RLE ST:[min: 12, max: 98, num_nulls: 1]`
class=3/xxx.parquet|`math: INT32 SNAPPY DO:0 FPO:316 SZ:93/95/1.02 VC:11 ENC:PLAIN,BIT_PACKED,RLE ST:[min: 34, max: 98, num_nulls: 0]`

Parquet 文件的 meta 信息保存了 STATISTIC 信息， 如一个 column 的最大值和最小值.

可以看到

file| math min | math max
----|----      | -----
class=1/xxx.parquet|65| 99
class=2/xxx.parquet|12| 98
class=3/xxx.parquet|34| 98

如果查询条件加上 math < 50, 那其实只需要读取 class=2 和 class=3 中的文件即可. 如果需要查询 class!=3 班上的(也就是class=1和class=2)， 那更确定只有 class2/xxx.parquet 里的数据才满足条件, 事实上 Spark 确实是这么做的.

``` scala
val df = spark.read.format("parquet").schema(schema).load(resource1).toDF().where("math < 50 and class != 3")
```

从 [InMemoryFileIndex](#inmemoryfileindex) 可知, InMemoryFileIndex 保存了所有的输入文件信息. 但有些文件可能 filter 条件下并不需要， 因此可以去除掉. 而 InMemoryFileIndex (FileIndex) 提供 listFiles

``` scala
def listFiles(partitionFilters: Seq[Expression], dataFilters: Seq[Expression]): Seq[PartitionDirectory]
```

listFiles 接收 partitionFilers(class != 3) 与 dataFilters (match < 50) 返回过滤后的结果. InMemoryFileIndex 已经推断中 PartitionSpec, 只需要过滤掉 class！=3 的文件夹即可， listFiles 只返回 `class=1/xxx.parquet 和 class=2/xxx.parquet`. InMemoryFileIndex.listFiles 并没有使用 dataFilters 且 在 driver 端调用.

当 Spark task 准备读取 Parquet 文件时先读取 BlockMetaData 信息， 然后根据 data filters 过滤到不需要的 Row Group, 返回有用的 Row Group. 具体是在 `filterRowGroups`.

可以参考 [Parquet的那些事（一）基本原理](https://blog.csdn.net/zwgdft/article/details/104582229)

``` console
== Physical Plan ==
*(1) Project [name#0, number#1, math#2, class#3]
+- *(1) Filter (isnotnull(math#2) AND (math#2 < 33))
   +- *(1) ColumnarToRow
      +- FileScan parquet [name#0,number#1,math#2,class#3] Batched: true,
          DataFilters: [isnotnull(math#2), (math#2 < 33)], Format: Parquet,
          Location: InMemoryFileIndex[file:/home/bobwang/tools/spark-data/student-parquet],
          PartitionFilters: [isnotnull(class#3), NOT (class#3 = 3)],
          PushedFilters: [IsNotNull(math), LessThan(math,33)],
          ReadSchema: struct<name:string,number:int,math:int>
```

可以看到 PushedFilters 确实是 push 到 FileScanRDD 中了.

注意: 这里依然是有个  Filter physical plan 的， 并不是说 filter pushdown了就不需要再进行 filter 了， 以本例来说， Parquet 文件是以 Row Group 单位来读取的， 这里面有些数据依然不符合要求， 所以依然需要 filter 掉. 但是 filter pushdown 后， 从原来的需要读取 3 个文件 到最后只需要读取 1 个文件了， 大大减少了输入数据.
