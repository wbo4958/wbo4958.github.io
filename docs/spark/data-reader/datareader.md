---
layout: page
title: Spark Data Reader
nav_order: 5
parent: Spark 
---

Spark 定义了 Data Source API 去处理各种各样的数据存储系统. 目前在 Spark 3.1.1 中存在两种Data Source API, 分别为 V1 和 V2. 至于 V2 引入的时间和原因, 可以参考这个 video [Apache Spark Data Source V2](https://databricks.com/session/apache-spark-data-source-v2)

本文通过运行一段 spark 读文件代码来学习 Spark 3.1.1 中关于 Data Reader的过程.

## 示例代码

``` scala
def testDataReaderParquet = {
  val resource1 = "file:/data/annual-enterprise"
  val spark = SparkSession.builder().master("local[1]").appName("test data reader").getOrCreate()
  //    spark.conf.set("spark.sql.sources.useV1SourceList", "avro")
  val schema = StructType(Array(
    StructField("Year", IntegerType),
    StructField("Value", StringType),
    StructField("Industry_code_ANZSIC06", StringType)
  ))
  val df = spark.read.format("parquet").schema(schema).load(resource1).toDF()
  df.explain(true)
  spark.close()
}
```

`/data/annual-enterprise` 是一个以 Year (从2013年到2019年)列进行分区的文件夹, 它的源数据取自于
[这里](https://www.stats.govt.nz/information-releases/annual-enterprise-survey-2019-financial-year-provisional)

``` console
'./Year=2013':
part-00000-20f3b683-60ba-4b4e-8133-17008d0e102a.c000.snappy.parquet

'./Year=2014':
part-00000-20f3b683-60ba-4b4e-8133-17008d0e102a.c000.snappy.parquet

'./Year=2015':
part-00000-20f3b683-60ba-4b4e-8133-17008d0e102a.c000.snappy.parquet

'./Year=2016':
part-00000-20f3b683-60ba-4b4e-8133-17008d0e102a.c000.snappy.parquet

'./Year=2017':
part-00000-20f3b683-60ba-4b4e-8133-17008d0e102a.c000.snappy.parquet

'./Year=2018':
part-00000-20f3b683-60ba-4b4e-8133-17008d0e102a.c000.snappy.parquet

'./Year=2019':
part-00000-20f3b683-60ba-4b4e-8133-17008d0e102a.c000.snappy.parquet
```

parquet文件的meta信息如下所示

``` console
Industry_aggregation_NZSIOC: OPTIONAL BINARY L:STRING R:0 D:1
Industry_code_NZSIOC:        OPTIONAL BINARY L:STRING R:0 D:1
Industry_name_NZSIOC:        OPTIONAL BINARY L:STRING R:0 D:1
Units:                       OPTIONAL BINARY L:STRING R:0 D:1
Variable_code:               OPTIONAL BINARY L:STRING R:0 D:1
Variable_name:               OPTIONAL BINARY L:STRING R:0 D:1
Variable_category:           OPTIONAL BINARY L:STRING R:0 D:1
Value:                       OPTIONAL BINARY L:STRING R:0 D:1
Industry_code_ANZSIC06:      OPTIONAL BINARY L:STRING R:0 D:1
```

示例代码只读取其中 Year, Value Industry_aggregation_NZSIOC06 字段.

## DataFrameReader

**spark.read** 返回一个 DataFrameReader 用于加载外部存储数据到 Dataset. DataFrameReader有三个成员变量分别为

``` scala
// 指定输入数据的 source format
private var source: String = sparkSession.sessionState.conf.defaultDataSourceName
// 用户是否提供了自定义的 schema
private var userSpecifiedSchema: Option[StructType] = None
// 每个source format或许会要求一些其它的参数, 而这些参数放在 extraOptions map中
private val extraOptions = new scala.collection.mutable.HashMap[String, String]
```

本示例中指定 "source=parquet", 然后自定义了需要读取的 schema. 如果没有自定义, 那默认会读取 Parquet文件中所有的列.

接下来看下 Spark 怎么 load 数据到 Dataset, 最后是怎么生成 RDD的, 如下图所示

![data reader](/docs/spark/data-reader/datareader-overview.svg)

从图中可以看出, DataFrameReader 首先根据 source 以及 `spark.sql.sources.useV1SourceList` 查找是否使用 data source v2 的实现.

目前在 spark 3.1.1 中, useV1SourceList 默认为 **"avro,csv,json,kafka,orc,parquet,text"**. 

另外需要向 META-INFO/services/`org.apache.spark.sql.sources.DataSourceRegister` 文件中声明DataSourceRegister 的 v2 data source 实现类, 参考[这里](https://github.com/apache/spark/blob/v3.1.1-rc1/sql/core/src/main/resources/META-INF/services/org.apache.spark.sql.sources.DataSourceRegister#L6).

### v1 LogicalPlan

对于 v1 的 data source, 最终生成的是 HadoopFsRelation, 最后再包裹成 LogicalPlan.

HadoopFsRelation字段 ||
------------ | -------------
location: FileIndex | 一个接口用来枚举出所有的源文件path,以及分区
partitionSchema: StructType | 用于分区的列 schema
dataSchema: StructType | 需要读取的列 schema
bucketSpec: Option[BucketSpec] | 描述是否是 bucketing ?
fileFormat: FileFormat | V1 的 FileFormat 用于读写文件
options: Map[String, String] | 用来读写数据的配置项,也就是 DataFrameReader 里的 extraOptions

### v2 LogicalPlan

对于 v2 的 data source, 最终生成的是 Table, 最后包裹到 DataSourceV2Relation 中, 几乎所有的 Table间接实现类都是继承于 FileTable.

FileTable字段 ||
------------ | -------------
**fileIndex: PartitioningAwareFileIndex** | 也主是可以识别分区的FileIndex
dataSchema: StructType| 需要读取的列 schema
schema: StructType | 整个 Table 的 schema
String name() | table的名字
StructType schema() | table的schema
Transform[] partitioning() | fileIndex.partitionSchema.names.toSeq.asTransforms
Map<String, String> properties() | Table的属性, options.asCaseSensitiveMap

### InMemoryFileIndex

v1 和 v2 最后生成的 LogicalPlan 都间接包含 FileIndex, 在这里其实都是 InMemoryFileIndex 这个实现类, InMemoryFileIndex 主要是根据输入的文件或文件夹递归的查找文件, 并推断中分区信息.

InMemoryFileIndex 在构造函数中就会扫描输入的文件. 如图所示 ![InMemoryFileIndex](/docs/spark/data-reader/datareader-InMemoryFileIndex.svg)

整个过程很简单, 通过 FileSystem.listStatus 来递归的查找 leaf 文件或文件夹. 如果当 leaf 的数量大于由 `spark.sql.sources.parallelPartitionDiscovery.parallelism` 指定的值时 (默认32), 会将查询提交到 Spark cluster 去做, 这样查询速度会更快.

InMemoryFileIndex 会通过 FileSystem.getFileBlockLocations 获得文件的 BlockLocation ---这个信息对于 Spark Locality 来说非常重要. 最后 InMemoryFileIndex 将查询到的文件信息缓存到

InMemoryFileIndex 字段 ||
------------| -----------
rootPathsSpecified: Seq[Path] | root path, 用于 scan
parameters: Map[String, String]| 同 DataFrameReader 里的 extraOptions
userSpecifiedSchema: Option[StructType]| 用户指定的 schema
userSpecifiedPartitionSpec: Option[PartitionSpec]| 用户指定的 PartitionSepc
cachedLeafFiles: mutable.LinkedHashMap[Path, FileStatus]| 扫描后缓存到 leaf 文件信息 
cachedLeafDirToChildrenFiles: Map[Path, Array[FileStatus]]| dir -> leafs_of_dir
cachedPartitionSpec: PartitionSpec| 如果用户没有指定, infer PartitionSpec