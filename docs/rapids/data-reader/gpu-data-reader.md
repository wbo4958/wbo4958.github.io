---
layout: page
title: GPU Data Reader
nav_order: 5
parent: Spark-Rapids
---

# Spark GPU Data Reader
{: .no_toc}

本文承接 [Spark CPU Data Reader](../../spark/data-reader/datareader.md) 介绍 Rapids 库是怎样利用 GPU 加速 Spark 的文件读取. 本文基于 [Rapids](https://github.com/NVIDIA/spark-rapids) branch 0.4.0

## 目录
{: .no_toc .text-delta}

1. TOC
{:toc}

## 替换 CPU Physical Plan 为 GPU physical plan

![gpu physical plan](/docs/rapids/data-reader/datareader-gpu-overview.svg)

如图所示,

- v1
  
  Rapids 将 FileSourceScanExec 替换成 GpuFileSourceScanExec, ParquetFileFormat 替换成 GpuReadParquetFileFormat. 并创建一个新的 HadoopFsRelation 指向转换后的 GpuReadParquetFileFormat, 其它保持不变.

- v2
  
  Rapids 将 ParquetScan 替换成 GpuParquetScan, BatchScanExec 替换成 GpuBatchScanExec.

## GPU physical plan 到 RDD

![gpu physical plan to rdd](/docs/rapids/data-reader/datareader-gpu-physicalplan-rdd.svg)

- v1
  
  对于 Parquet 格式的文件, Rapids 提供了多种 reader 来针对不同的使用场景, 由 `spark.rapids.sql.format.parquet.reader.type` 控制

  reader type||
  -----|------
  PERFILE | 串行读取读取每个文件，每读完一个文件交给 GPU decode
  COALESCING | 适用于数据在本地的场景, 并行读取所有的文件合并成, 然后再交给 GPU decode
  MULTITHREADED | 适用于数据在 Cloud 的环境中. Rapids 并行的读取每个文件，再依次将读取的每个文件交给 GPU decode. 这样可以达到很高的吞吐量.

  PERFILE和MULTITHREADED 会产生多个 ColumnarBatch, 每个 ColumnarBatch 数据量比较少，很难充分利用 GPU 性能。 而COALESCING 将小而多的文件合并成大的文件交给 GPU 处理会比较充分利用 GPU 的优势。

  当 `isPerFileReadEnabled = true` 时，生成 FileScanRDD, 其中计算函数 readFunction 由 `GpuReadParquetFileFormat.buildReaderWithPartitionValuesAndMetrics` 生成, 最后生成的是 `GpuParquetPartitionReaderFactory`.

  当 `isPerFileReadEnabled = false` 时, 生成 GpuDataSourceRDD 和 GpuParquetMultiFilePartitionReaderFactory, 其中 GpuParquetMultiFilePartitionReaderFactory 用来创建真正的 reader

- v2

  只生成 GpuDataSourceRDD, 但是对于 **isParquetPerFileReadEnabled=true** 的情况，生成 `GpuParquetPartitionReaderFactory` 反之生成 `GpuParquetMultiFilePartitionReaderFactory`
  
## GpuDataSourceRDD 读取数据

1. 创建 reader, Iterator

   ![GpuDataSourceRDD iterator](/docs/rapids/data-reader/datareader-gpu-data-iterator.svg)

   对于 COALESCING type, 创建 MultiFileParquetPartitionReader, 对于 MULTITHREADED 创建 MultiFileCloudParquetPartitionReader.

2. 读取数据

   ![iterator](/docs/rapids/data-reader/datareader-gpu-rdd-read.svg)

## FileSourceRDD 读取数据

![FileSourceRDD](/docs/rapids/data-reader/datareader-gpu-filescanrdd-read.svg)

`GpuParquetPartitionReaderFactory` 最终创建 `ParquetPartitionReader` 来依次读取文件.

## GPU 读取 Parquet 文件的格式

不管是 PERFILE, COALESCING 还是 MULTITHREADED, GPU 读取 Parquet 文件的过程如下

1. CPU 先将需要的 RowGroup 读取出来，放到 CPU memory 里.
2. 根据这些 RowGroup, 创建 Footer 信息， 最后生成一个 Parquet 文件格式的 buffer 放到 CPU memory 里
3. 将 Parquet 格式的 buffer 放到 CUDF 中去 decode, 生成 ColumnarBatch.

比如一个 Parquet 文件时有 10 个 row group, 每一次只需要读取 5 个 row group并生成只有这 5 个 row group 的 parquet 文件, 这个文件不会落盘，存储在 CPU memory 里。 最后将该 memory 拷到 GPU 上解析.

## GPU 如何组装 Partition 字段

- 不需要合并的情况

  对于不同的 reader, 组装 Partition 字段不相同， 比如对于 PERFILE 和 MULTITHREADED 这两种类型的 reader, 它们都是以文件单位进行读取，不同的文件之间是不会合并的, 因此对于它们，只需要知道他们的 Partition Value, 然后再创建相应的列，并填充 Partition Value 即可. 而每个文件包括 Partition的值保存在 PartitionedFile 里.

  比如

  ``` console
  file:/data/student-parquet/class=2/part-00000-420b3c0a-508e-42d8-bb5d-fbdf645e7206.c000.snappy.parquet
  ```

  PartitionedFile || value
  ------- | ---- |----
  partitionValues: InternalRow | partition 具体的值 | 2
  filePath: String | 文件 path | file:/data/student-parquet/class=2/part-00000-420b3c0a-508e-42d8-bb5d-fbdf645e7206.c000.snappy.parquet
  start: Long | 需要读取的文件的起始地址 |
  length: Long | 读取的长度 |

  针对这种情况, Rapids 通过 cudf.fromScalar 创建每行全是 2 的一个 Columnar, 然后再和从文件里读取出来的 ColumnarBatch 组合成一个新的 ColumnarBatch 即可. 具体可以参考 `ColumnarPartitionReaderWithPartitionValues.addPartitionValues`

  如[GpuDataSourceRDD 读取数据](#gpudatasourcerdd-读取数据)所示, 针对有 Partition 的 Parquet 文件， 如果 Partition 的 value

- 需要合并的情况

  对于需要合并且Partition的value超过1个的情况时， Rapids 使用 `concatAndAddPartitionColsToBatch` 函数进行填充 Partition 的value. 假设 task 要合并 class=1和class=3以及class=2下面的所有文件

  1. Rapids 按 `row_group1 | row_group3 ｜ row_group2` 的顺序合并
  2. Rapids 将合并后的数据拷贝到 GPU decode 生成 data columns
  3. Rapids 按照 row_group 合并的顺序，依次为 row_group1 生成全为 1 的 column_1, row_group3 -> column_3, row_group2 -> column_2.
  4. 然后按 `column_1|columne_3|column_3` 合并成一个 Partition column
  5. 最后 Data columns 与 Partition column 合并成一个 ColumnarBatch

  ![fill partition](/docs/rapids/data-reader/datareader-fill_partition.svg)
