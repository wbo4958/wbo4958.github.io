---
layout: page
title: Parquet format
nav_order: 105
parent: Parquet
---

# Parquet format
{: .no_toc}

## 目录
{: .no_toc .text-delta}

1. TOC
{:toc}

## FileMetaData

描述整个 Parquet file 的信息

|类型|变量名|字节数|含义|
|--|--|--|--|
|i32 |version| 4 | parquet file version |
|list\<SchemaElement\> |schema||保存parquet文件的schema|
|i64| num_rows|64|parquet文件总行数|
|list\<RowGroup\>| row_groups||parquet文件中 RowGroup信息|
|optional list\<KeyValue\>| key_value_metadata||extra key/value meta|
|optional string| created_by||create by 信息|
|optional list\<ColumnOrder\>| column_orders|||
|optional EncryptionAlgorithm| encryption_algorithm||文件加密算法|
|optional binary| footer_signing_key_metadata||用于footer签名的信息|

### RowGroup

RowGroup 信息

|类型|变量名|字节数|含义|
|--|--|--|--|
|list\<ColumnChunk\>| columns|  |RowGroup中column meta信息 |
|i64| total_byte_size|8|RowGroup中所有列的未压缩的数据大小之和|
|i64| num_rows | 8| row group中多少行|
|optional list\<SortingColumn\>| sorting_columns||RowGroup 里 Row 的 Sort 顺序|
|i64| file_offset ||第一个 page(Dictionary或Data page)在 Parquet 文件中的偏移|
|optional i64| total_compressed_size||RowGroup中所有列的压缩后的数据之和|
|optional i16| ordinal||第几个RowGroup|

#### ColumnChunk

ColumnChunk 表示 RowGroup 中一列所有的 Page, ColumnIndex 以及 ColumnOffset 信息

|类型|变量名|字节数|含义|
|--|--|--|--|
|optional string| file_path||column数据保存在哪个文件里，默认是本文件|
|i64| file_offset|8|ColumnMetaData在file_path中的偏移|
|optional ColumnMetaData| meta_data ||Column Chunk 的MetaData |
|optional i64| offset_index_offset|| ColumnChunk's OffsetIndex 在文件中的偏移|
|optional i32| offset_index_length ||ColumnChunk's OffsetIndex 的长度|
|optional i64| column_index_offset||ColumnChunk's ColumnIndex 在文件中的偏移|
|optional i32| column_index_length ||ColumnChunk's ColumnIndex 的长度|
|optional ColumnCryptoMetaData| crypto_metadata|||
|optional binary| encrypted_column_metadata|||

#### ColumnMetaData

描述 RowGroup中某列所有的 Page 包括 Dictionary 信息

|类型|变量名|字节数|含义|
|--|--|--|--|
|Type| type|| Column的 Type, BOOLEAN,INT32 ...|
|list\<Encoding\>| encodings|||
|list\<string\>| path_in_schema|||
|CompressionCodec| codec||数据压缩算法|
|i64| num_values||Column中的数据个数|
|i64| total_uncompressed_size||一个RowGroup中一列所有page的uncompressed_size,包括header|
|i64| total_compressed_size||一个RowGroup中一列所有page的compressed_size,包括header|
|list\<KeyValue\>| key_value_metadata|||
|i64| data_page_offset||Column第一个 data page offset|
|optional i64| index_page_offset||root index page offset|
|optional i64| dictionary_page_offset||Column的Dictionary page在文件中的偏移|
|optional Statistics| statistics||一个RowGroup里一列所有Page的统计信息|
|optional list\<PageEncodingStats\>| encoding_stats|||
|optional i64| bloom_filter_offset|||

## PageHeader

PageHeader 可以描述 Parquet 的 Page. 如 Data page, Dictionary page等. 由PageType决定.

|类型|变量名|字节数|含义|
|--|--|--|--|
|PageType| type | 1| DATA_PAGE, INDEX_PAGE, DICTIONARY_PAGE, DATA_PAGE_V2|
|i32| uncompressed_page_size|4| 未被压缩的大小  |
|i32| compressed_page_size|4| 压缩后的大小  |
|optional i32| crc |4| page 的 crc 校验码, 只有在 DictionaryPage有用 |
|optional IndexPageHeader| index_page_header||index page header|
|optional DictionaryPageHeader| dictionary_page_header||DictionaryPageHeader|
|optional DataPageHeaderV2| data_page_header_v2||data page header 格式|

### DataPageHeaderV2

DataPageHeaderV2 是 V2版本的 DataPageHeader. 允许在不解压数据的情况下读取 definition/repetition level信息, 因为definition/repetition 是未压缩的原始数据.

``` console
compressed_page_size = encoded_data_size + repetition_size + definition_size
uncompressed_page_size = encoded_data_compressed_size + repetition_size + definition_size
```

|类型|变量名|字节数|含义|
|--|--|--|--|
|i32 |num_values | 4| 总共多少数据，包括nulls|
|i32 |num_nulls|4| null 的个数 |
|i32 |num_rows |4| 总共多少行数据 |
|Encoding| encoding|1|数据 encoding 方式, 非repetition/definition|
|i32 |definition_levels_byte_length|4|definition字节长度|
|i32 |repetition_levels_byte_length|4|repetition字节长度|
|optional bool | is_compressed| 1| 数据是否压缩|
|optional Statistics |statistics|| Data Page中统计信息|

### DictionaryPageHeader

对于 DICTIONARY 的编码方式，才有 DictionaryPageHeader, 而非 DICTIONARY 的编码是没有该 PageHeader的.

|类型|变量名|字节数|含义|
|--|--|--|--|
|i32| num_values |4|dictionary 里有多少数据|
|Encoding |encoding||dictionary encoding 方式|
|optional bool| is_sorted||dictionary 数据是否是已经排序过的|

### Statistics

Column的统计信息, 可能是 Page 里的，也可能是 RowGroup 中的统计信息

|类型|变量名|字节数|含义|
|--|--|--|--|
|@DEPRECATED optional binary| max||最大值. for older readers|
|@DEPRECATED optional binary| min||最小值. for older readers|
|optional i64| null_count||null个数|
|optional i64| distinct_count|||
|optional binary| max_value||column 最大值|
|optional binary| min_value||column 最小值|

## ColumnIndex

ColumnIndex 描述一个RowGroup中某一列ColumnChunk(该列在该RowGroup里所有Pages) 的索引信息，包括ColumnChunk里所有 Data Page 里的最大值、最小值，以及null..

|类型|变量名|字节数|含义|
|--|--|--|--|
|list\<bool\> |null_pages | |  min_values[N] = true 表示 page[N]里全为null值|
|list\<binary\> |min_values | | min_values[N] 表示 page[N]里的最小值 |
|list\<binary\> |max_values | | max_values[N] 表示 page[N]里的最大值 |
|BoundaryOrder |boundary_order||min_values /max_values 的顺序 ???|
|optional list\<i64\> |null_counts||可选的，null_counts[N]=M 表示 page[N]有 M 个的 null count|

## OffsetIndex

描述一个RowGroup中某一列Column 的Page offset信息

|类型|变量名|字节数|含义|
|--|--|--|--|
|list\<PageLocation\>| page_locations| | page_locations[N] 表示 page[N] 的 PageLocation  |

### PageLocation

|类型|变量名|字节数|含义|
|--|--|--|--|
|i64| offset| 8 | page 在file中的偏移 |
|i32 |compressed_page_size|4|page的大小，包括 header. compressed_page_size + header_size|
|i64 |first_row_index|8|RowGroup中page第一行的索引|

## 参考

- [https://github.com/apache/parquet-format/blob/master/src/main/thrift/parquet.thrift](https://github.com/apache/parquet-format/blob/master/src/main/thrift/parquet.thrift)
- [https://github.com/apache/parquet-format](https://github.com/apache/parquet-format)