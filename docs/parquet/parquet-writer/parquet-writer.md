---
layout: page
title: Parquet writer
nav_order: 5
parent: Parquet
---

# Parquet Writer
{: .no_toc}

本文通过一个创建parquet格式文件的代码来学习 Parquet writer以及 Parquet format. 本文基于 [Parquet-mr](https://github.com/apache/parquet-mr) 1.12.0 release.

## 目录
{: .no_toc .text-delta}

1. TOC
{:toc}

## 示例代码

``` java

```

## 生成 Parquet 文件的流程

![parquet-write_flow](/docs/parquet/parquet-writer/parquet-write_flow.svg)

### ColumnChunkPageWriterStore

为每个列创建 PageWriter

### ColumnWriteStoreV2

V2 版的 ColumnWriteStore, 为每个列创建 ColumnWriterV2.

### MessageColumnIORecordConsumer

通过 ColumnWriterV2 将一行中对应列的数据写入到对应的 ValuesWriters 中

### ColumnWriterV2

每列的 Writer, 分别保存 repetition level, definition level, 数据以及数据列的统计信息.

## ParquetWriter.write

Parquet.write 是典型的生产者消费者设计模式. ParquetWriter.write 将一行数据交给 MessageColumnIORecordConsumer 去消费. MessageColumnIORecordConsumer 通过每列的 ColumnWriterV2 将每行每列数据写入到 ValuesWriter 中.

ColumnWriterV2 根据 field type 生成 dataColumn ValuesWriter

**dataColumn的类型**

| 列的类型 |  |initialWriter|fallBackWriter|
| --- | --- | --- | --- |
| BOOLEAN | RunLengthBitPackingHybridValuesWriter |||
| FIXED_LEN_BYTE_ARRAY | FallbackValuesWriter | PlainFixedLenArrayDictionaryValuesWriter|DeltaByteArrayWriter|
|BINARY|FallbackValuesWriter|PlainBinaryDictionaryValuesWriter|DeltaByteArrayWriter|
|INT32|FallbackValuesWriter|PlainIntegerDictionaryValuesWriter|DeltaBinaryPackingValuesWriterForInteger|
|INT64|FallbackValuesWriter|PlainLongDictionaryValuesWriter|DeltaBinaryPackingValuesWriterForLong|
|DOUBLE|FallbackValuesWriter|PlainDoubleDictionaryValuesWriter|DoubleByteStreamSplitValuesWriter or PlainValuesWriter|
|FLOAT|FallbackValuesWriter|PlainFloatDictionaryValuesWriter|FloatByteStreamSplitValuesWriter or PlainValuesWriter|

当 dataColumn 为 FallbackValuesWriter 时，首先会通过 initialWriter 对数据进行 encoding, 如果最后 encoding 出来的数据字节数据大于原始的数据时， 则会 fallback 到 fallBackWriter 重新对数据进行 encoding.

以 INT32 所对应的 PlainIntegerDictionaryValuesWriter 为例.

``` java
public void writeInteger(int v) {
  // 检查数据是否已经存在, 如果存在则返回数据在intDictionaryContent的索引, 如果不存在则返回 -1
  int id = intDictionaryContent.get(v); 
  if (id == -1) { // 不存在
    id = intDictionaryContent.size(); //生成数据与之对应的 index
    intDictionaryContent.put(v, id); //将数据加入到 intDictionaryContent
    dictionaryByteSize += 4; // 更新  intDictionaryContent 中数据的字节数 
  }
  encodedValues.add(id); //encodedValues依次记录数据在 intDictionaryContent 中的索引
}
```

如有以下数据

```
21111111, 21111111, 390909090, 390909090, 47766521212

intDictionaryContent: 21111111->0,  390909090->1, 47766521212->2
encodedValues: 0 0 1 1 2
```

这种编码方式有什么好处? 这种编译可以将很大的数据通过很小的数据进行表示, 如上所示, 21111111至少需要7个字节，而通过映射后 0 就可以表示 21111111. 而对于小的数可以用 bit 位来表示， 如上图的  `0 0 1 1 2` 只需要2个bit位就可以表示最大的值，因此可以用 2个字节 (其中10位) 就可表示该编码. 大大的节省的空间.

`2 << 8 | 1 << 6 | 1 << 4 | 0 << 2 | 0`

实际上在 DictionaryValuesWriter 里在获得最后的 bytes时, 就是通过 RunLengthBitPackingHybridEncoder 这么做的

## ParquetWriter.close

ParquetWriter.write 仅仅是将数据写入到 initalWriter 中. 并没有生成最后的 data bytes. 而真正触发生成最后的 data bytes, 是 ParquetWriter.close 中 writePage 来触发 getBytes 的.

### ValuesWriter.getBytes

对于是 FallbackValuesWriter, 会触发 initialWriter.getBytes.

getBytes 首先通过 RunLengthBitPackingHybridEncoder 将 DictionaryValuesWriter 里的 encodedValues 执行 RunLength以及BitPacking 混合编码方式获得最后的数据字节.

RunLengthBitPackingHybridEncoder 算法如下所示

![RunLengthBitPackingHybridEncoder](/docs/parquet/parquet-writer/parquet-RunLengthBitPackingHybridEncoder.svg)

![parquet-runlenbitpack](/docs/parquet/parquet-writer/parquet-runlenbitpack.svg)

## TODO

how to split page
how to split row group
repetition level
definition levlel.

## reference

https://yoelee.github.io/2018/04/08/2018-04-06_%E5%88%97%E5%BC%8F%E5%AD%98%E5%82%A8_Parquet/
