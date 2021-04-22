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

- ColumnChunkPageWriterStore 为每个列创建 PageWriter, 管理所有列的 PageWriter.
- ColumnWriteStoreV2 V2 版的 ColumnWriteStore, 为每个列创建 ColumnWriterV2, 管理所有的列的 ColumnWriterV2.
- MessageColumnIORecordConsumer 通过 ColumnWriterV2 将一行中每列数据写入到每列对应的 ValuesWriters 中
- ColumnWriterV2 每列的 Writer, 分别保存 repetition level, definition level, 数据以及 Statistic.

## ParquetWriter.write

Parquet.write 是典型的生产者消费者设计模式. ParquetWriter.write 将一行数据交给 MessageColumnIORecordConsumer 去消费. MessageColumnIORecordConsumer 通过每列的 ColumnWriterV2 将每行每列数据写入到 ValuesWriter 中. MessageColumnIORecordConsumer 一次只能消费一行的数据.

ColumnWriterV2 中有三个 ValuesWriter, 分别为 repetition levels, definition level, dataColumn. 后面会单独分析什么是 repetition 和 definition

dataColumn field type 相对应的 ValuesWriter 实现类，如下所示

| 列的类型 |  |initialWriter|fallBackWriter|
| --- | --- | --- | --- |
| BOOLEAN | RunLengthBitPackingHybridValuesWriter |||
| FIXED_LEN_BYTE_ARRAY | FallbackValuesWriter | PlainFixedLenArrayDictionaryValuesWriter|DeltaByteArrayWriter|
|BINARY|FallbackValuesWriter|PlainBinaryDictionaryValuesWriter|DeltaByteArrayWriter|
|INT32|FallbackValuesWriter|PlainIntegerDictionaryValuesWriter|DeltaBinaryPackingValuesWriterForInteger|
|INT64|FallbackValuesWriter|PlainLongDictionaryValuesWriter|DeltaBinaryPackingValuesWriterForLong|
|DOUBLE|FallbackValuesWriter|PlainDoubleDictionaryValuesWriter|DoubleByteStreamSplitValuesWriter or PlainValuesWriter|
|FLOAT|FallbackValuesWriter|PlainFloatDictionaryValuesWriter|FloatByteStreamSplitValuesWriter or PlainValuesWriter|

当 dataColumn 为 FallbackValuesWriter 时. 首先会通过 initialWriter 对数据进行 encoding, 如果最后 encoding 出来的数据字节数据大于原始的数据时， 则会 fallback 到 fallBackWriter 重新对数据进行 encoding.

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

## ParquetWriter.close 将数据写入到文件

ParquetWriter.close 有两个作用， 一个是 flushRowGroupToStore 将 RowGroups 写入到文件， 另一个是将 ColumnIndex/OffsettIndex 以及 Footer 等写入到文件.

### flushRowGroupToStore

ParquetWriter.write 仅仅是将数据写入到 initalWriter 中(这里假设 ParquetWriter.write时不会分页和分RowGroup, 实际上是可能分页和分RowGroup的), 并没有生成最后的数据字节流. 而 flushRowGroupToStore 将会生成最后的 RowGroup 最终数据, 并将 RowGroup 写入到文件中.

1. ColumnWriteStoreV2 生成 page 数据 和 dictionaryPage 数据

    ColumnWriterStoreV2.flush 触发所有列的 ColumnWriterV2 将 repetition/definition/data 通过writePage写入到 Page中, 然后再写入  DictionaryPage.

    ``` java
    public void flush() {
      for (ColumnWriterV2 memColumn : columns.values()) {
        long rows = rowCount - memColumn.getRowsWrittenSoFar();
        if (rows > 0) {
          memColumn.writePage(rowCount); //先将page保存到PageWriter里的 buffer 中
        }
        memColumn.finalizeColumnChunk(); // 再生成 Dictionary 数据 
      }
    }
    ```

    - writePage

      如下代码所示

      ```java
      void writePage(int rowCount, int valueCount, Statistics<?> statistics,ValuesWriter repetitionLevels,
          ValuesWriter definitionLevels, ValuesWriter values) throws IOException {
        // TODO: rework this API. The bytes shall be retrieved before the encoding (encoding might be different otherwise)
        BytesInput bytes = values.getBytes(); // 获得 values 的字节流
        Encoding encoding = values.getEncoding(); // Values 的 encoding方式,如果是RLE-DICTIONARY编码,后面在写page后会写入dictionary page
        pageWriter.writePageV2(
            rowCount,
            Math.toIntExact(statistics.getNumNulls()),
            valueCount,
            repetitionLevels.getBytes(),
            definitionLevels.getBytes(),
            encoding,
            bytes,
            statistics); //统计信息
      }
      ```

    - ValuesWriter.getBytes 获得编码后的数据

      以 INT32 为例, dataColumn是FallbackValuesWriter, 首先触发 initialWriter.getBytes 获得编码后的数据字节流.

      getBytes 首先通过 RunLengthBitPackingHybridEncoder 将 DictionaryValuesWriter 里的 encodedValues 执行 RunLength以及BitPacking 混合编码方式获得最后的数据字节.

      RunLengthBitPackingHybridEncoder 算法如下所示

      ![RunLengthBitPackingHybridEncoder](/docs/parquet/parquet-writer/parquet-RunLengthBitPackingHybridEncoder.svg)

      RunLengthBitPackingHybridEncoder 编码示例

      ![parquet-runlenbitpack](/docs/parquet/parquet-writer/parquet-runlenbitpack.svg)

      如果 initalWriter 对数据索引(encodedValues)编码后的字节大小 encodedSize 与 dictionaryByteSize (真实的数据) 之和 大于原始的数据字节大小，则说明 initialWriter 的编码方式不优，这时候触发 fallBackWriter. 注意， fallback 只在写第一个 Page 的时候才有可能触发，如果已经触发 fallback 了，写后面的page时，都使用 fallback 编码.

      ``` java
      public boolean isCompressionSatisfying(long rawSize, long encodedSize) {
        return (encodedSize + dictionaryByteSize) < rawSize;
      }
      ```

      对于 INT32, fallBackWriter 是 DeltaBinaryPackingValuesWriterForInteger. 那它的编码方式如下

      ![parquet-deltaDictionary.svg](/docs/parquet/parquet-writer/parquet-deltaDictionary.svg)

    - writePageV2

      ``` java
      public void writePageV2(
          int rowCount, int nullCount, int valueCount,
          BytesInput repetitionLevels, BytesInput definitionLevels,
          Encoding dataEncoding, BytesInput data,
          Statistics<?> statistics) throws IOException {
        pageOrdinal++;
        
        int rlByteLength = toIntWithCheck(repetitionLevels.size()); //repetition level长度
        int dlByteLength = toIntWithCheck(definitionLevels.size()); //definition level
        int uncompressedSize = toIntWithCheck(
            data.size() + repetitionLevels.size() + definitionLevels.size()
        ); //未压缩的数据长度
        // TODO: decide if we compress
        BytesInput compressedData = compressor.compress(data); //只压缩数据，如 snappy压缩
        if (null != pageBlockEncryptor) { // 对压缩后的数据进行加密
          AesCipher.quickUpdatePageAAD(dataPageAAD, pageOrdinal);
          compressedData = BytesInput.from(pageBlockEncryptor.encrypt(compressedData.toByteArray(), dataPageAAD));
        }
        int compressedSize = toIntWithCheck( // 获得压缩后大小
            compressedData.size() + repetitionLevels.size() + definitionLevels.size());
        tempOutputStream.reset(); // page header 会写入到 tempOutputStream
        if (null != headerBlockEncryptor) {
          AesCipher.quickUpdatePageAAD(dataPageHeaderAAD, pageOrdinal);
        }
        parquetMetadataConverter.writeDataPageV2Header( //写入 page header
            uncompressedSize, compressedSize,
            valueCount, nullCount, rowCount,
            dataEncoding,
            rlByteLength,
            dlByteLength,
            tempOutputStream,
            headerBlockEncryptor,
            dataPageHeaderAAD);
        this.uncompressedLength += uncompressedSize; //记录所有的 uncompressedLength
        this.compressedLength += compressedSize; // 记录所有的 compressedLength
        this.totalValueCount += valueCount; //记录所有的行数
        this.pageCount += 1;  //记录所有的 page count
        // Copying the statistics if it is not initialized yet so we have the correct typed one
        if (totalStatistics == null) {
          totalStatistics = statistics.copy();
        } else {
          totalStatistics.mergeStatistics(statistics); // 记录所有页的统计信息
        }
        columnIndexBuilder.add(statistics); //当前页的统计信息
        offsetIndexBuilder.add(toIntWithCheck((long) tempOutputStream.size() + compressedSize), rowCount); //offset index
        // by concatenating before collecting instead of collecting twice,
        // we only allocate one buffer to copy into instead of multiple.
        buf.collect( // 组装 Page 而, PageHeader + repetition + definition + compressedData
            BytesInput.concat(
                BytesInput.from(tempOutputStream),
                repetitionLevels,
                definitionLevels,
                compressedData)
        );
        dataEncodings.add(dataEncoding); //记录所有的数据 encoding 方式
      }
      ```

    - writeDictionaryPage 获得 Dictionary 字节流

      RunLengthBitPackingHybridEncoder编码 只对数据在 Dictionary 的索引进行编码，它并不知道真正的数据是什么，所以需要在 Parquet 文件中记录真实的数据, 也就是 Dictionary 的数据.

      如上面 RunLengthBitPackingHybridEncoder sample 所示，对于下面数据，它的 Dictionary 数据为

      ```
      value: 7 7 7 7 7 7 7 7 7 3 7 7 7 7 7 7 7 7 2 3 3 2 3 2 8 8 8 8 8 8 8 8 8
      dictionary bytes:  7 0 0 0 3 0 0 0 2 0 0 0 8 0 0 0 //一个数据点4个字节
      ```

2. writeToFileWriter 将 dictionary 和 page 数据写入到文件中

### 写入 ColumnIndex/OffsetIndex以及 Footer

当把所有的RowGroup写入到文件后，紧接着需要写入 ColumnIndex/OffsetIndex 以及整个 parquet 文件的 Footer 信息了

``` java
  public void end(Map<String, String> extraMetaData) throws IOException {
    state = state.end();
    serializeColumnIndexes(columnIndexes, blocks, out, fileEncryptor); //写入 ColumnIndex
    serializeOffsetIndexes(offsetIndexes, blocks, out, fileEncryptor); //写入 OffsetIndex
    serializeBloomFilters(bloomFilters, blocks, out, fileEncryptor); // 写入 BloomFilters
    LOG.debug("{}: end", out.getPos());
    this.footer = new ParquetMetadata(new FileMetaData(schema, extraMetaData, Version.FULL_VERSION), blocks); 
    serializeFooter(footer, out, fileEncryptor); // 写入 Footer
    out.close();
  }
```

### Properties

|Parquet文件的配置项|默认值|意义|
|---|----|----|
|minRowCountForPageSizeCheck| 10 | 每当写入多少行后开始检查是否需要将数据写入到 Page. 因为 ColumnWriterV2将数据写入内存，当写入数据非常多时可能会OOM,所以设置一个值去多久去检查remaining mem, 以及 是否到pageRowCountLimit|
|pageRowCountLimit|20,000| 每个page 的行数 |
|pageSizeThreshold|1K| page 的大小|
|dictionaryPageSizeThreshold| dictionary page大小|





## TODO

how to split page
how to split row group
repetition level
definition levlel.

## reference

https://yoelee.github.io/2018/04/08/2018-04-06_%E5%88%97%E5%BC%8F%E5%AD%98%E5%82%A8_Parquet/
