---
layout: page
title: Parquet writer
nav_order: 5
parent: Parquet
---

# Parquet Writer
{: .no_toc}

本文通过创建 parquet 格式文件的代码来学习 Parquet writer 以及 Parquet format. 本文基于 [Parquet-mr](https://github.com/apache/parquet-mr) 1.13.0-SNAPSHOT release.

## 目录
{: .no_toc .text-delta}

1. TOC
{:toc}

## 示例代码

下面的代码用来创建一个 `/tmp/test-x1231.parquet` 文件

``` java
    MessageType schema = MessageTypeParser.parseMessageType(
        "message Document {\n" +
            "     required int32 DocId;\n" +
            "     optional group Links {\n" +
            "         repeated int32 Backward;\n" +
            "         repeated int32 Forward; }\n" +
            "     repeated group Name {\n" +
            "         repeated group Language {\n" +
            "             required binary Code;\n" +
            "             optional binary County; }\n" +
            "         optional binary Url; }}");

    File file = new File("/tmp/test-x1231.parquet");
    if (file.exists()) {
      file.delete();
    }
    Path fsPath = new Path(file.getAbsolutePath());
    SimpleGroupFactory factory = new SimpleGroupFactory(schema);

    ParquetWriter writer = ExampleParquetWriter.builder(fsPath)
        .withType(schema)
        .withWriterVersion(ParquetProperties.WriterVersion.PARQUET_2_0)
        .build();

    // Group 0
    Group group = factory.newGroup()
        .append("DocId", 10);
    group
        .addGroup("Links")
            .append("Forward", 20)
            .append("Forward", 40)
            .append("Forward", 60);

    Group name1 = group
        .addGroup("Name")
            .append("Url", "http://A");
        name1
            .addGroup("Language")
                .append("Code", "en-us")
                .append("County", "us");
        name1
            .addGroup("Language")
                .append("Code", "en");
    group
        .addGroup("Name")
            .append("Url", "http://B");
    group
        .addGroup("Name")
            .addGroup("Language")
                .append("Code", "en-gb")
                .append("County", "gb");
    // Group 0 --------------

    // Group 1 ++++++++++++++
    Group group1 = factory.newGroup()
        .append("DocId", 20);
    group1
        .addGroup("Links")
            .append("Backward", 10)
            .append("Backward", 30)
            .append("Forward", 80);
    group1
        .addGroup("Name")
            .append("Url", "http://C");
    // Group 1 --------------

    System.out.println(schema);
    writer.write(group);
    writer.write(group1);
    writer.close();
```

## Schema

通过 Parquet 自带的 `MessageTypeParser.parseMessageType` 可以将 Schema 描述语言解析成 Parquet MessageType.

``` java
MessageType schema = MessageTypeParser.parseMessageType(
    "message Document {\n" +
    "     required int32 DocId;\n" +
    "     optional group Links {\n" +
    "         repeated int32 Backward;\n" +
    "         repeated int32 Forward; }\n" +
    "     repeated group Name {\n" +
    "         repeated group Language {\n" +
    "             required binary Code;\n" +
    "             optional binary County; }\n" +
    "         optional binary Url; }}");
```

- required: 表示该字段出现一次
- optional: 表示该字段出现 0 次或 1 次
- repeated: 表示该字段出现 0 次或 多次

![Message Type](/docs/parquet/parquet-writer/parquet-MessageType.svg)

### repetition与definition

数据本身是无法表达一行的结构信息， 如 [Name, Language, Code]中包含2个repeated字段, Name和Language都可以多次重复，
而所有Code数据是 flatten 到一个column中的，光靠 Code 我们无法得知到底Code是属于哪个Name.Language.

Google dremel 引入了 repetition 概念, [Name, Language, Code]包含 2 个 repeated字段 (Name 和 Language), 因此Code
的 repetition level 范围是 0~2, 0 表示创建一个新的Row, 1表示在Names处重复, 2表示在Names.Language重复.

对于数据在哪层上为 NULL, dremel同样引入了 defintition level 来表示

如 [Name, Language, County], 它的最大 definition level 为 3 (Name是repeated, Language也是repeated, Code为 optional)
当 Code 有值时，它的 definition=Max Definition(3), 当 Code 没有值时, d=2表示 Name.Language有定义但是Code=NULL,
d=1表示 Name有定义但是Name.Language=NULL, d=0时，表示Name=NULL.

可以通过下面的计算可以获得一个Primitive column的最大 definition与repetition

``` c
Max Definition = sizeof(optional) + sizeof(repeated)
Max Repetition = sizeof(repeated)
```

| Column Path              | Max Definition Level   | Max Repetition Level |
| ---                      | -----                  | -----                |
| [DocId]                  | 0 ( required)          | 0                    |
| [Links, Backward]        | 2                      | 1                    |
| [Links, Forward]         | 2                      | 1                    |
| [Name, Language, Code]   | 2 (Code is required)   | 2                    |
| [Name, Language, County] | 3 (County is optional) | 2                    |
| [Name, Url]              | 2                      | 1                    |

以数据为例

- [DocId]

  | value | r  | d  |
  | ----  | --- | --- |
  | 10    | 0  | 0  |
  | 20    | 0  | 0  |

  对于 `required int32 DocId`, r/d 没有意义, 它必须存在且不能为空.

- [Links, Backward]

  | value | r  | d  |
  | ----  | -- | -- |
  | NULL  | 0  | 1  |
  | 10    | 0  | 2  |
  | 30    | 1  | 2  |

  Links 声明为 Optional, 最多只出现 1 次, 而 Backward声明为 repeated, 可以出现多次. Links.Backward 包含一个 repeated 字段
  因此它的 repetition level 范围是 0~1, 0表明创建一个新的行, 1表示在 Links.Backward 重复.

  d=1 表示 Links.Backward = NULL, d=Max_Definition_Level 表示该行是有值的.

- [Links, Forward]

  | value | r  | d  | Row  ||
  | ----  | -- | -- | ---  | --- |
  | 20    | 0  | 2  | Row1 | r=0 表示创建 Row1, d=Max definition(2) 表示[Links, Forward]此时为 [20] |
  | 40    | 1  | 2  | Row1 | r=1 表示在Forward处重复, 即添加一个新元素, d=Max definition(2), 表示[Links, Forward]此时为 [20, 40]|
  | 60    | 1  | 2  | Row1 | r=1 表示在Forward处重复, 即添加一个新元素, d=Max definition(2), 表示[Links, Forward]此时为 [20, 40, 60]|
  | 80    | 0  | 2  | Row2 | r=0 表示创建 Row2, d=Max definition(2), 表示[Links, Forward] 此时为 [80]|

- [Name, Language, Code]

  | value | r  | d  | Row  ||
  | ----  | -- | -- | ---  | --- |
  | en-us | 0  | 2  | Row1 | r=0, 创建 Row1, d=Max definition(2), 表示[Name, Language, Code] 此时为[en-us]|
  | en    | 2  | 2  | Row1 | r=2表示在Language处进行重复, d=Max definition(2), 表示此时Name.Language.Code=[en]|
  | _NULL_| 1  | 1  | Row1 | r=1表示在Names处进行重复, d=1 表示第一级为 Null, 即Name.Language=NULL|
  | en-gb | 1  | 2  | Row1 | r=1表示在Names处进行重复, d=Max definition(2), 表示此时Name.Language.Code=[en-gb]|
  | _NULL_| 0  | 1  | Row2 | r=0, 创建新的Row2, d=1 表示第一级为 NULL, 即 Name.Language=NULL|

- [Name, Language, County]

  | value | r  | d  | Row  ||
  | ----  | -- | -- | ---  | --- |
  | us    | 0  | 3  | Row1 | r=0, 创建 Row1, d=Max definition(3), 表示[Name, Language, Code] 此时为[us]|
  | _NULL_| 2  | 2  | Row1 | r=2表示在Name.Language处进行重复, d=2, 表示此时Name.Language.Code = null|
  | _NULL_| 1  | 1  | Row1 | r=1表示在Name处进行重复, d=1, 表示此时Name.Language = null|
  | gb    | 1  | 3  | Row1 | r=1表示在Names处进行重复, d=Max definition(3), 表示[Name, Language, Code] 此时为[gb]|
  | _NULL_| 0  | 1  | Row2 | r=0创建新的Row2, d=1 表示第一级为 NULL, 即Name.Language=NULL|

- [Name, Url]

  | value       | r  | d  | Row  ||
  | ----        | -- | -- | ---  | --- |
  | http://A    | 0  | 2  | Row1 | r=0, 创建 Row1, d=Max definition(2), 表示[Name, Url] 此时为[http://A]|
  | http://B    | 1  | 2  | Row1 | r=1表示在Name处进行重复, d=2, 表示此时[Name, Url] = [http://B]|
  | _NULL_      | 1  | 1  | Row1 | r=1表示在Name处进行重复, d=1, 表示此时Name.Url = null|
  | http://C    | 0  | 2  | Row2 | r=0创建新的Row2, d=Max definition(2), 表示[Name, Url] 此时为[http://C]|

## 生成 Parquet 文件的流程

![parquet-write_flow](/docs/parquet/parquet-writer/parquet-write_flow.svg)

- ColumnChunkPageWriterStore

  为每个列创建 PageWriter, 管理所有列的 PageWriter.

- ColumnWriteStoreV2

  V2 版的 ColumnWriteStore, 为每个列创建 ColumnWriterV2, 管理所有的列的 ColumnWriterV2.

- MessageColumnIORecordConsumer

  通过 ColumnWriterV2 将一行中每列数据写入到每列对应的 ValuesWriters 中

- ColumnWriterV2

  每列的 Writer, 分别保存 repetition level, definition level, 数据以及 Statistic.

## ParquetWriter.write

Parquet.write 是典型的生产者消费者设计模式. ParquetWriter.write 将一行数据交给 MessageColumnIORecordConsumer 去消费.
MessageColumnIORecordConsumer 通过每列的 ColumnWriterV2 将每行每列数据写入到 ValuesWriter 中.
MessageColumnIORecordConsumer 一次只能消费一行的数据.

ColumnWriterV2 中有三个 ValuesWriter, 分别为 dataColumn, repetition levels, definition level.

### dataColumn

dataColumn 用于保存实际的数据, 它会根据 column type 选择不同 ValuesWriter，如下所示

| column type | dataColumn |initialWriter|fallBackWriter|
| ---         | ---        | ---         | ---          |
| BOOLEAN | RunLengthBitPackingHybridValuesWriter |无|无|
| FIXED_LEN_BYTE_ARRAY | FallbackValuesWriter | PlainFixedLenArrayDictionaryValuesWriter|DeltaByteArrayWriter|
|BINARY|FallbackValuesWriter|PlainBinaryDictionaryValuesWriter|DeltaByteArrayWriter|
|INT32|FallbackValuesWriter|PlainIntegerDictionaryValuesWriter|DeltaBinaryPackingValuesWriterForInteger|
|INT64|FallbackValuesWriter|PlainLongDictionaryValuesWriter|DeltaBinaryPackingValuesWriterForLong|
|DOUBLE|FallbackValuesWriter|PlainDoubleDictionaryValuesWriter|DoubleByteStreamSplitValuesWriter or PlainValuesWriter|
|FLOAT|FallbackValuesWriter|PlainFloatDictionaryValuesWriter|FloatByteStreamSplitValuesWriter or PlainValuesWriter|

ValuesWriter 的类继承图如下所示

![parquet](/docs/parquet/parquet-writer/valueswriter.png)

当 dataColumn 为 FallbackValuesWriter 时. 首先会通过 initialWriter 对数据进行 encoding, 如果最后 encoding
出来的数据字节数大于原始的字节数时， 则会 fallback 到 fallBackWriter 重新对数据进行 encoding.

以 INT32 所对应的 PlainIntegerDictionaryValuesWriter 为例. PlainIntegerDictionaryValuesWriter 继承于 DictionaryValuesWriter,
它将实际的数据映射到从0开始的较小的数,

``` java
public void writeInteger(int v) {
  // 检查数据是否已经存在, 如果存在则返回数据在intDictionaryContent的索引, 如果不存在则返回 -1
  int id = intDictionaryContent.get(v);
  if (id == -1) { // 不存在
    id = intDictionaryContent.size(); //生成数据与之对应的 index
    intDictionaryContent.put(v, id); //将数据加入到 intDictionaryContent
    dictionaryByteSize += 4; // 更新 intDictionaryContent 中数据的字节数
  }
  encodedValues.add(id); //encodedValues依次记录数据在 intDictionaryContent 中的索引
}
```

如有以下数据

``` console
21111111, 21111111, 390909090, 390909090, 47766521212

intDictionaryContent: 21111111->0, 390909090->1, 47766521212->2
encodedValues: 0 0 1 1 2
```

这种编码方式有什么好处呢?

这种编码可以将很大的数据通过很小的数据进行表示, 如上所示,
21111111至少需要7个字节，而通过映射后 0 就可以表示 21111111. 而对于较小的数可以用
bit 位来表示， 如上图的 `0 0 1 1 2` 只需要2个bit位就可以表示最大的值，因此可以用
2个字节 (其中10位) 就可表示该编码. 大大的节省的空间.

`2 << 8 | 1 << 6 | 1 << 4 | 0 << 2 | 0`

实际上在 DictionaryValuesWriter.getBytes() 里在获得bytes时, 就是通过
RunLengthBitPackingHybridEncoder 这么做的.

而数据的真实值此时保存在 intDictionaryContent 中， 最后将 intDictionaryContent 保存到该列的 DictionaryPage 中

## ParquetWriter.close 将数据写入到文件

ParquetWriter.close 有两个作用，

- 一个是 flushRowGroupToStore 将 RowGroups 写入到文件,
- 另一个是将 ColumnIndex/OffsettIndex 以及 Footer 等写入到文件.

### flushRowGroupToStore

ParquetWriter.write 仅仅是将数据写入到 initalWriter 中(这里假设 ParquetWriter.write时不会分页和分RowGroup,
实际上是可能分页和分RowGroup的), 并没有生成最后的数据字节流. 而 flushRowGroupToStore 将会生成最后的
RowGroup 最终数据, 并将 RowGroup 写入到文件中.

1. ColumnWriteStoreV2 生成 page 数据 和 dictionaryPage 数据

    ColumnWriterStoreV2.flush 触发所有列的 ColumnWriterV2 将 repetition/definition/dataColumn 通过writePage写入到 Page中,
    然后再写入 DictionaryPage.

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

      getBytes 首先通过 RunLengthBitPackingHybridEncoder 将 DictionaryValuesWriter 里的 encodedValues 执行
      RunLength以及BitPacking 混合编码方式获得最后的数据字节.

      RunLengthBitPackingHybridEncoder 算法如下所示

      ![RunLengthBitPackingHybridEncoder](/docs/parquet/parquet-writer/parquet-RunLengthBitPackingHybridEncoder.svg)

      RunLengthBitPackingHybridEncoder 编码示例

      ![parquet-runlenbitpack](/docs/parquet/parquet-writer/parquet-runlenbitpack.svg)

      如果 initalWriter 对数据索引(encodedValues)编码后的字节大小 encodedSize 与 dictionaryByteSize (真实的数据大小)
      之和 大于原始的数据字节大小，则说明 initialWriter 的编码方式不优，这时候触发 fallBackWriter. 注意， fallback
      只在写第一个 Page 的时候才有可能触发，如果已经触发 fallback 了，写后面的page时，都使用 fallback 编码.

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
        this.pageCount += 1; //记录所有的 page count
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

      RunLengthBitPackingHybridEncoder编码 只对数据在 Dictionary 的索引进行编码，它并不知道真正的数据是什么,
      所以需要在 Parquet 文件中记录真实的数据, 也就是 Dictionary 的数据.

      如上面 RunLengthBitPackingHybridEncoder sample 所示，对于下面数据，它的 Dictionary 数据为

      ``` console
      value: 7 7 7 7 7 7 7 7 7 3 7 7 7 7 7 7 7 7 2 3 3 2 3 2 8 8 8 8 8 8 8 8 8
      dictionary bytes: 7 0 0 0 3 0 0 0 2 0 0 0 8 0 0 0 //一个数据点4个字节
      ```

2. writeToFileWriter 将 dictionary 和 page 数据写入到文件中

### 写入 ColumnIndex/OffsetIndex以及 Footer

当把所有的RowGroup写入到文件后，紧接着需要写入 ColumnIndex (每个 Page里每列的统计信息)/OffsetIndex(每个Page中每列的 offset信息)
以及整个 parquet 文件的 Footer 信息了.

``` java
  public void end(Map<String, String> extraMetaData) throws IOException {
    state = state.end();
    serializeColumnIndexes(columnIndexes, blocks, out, fileEncryptor); //写入 ColumnIndex
    serializeOffsetIndexes(offsetIndexes, blocks, out, fileEncryptor); //写入 OffsetIndex
    serializeBloomFilters(bloomFilters, blocks, out, fileEncryptor); // 写入 BloomFilters
    this.footer = new ParquetMetadata(new FileMetaData(schema, extraMetaData, Version.FULL_VERSION), blocks);
    serializeFooter(footer, out, fileEncryptor); // 写入 Footer
    out.close();
  }
```

## Parquet format

最后整个 Parquet 文件的格式如下所示

![parquet](/docs/parquet/parquet-writer/parquet-finaldata.svg)

## reference

- [https://static.googleusercontent.com/media/research.google.com/en//pubs/archive/36632.pdf](https://static.googleusercontent.com/media/research.google.com/en//pubs/archive/36632.pdf
)
- [https://blog.twitter.com/engineering/en_us/a/2013/dremel-made-simple-with-parquet.html](https://blog.twitter.com/engineering/en_us/a/2013/dremel-made-simple-with-parquet.html)
- [https://bigdata.devcodenote.com/2015/04/parquet-file-format.html](https://bigdata.devcodenote.com/2015/04/parquet-file-format.html)
