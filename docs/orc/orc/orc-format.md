---
layout: page
title: ORC format
nav_order: 5
parent: ORC 
---

# ORC Format
{: .no_toc}

本文通过创建 ORC 文件的代码来学习 ORC writer 以及 ORC format. 本文基于 [Orc](https://github.com/apache/orc) 1.13.0-SNAPSHOT.

## 目录
{: .no_toc .text-delta}

1. TOC
{:toc}

## ORC write

### 示例代码

下面的代码用来创建一个 `/tmp/my-file.orc` 文件

``` java
TypeDescription schema =
    TypeDescription.fromString("struct<x:int,y:string>");
File file = new File("/tmp/my-file.orc");
if (file.exists()) {
  file.delete();
}

Path fsPath = new Path(file.getAbsolutePath());
try (Writer writer = OrcFile.createWriter(fsPath,
    OrcFile.writerOptions(new Configuration())
        .rowIndexStride(3)
        .compress(CompressionKind.NONE)
        .setSchema(schema))) {
  VectorizedRowBatch batch = schema.createRowBatch();
  LongColumnVector x = (LongColumnVector) batch.cols[0];
  BytesColumnVector y = (BytesColumnVector) batch.cols[1];

  for (int r = 12; r < 18; ++r) {
    int row = batch.size++;
    x.vector[row] = r;
    if (row == 1) {
      x.isNull[row] = true;
      x.noNulls = false;
    }
    byte[] buffer = ("Last-" + (r * 3)).getBytes(StandardCharsets.UTF_8);
    y.setRef(row, buffer, 0, buffer.length);
    if (row == 2) {
      y.isNull[row] = true;
      y.noNulls = false;
    }
    // If the batch is full, write it out and start over.
    if (batch.size == batch.getMaxSize()) {
      writer.addRowBatch(batch);
      batch.reset();
    }
  }
  if (batch.size != 0) {
    writer.addRowBatch(batch);
  }
}
```

上面的示例代码用于创建两列 x, y. 总共写入 6 行数据.

整个 write 的流程图如下所示

![write](/docs/orc/orc/orc-write.svg)

### TypeDescription

| field | |
| -- | --|
| int id | column id|
| category | column的类型 |
| List<String> fieldNames |只有STRUCT才有 fieldNames |
| List<TypeDescription> children | STRUCT/LIST/MAP 都有 children |

TypeDescription 表示 ORC 文件的 schema 信息, TypeDescription中的 id 表示的是ORC实现中的 column id,
而非我们一般认为的 column id, 比如 "struct<x:int,y:string>" 物理上只有两列, 一列 x, 另一列 y. 但是在 ORC实现中,
用３列来表示,即 STRUCT/MAP/LIST 这样的复合结构都会有单独的列表示.

TypeDescription是一个树型结构. 在设置 TypeDescription 的 id 时,采用的是深度优先依次递归设置 id. 如下图所示,

![TypeDescription](/docs/orc/orc/orc-TypeDescription.svg)

整个 schema 有多少列可以通过 `schema.getMaximumId() + 1` 来获得.

### Stream

Stream 保存着 TreeWriter 写入的临时数据.

|field|意义|
|---|---|
|ByteBuffer current|保存序列化好但是 uncompress的数据 |
|ByteBuffer compressed|保存序列化好 compress的数据 |
|compressedBytes| 压缩后的数据大小 |
|uncompressedBytes| 未压缩的数据大小|
|byte[] iv| 初始化的 vector,只有当需要 encrypted 时才有值,否则为 null|

不管是 uncompress 还是 compress, 前３个字节是 HEADER

``` java
private static void writeHeader(ByteBuffer buffer,
                                int position,
                                int val,
                                boolean original) {
  // ３个字节保存 保存 HEADER, val 一般是 buffer size. original 表示此数据是压缩后还是没有压缩
  buffer.put(position, (byte) ((val << 1) + (original ? 1 : 0)));
  buffer.put(position + 1, (byte) (val >> 7));
  buffer.put(position + 2, (byte) (val >> 15));
}
```

即使 Stream 有 codec, 如果压缩后的数据长度大于未压缩的数据长度,则stream**会使用非压缩**的数据. 一个 Stream 是否是压缩的,
可以从第一个字节最后一个 bit 来判断.

### TreeWriter

![TreeWriter](/docs/orc/orc/orc-TreeWriter.svg)

ORC 根据 schema 信息为每个 column 创建 TreeWriter, TreeWriter 最终将数据写入到 Stream 中. 每个 TreeWriter 至少包含
PRESENT/ROW_INDEX stream. PRESENT stream 表示某些行是否为 NULL, 如果某列的数据都不为 NULL, TreeWriter 不会将 PRESENT stream
flush到 ORC 文件中. ROW_INDEX 表示的是 ORC stripe 中 Row Group 的 statistics信息, 当我们将 rowsInStripe 设置为 0 时
此时也不会创建 ROW_INDEX 信息.

| field | |
| --- | -- |
| id | 与 schema column id 一一对应 |
| boolean foundNulls | 该列是否有 null 的行, 如果没有 null 的行,则不会将 PRESENT Stream 写入到文件 |
| rowIndex | Row Index 信息 |

对于 STRUCT 列,创建 StructTreeWriter, 接着为其 children x 创建 IntegerTreeWriter, y 创建 StringTreeWriter.
IntegerTreeWriter只一个 DATA stream, 对 x 列写入数据最后都是写入到该 DATA stream 中. StringTreeWriter 有 DATA/DICTIONARY_DATA/LENGTH Stream.
对 y 写入数据最终是写入到上述的几个 stream 中.

### Stripe

ORC 文件由一组 的 Stripe 组成, Stripe类似于 Parquet 格式中的 RowGroup. ORC 中, stripe默认为64M, 由 `orc.stripe.size` 指定.
Stripe 又和 RowGroup 不完全一样, 每个 Stripe 有自己的 StripeFooter, Parquet中的RowGroup没有footer信息.

Stripe 由 `Index Stream (unencrypted + encrypted) + DATA Stream (unencrypted + encrypted) + StripeFooter` 组成, 

一个 Stripe 由多个 RowGroup 组成, 每个 RowGroup 默认包含 10,000 行. 由 `orc.row.index.stride` 控制.
ORC中 RowIndexEntry 表示一个 RowGroup 信息, 它保存了该 RowGroup的 统计信息以及位置信息.

统计信息也就是该 RowGroup 里 max/min/sum 等, 而位置信息, 标识该 RowGroup 中isPresent信息以及真实数据信息, 如
uncompressedSize/compressedSize/RowGroup中的行数等

``` protobuf
message RowIndexEntry {
  repeated uint64 positions = 1 [packed=true]; // 记录 isPresent 以及真实数据的相关信息
  optional ColumnStatistics statistics = 2; // Row Group index
}
                                                                                                                      
message RowIndex {
  repeated RowIndexEntry entry = 1; //repeated表明一个 Stripe中有多少个 RowIndex
}
```

### encryption

ORC 允许单独为某些列设置 encryption 加密, 如下代码所示

``` java
TypeDescription schema =
    TypeDescription.fromString("struct<i:int,norm:int,x:array<string>>");

byte[] piiKey = new byte[16];
random.nextBytes(piiKey); //生成一组 piiKey
byte[] creditKey = new byte[32];
random.nextBytes(creditKey); //生成一组 creditKey
InMemoryKeystore keys = new InMemoryKeystore(random)
    .addKey("pii", EncryptionAlgorithm.AES_CTR_128, piiKey)
    .addKey("credit", EncryptionAlgorithm.AES_CTR_256, creditKey);
Writer writer = OrcFile.createWriter(testFilePath,
    OrcFile.writerOptions(conf)
        .setSchema(schema)
        .version(fileFormat)
        .setKeyProvider(keys)
        .encrypt("pii:i;credit:x")); // 用 pii key 对 column i 进行加密, 用 credit 对 x 进行加密.
```

设置一组 key, pii AES_CTR_128 对 column i 进行加密, credit AES_CTR_256 对 column x 进行加密.

对应的读代码也需要设置相应的 keys 才能正常读取出来.

``` java
Reader reader = OrcFile.createReader(testFilePath,
    OrcFile.readerOptions(conf)
           .setKeyProvider(keys));
```

encryption的相关信息保存在 File Footer 的 Encryption 中.

### compression

ORC writer 如果选择了 compression(由 orc.compress 控制，默认为 zlib). 则 ORC文件除了 HEADER 与 PostScript 外，其它部分
(所有的Stream/MetaInfo/Footer/StripeFooter) 都会采用该 compress 算法进行分别压缩. Compress 的相关信息保存在 PostScript 中.

## ORC format

通过上面的示例代码写出来 ORC 文件如下所示.

![orc format](/docs/orc/orc/orc-orc-format.svg)

## ORC read

![orc read](/docs/orc/orc/orc-read.svg)

Orc read是对 ORC 文件进行解析, 附带着 Column Prune以及 Predict Push Down.
