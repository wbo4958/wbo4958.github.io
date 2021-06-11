---
layout: page
title: Parquet reader
nav_order: 20
parent: Parquet
---

# Parquet reader
{: .no_toc}

本文通过代码读取 parquet 格式文件的代码来学习 Parquet reader关于 Column Prune以及PushDonw filter.
本文基于 [Parquet-mr](https://github.com/apache/parquet-mr) 1.13.0-SNAPSHOT release.

## 目录
{: .no_toc .text-delta}

1. TOC
{:toc}

## 示例代码

``` java
    File file = new File("/tmp/test-x1231.parquet");
    if (!file.exists()) {
      assert false;
    }
    Path fsPath = new Path(file.getAbsolutePath());

    Configuration configuration = new Configuration();

    String partialSchemaStr =
        "message Document {\n" +
            "     required int32 DocId;\n" +
            "     optional group Links {\n" +
            "         repeated int32 Backward;\n" +
            "         repeated int32 Forward; }}";

    // column prune, 只读取 DocId 与 Links.Backward 以及 Links.Forward
    configuration.set(ReadSupport.PARQUET_READ_SCHEMA, partialSchemaStr);
    // push-down filter, 只读取  DocId < 13 的行
    ParquetInputFormat.setFilterPredicate(configuration, FilterApi.lt(FilterApi.intColumn("DocId"), 13));
    ParquetReader<Group> parquetReader = ParquetReader.builder(new GroupReadSupport(), fsPath)
        .withConf(configuration)
        .useColumnIndexFilter()
        .build();

    Group group;
    while ((group = parquetReader.read()) != null) {
      System.out.println(group);
    }
```

流程图如下所示

![parquet read](/docs/parquet/parquet-reader/parquet-read-ParquetReader.svg)

## FushDown filer - RowGroup filter

用户可以通过设置某列的 filter 条件来过滤 RowGroup

``` java
ParquetInputFormat.setFilterPredicate(configuration, FilterApi.lt(FilterApi.intColumn("DocId"), 13));
```

Parquet 在读取文件时会首先读取 Footer, 然后会根据下面的 filters 条件判断该 RowGroup 是否是需要的.

### Statistics Filter

Parquet写每个 RowGroup 中每列数据时，已经将该列的 max/min 以及 null count 写入到 Statistics 中了,
所以可以通过 Statistics 的 max/min 来判断是否需要继续读取该 RowGroup.

比如 Statistics 中 max = 10, min = 1, 现在 filter 查询条件是 `> 20`, 直观上可以得出 该列最大值为10
现在查询值至少是 20, 所以即使将该 RowGroup 全部读取出来也不会有 大于 20的结果，因此我们不需要读取该 RowGroup.

### Dictionary Statistics Filter

Statistics 中 Max/Min 是很粗粒度的过滤, 比如 filter 条件 是 `=5`, 如果只通过 statistics 判断的话，
那上面的 RowGroup 就会被全部读取出来，然后再将每行和 5 比较看是否是需要的结果. 如果该行没有 5 时，那该
RowGroup 就没必要读取出来.

所以此时 Dictionary Statistics 派上用处了， Dictionary 编码的条件适用于很多重复的数据，那经过 uniq 后,
真实的数据是比较少的.

Dictionary Statistics 会将所有的真实数据取出来，然后依次与 filter 条件进行比较, 比如当前 column value
为  [1, 2, 3, 7], 但是有 100,000,000 行， 这样我们发现  column value 并不包含5, 所以我们将该 RowGroup
给过滤掉， 这样可以避免读取 100,000,000 行.

### BloomFilter

如果 column 有大量离散的数据时, 一般不会采用 Dictionary 编码, 此时也就没有 Dictionary Statistics. Parquet 引入了 BloomFilter. 可以参考下面几篇文章.

- [布隆过滤器，这一篇给你讲的明明白白](https://developer.aliyun.com/article/773205)
- [https://github.com/apache/parquet-format/blob/master/BloomFilter.md](https://github.com/apache/parquet-format/blob/master/BloomFilter.md)
- [https://www.cnblogs.com/liyulong1982/p/6013002.html](https://www.cnblogs.com/liyulong1982/p/6013002.html)

## Column prune

用户可以通过下面的代码来设置只读取的 column

``` java
// column prune, 只读取 DocId 与 Links.Backward 以及 Links.Forward
configuration.set(ReadSupport.PARQUET_READ_SCHEMA, partialSchemaStr);
```

这样 Parquet reader只会读取用户设置的列.

## PushDown Filter - Row filter

RowGroup filter是一个粗粒度的 filter, RowGroup 里仍然有很多不满足条件的行， 这里 Parquet reader将 filter push down到
每当读取一行时就进行 filter.
