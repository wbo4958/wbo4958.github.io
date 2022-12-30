---
layout: page
title: arrow-cpp
nav_order: 5
parent: Arrow
---

# Arrow 学习
{: .no_toc}

本文通过实例学习 Arrow 原理.

## 目录
{: .no_toc .text-delta}

1. TOC
{:toc}

## Array

![Array](/docs/arrow/arrow-cpp/images/arrow-cpp-Array.drawio.svg) Arrow Array 表示列数据, 比如 Numeric Array, 以及 Nested Array等.
Array 类中 null_bitmap_data_ 表示列数据中每一行的 validity. 数据则由 ArrayData 表示, 

**ArrayData**

- type 表示该 Array 的 DataType,
- length 表示 Array 中有 element 个数, 如果表 Array 看作是 Columnar, 那 length 就是行数
- null_count 表示 null 个数
- buffers 表示该 Array 的真实数据, buffers[0] 表示 null bit map, buffers[1] ...表示真实的数据. 对于 Nested Array, 该 buffer 为空,真实的数据保存在 child_data 中.
- child_data 对于 nested Array, 则由 child Array 组成.

**Buffer**

Buffer 表示一块 in-memory 数据.

- data_ 数据首地址
- size  数据长度
- capacity buffer的容量,  size <= capacity
- is_cpu_ 是否数据在 CPU memory 上, 与之对应的是数据在 GPU 上.
- is_mutable_ 数据是否是 mutable

**MemoryManager**

MemoryManager 负责管理内存, 如何 allocate/reallocate/free, 如何 copy等. 对于不同的 device, 有 cpu MemoryManager和 gpu MemoryManager.

### Array Builder to construct Array

Arrow 提供了对应的 Array 的 builder 构建 Array. 下面以 NumericBuilder 为例进行说明,

- Numeric Array

``` cpp
  arrow::Int32Builder int32builder;  // 生成 Int32Builder 用来build Int32 Array
  int32_t days_raw[5] = {1, 12, 17, 23, 28};
  // AppendValues, as called, puts 5 values from days_raw into our Builder object.
  ARROW_RETURN_NOT_OK(int32builder.Append(11));
  ARROW_RETURN_NOT_OK(int32builder.AppendValues(days_raw, 5)); // Append整个数组
  ARROW_RETURN_NOT_OK(int32builder.AppendNull()); //添加 null
  ARROW_RETURN_NOT_OK(int32builder.AppendEmptyValue()); // 添加 empty value

  // We only have a Builder though, not an Array -- the following code pushes out the
  // built up data into a proper Array.
  std::shared_ptr<arrow::Array> days;
  ARROW_ASSIGN_OR_RAISE(days, int32builder.Finish()); // 生成 Array.
```

整个流程图如下所示,

![builder](/docs/arrow/arrow-cpp/images/arrow-cpp-builder.drawio.svg)

特别需要注意, 上述的例子中, 初始化时为该 builder 分配了 32 个 elements, 也就是 `32 * 4 = 128` 个字节保存数据.
但是最后实际上只存放了 8 个 elements, 也就是实际只有 `8*4=32` 个字节的数据, 但是 Arrow-cpp 要求 64 字节对齐,
所以最后会重新分配 64 字节的大小,然后将数据(32bytes) memcpy 到新的内存中.

- String Array

String builder 包含3个 buffer builder: null bitmap buffer builder, value_data_builder_ 以及 offsets_builder_.

所有的string依次放在 value_data_builder buffer中, offsets 记录每个 string 的 offset.

最后 build 出来的 StringArray 中三个 buffers 依次是 `null_bitmap`, `offset`, `values`.

- Struct Builder

``` cpp
  std::shared_ptr<arrow::DataType> type_;
  std::shared_ptr<arrow::StructBuilder> builder_;
  std::shared_ptr<arrow::StructArray> result_;
  std::vector<std::shared_ptr<arrow::Field>> value_fields_;

  std::vector<std::shared_ptr<arrow::Field>> fields;
  fields.push_back(arrow::field("int16", arrow::int16()));
  fields.push_back(arrow::field("int32", arrow::int32()));

  type_ = struct_(fields);
  value_fields_ = fields;

  std::unique_ptr<arrow::ArrayBuilder> tmp;
  RETURN_NOT_OK(MakeBuilder(arrow::default_memory_pool(), type_, &tmp));
  builder_.reset(arrow::internal::checked_cast<arrow::StructBuilder *>(tmp.release()));

  // 获得 struct 中的第一个 field
  arrow::Int16Builder *int16_vb = arrow::internal::checked_cast<arrow::Int16Builder *>(
    builder_->field_builder(0));
  // 获得 struct 中的第二个 field
  arrow::Int32Builder *int32_vb = arrow::internal::checked_cast<arrow::Int32Builder *>(
    builder_->field_builder(1));

  // 第一个 element
  RETURN_NOT_OK(builder_->Append()); // 表示即将为该struct append一个element
  RETURN_NOT_OK(int16_vb->Append(1));
  RETURN_NOT_OK(int32_vb->Append(2));

  // 第二个 element
  RETURN_NOT_OK(builder_->AppendNull()); // 表示即将为该struct append一个null

  // 第三个 element
  RETURN_NOT_OK(builder_->Append()); // 表示即将为该struct append一个element
  RETURN_NOT_OK(int16_vb->AppendNull()); // 第一列为 null
  RETURN_NOT_OK(int32_vb->Append(2));

  std::shared_ptr<arrow::Array> out;
  ARROW_ASSIGN_OR_RAISE(out, builder_->Finish());

  std::cout << out->ToString() << std::endl;
```

Struct Builder 包含 null_bitmap builder 以及 children builder. 最后 build 出来的 StructArray 包含一个 null bitmap buffer,
以及所有的 children buffers.

- List builder

``` cpp
  auto pool = arrow::default_memory_pool();
  arrow::ListBuilder listBuilder(pool, std::make_unique<arrow::Int16Builder>(pool));

  // 获得 value builder
  auto int16_vb = static_cast<arrow::Int16Builder *>(listBuilder.value_builder());

  // 第一个 element
  RETURN_NOT_OK(listBuilder.Append());
  RETURN_NOT_OK(int16_vb->Append(1));
  RETURN_NOT_OK(int16_vb->Append(2));

  // 第二个 element
  RETURN_NOT_OK(listBuilder.Append());
  RETURN_NOT_OK(int16_vb->Append(6));
  RETURN_NOT_OK(int16_vb->AppendNull());
  RETURN_NOT_OK(int16_vb->Append(8));

  std::shared_ptr<arrow::Array> out;
  ARROW_ASSIGN_OR_RAISE(out, listBuilder.Finish());

  std::cout << out->ToString() << std::endl;
```

ListBuilder 由 null_bitmap builder, offset builder 以及 value_builder构成, 最后生成的
ListArray由 buffers (null bitmap buffer, offset buffer) 以及  child Array.

## Table && RecordBatch

- Table

![Table](/docs/arrow/arrow-cpp/images/arrow-cpp-Table.drawio.svg)

- RecordBatch

![RecordBatch](/docs/arrow/arrow-cpp/images/arrow-cpp-RecordBatch.drawio.svg)

Array 是 1-D 的数据结构, 而 Table 和 RecordBatch 是由多个 Array 组合成的带着 schema 信息的 2-D 数据结构.

Table 与 RecordBatch 的区别如官网所示

![Table vs record batch](/docs/arrow/arrow-cpp/images/tables-versus-record-batches.svg)

> Record batches can be sent between implementations, such as via IPC or via the C Data Interface. 
Tables and chunked arrays, on the other hand, are concepts in the C++ implementation, not in the 
Arrow format itself, so they aren’t directly portable.

``` cpp
// 创建 schema
  std::shared_ptr<arrow::Field> field_day, field_month, field_year;
  std::shared_ptr<arrow::Schema> schema;

  field_day = arrow::field("Day", arrow::int8());
  field_month = arrow::field("Month", arrow::int8());
  field_year = arrow::field("Year", arrow::int16());

  schema = arrow::schema({field_day, field_month, field_year});

// 创建 RecordBatch

  std::shared_ptr<arrow::RecordBatch> rbatch;
  rbatch = arrow::RecordBatch::Make(schema, days->length(), {days, months, years});

// 创建 ChunkedArray
  arrow::ArrayVector day_vecs{days, days2};
  std::shared_ptr<arrow::ChunkedArray> day_chunks =
      std::make_shared<arrow::ChunkedArray>(day_vecs);
  arrow::ArrayVector month_vecs{months, months};
  std::shared_ptr<arrow::ChunkedArray> month_chunks =
      std::make_shared<arrow::ChunkedArray>(month_vecs);

  arrow::ArrayVector year_vecs{years, years2};
  std::shared_ptr<arrow::ChunkedArray> year_chunks =
      std::make_shared<arrow::ChunkedArray>(year_vecs);

// 创建 Table
  std::shared_ptr<arrow::Table> table;
  table = arrow::Table::Make(schema, {day_chunks, month_chunks, year_chunks}, 10);
```
