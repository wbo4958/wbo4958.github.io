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

- Numeric Builder

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

特别需要注意, 上述的例子中, 初始化时为该 builder 分配了 32 个 elements, 也就是 32 * 4 = 128 个字节保存数据.
但是最后实际上只存放了 8 个 elements, 也就是实际只有 8*4=32 个字节的数据, 但是 Arrow-cpp 要求 64 字节对齐,
所以最后会重新分配 64 字节的大小,然后将数据(32bytes) memcpy 到新的内存中.


