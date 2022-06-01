---
layout: page
title: Protocbuf 
nav_order: 5
parent: Protocbuf
---

# Protocbuf 学习
{: .no_toc}

本文通过实例学习 Protocbuf.

## 目录
{: .no_toc .text-delta}

1. TOC
{:toc}

## 示例代码

编写 Person.proto 文件

```protoc
syntax = "proto3";

option java_package = "serialization.protobuf";
option java_outer_classname = "PersonProto";

message Person {
  string name = 1;
  int32 age = 2;

  enum PhoneType {
    MOBILE = 0;
    HOME = 1;
    WORK = 2;
  }

  message PhoneNumber {
    string number = 1;
    PhoneType type = 2;
  }

  repeated PhoneNumber phones = 3;
};
```

其中 `string name = 1` 中 `1` 表示 field number, filed Number 用在序列化后的 byte 中用于
识别是哪个 field, 这样才能将 byte 反序列化为对应的 field.

生成 PersonProto.java 文件

``` bash
protoc --java_out=. Person.proto
```

序列化与反序列化

``` java
//序列化
PersonProto.Person person =
    PersonProto.Person.newBuilder()
        .setName("abcd")
        .setAge(39)
        .addPhones(
            PersonProto.Person.PhoneNumber.newBuilder()
                .setNumber("123")
                .setType(PersonProto.Person.PhoneType.HOME)
                .build())
        .addPhones(
            PersonProto.Person.PhoneNumber.newBuilder()
            .setNumber("456")
            .setType(PersonProto.Person.PhoneType.WORK)
            .build())
        .build();
byte[] personBytes = person.toByteArray();

// 反序列化
PersonProto.Person p = PersonProto.Person.parseFrom(personBytes);
```

## Overview

![protocolbufer](/docs/protocbuf/protocbuf/protocbuf.drawio.svg)

## Protocol buffer 序列化

从序列化后的字节数组来看, 没有一个字节是多余的. Protocol buffer message 采用 key-values 格式.

key: `(field_number << 3) | wire_type` 用来识别是哪个 field 以及wire_type, 这样可以从后面的字节数组反序列化出对应的 field.

wire_type 如下所示,

Type | Meaning | Used For
---- | ----- | ----
0 | Varint | int32, int64, uint32, uint64, sint32, sint64, bool, enum
1 | 64-bit | fixed64, sfixed64, double
2 | Length-delimited | string, bytes, embedded messages, packed repeated fields
3 | Start group	| groups (deprecated)
4 | End group	| groups (deprecated)
5 | 32-bit | fixed32, sfixed32, float

Protocol buffer 序列化后的字节长度很短, 它采用了 [Base 128 Varints](https://developers.google.com/protocol-buffers/docs/encoding#varints) 编码.

它使用一个或多个字节来序列化"整数"的方法, 较小的整数使用更少的字节数. 其中每个字节最高位(第8位)表示后面还有更多的字节需要一起解析, 而每个字节的低7位保存整数的7个bit位.

比如要序列化整数 300 (1 0010 1100), 只需要 2 个字节即可

``` console
1010 1100 0000 0010
```

``` java
public final void writeUInt32NoTag(int value) throws IOException {
  try {
    while (true) {
      if ((value & ~0x7F) == 0) { //如果该字节小于0x7F, 则序列化完成
        buffer[position++] = (byte) value;
        return;
      } else {
        //反之保存该字节的7位,并标识高位为1指明后面还有字节
        buffer[position++] = (byte) ((value & 0x7F) | 0x80);
        value >>>= 7;
      }
    }
  } catch (IndexOutOfBoundsException e) {
    throw new OutOfSpaceException(
        String.format("Pos: %d, limit: %d, len: %d", position, limit, 1), e);
  }
}
```

参考,

- [Protocol buffers协议分析](https://www.jianshu.com/p/5b7e2061ba8a)
- [protocol-buffers](https://developers.google.com/protocol-buffers)