---
layout: page
title: Protocbuf 
nav_order: 5
parent: Protocbuf
---

# FlatBuffers学习
{: .no_toc}

本文通过实例学习 flatbuffer 原理.

## 目录
{: .no_toc .text-delta}

1. TOC
{:toc}

## 示例代码

编写 people.fbs 文件

```flatbuffers
namespace com.example

table People {
    name:string;
    age:int;
}
root_type People;
```

生成 People.java 文件

``` bash
flatc --java people.fbs
```

write and read

``` java
//创建 Person Table

FlatBufferBuilder flatBufferBuilder = new FlatBufferBuilder(64);
// createString 将 "abcdef" 写入到 buffer 中, 返回的nameOffset 是相对于 buffer end 的偏移
int nameOffset = flatBufferBuilder.createString("abcde");
// root 为 vtable_loc 的位置, 即 vtable 的 "中心点位置"
int root = People.createPeople(flatBufferBuilder, nameOffset, 29);
// 结束对buffer写入, 并更新相关offset.
flatBufferBuilder.finish(root);

// 读取 People
People person = People.getRootAsPeople(flatBufferBuilder.dataBuffer());
System.out.println("Name: " + person.name() + ", Age: " + person.age());
```

## Overview

![flatbuffers](/docs/flatbuffers/flatbuffers/flatbuffers.drawio.svg)

## flatbuffers 原理

编译出来的 People.java 继承于 Table. People.java 提供了 `addName()/name()` 和 `addAge()/age()` API 去访问 "成员变量 name和age", 这里打引号是因为 People 并没有成员变量 name/age, 它有一个 `ByteBuffer bb`缓冲区 来保存 People 对象的相关数据, 即 `addName()/addAge()/name()/age()` 是访问的缓冲区里的数据, 而非 People 的成员变量, 只不过 flatbuffers 增加一层 wrapper, 让 People behave 的像一般 JVM 对象一样使用成员变量.

所以 flatbuffers 操作的是缓冲区, 也就是可以直接将 flatbuffers 数据写入文件或网络传输, 并不需要序列化和反序列化. 最快的序列化反序列化框架确是没有序列化/反序列化.

flatbuffers 的缓冲区通过 ByteBuffer 来实现, flatbuffers 提供接口 `ByteBufferFactory` 来创建所需要的缓冲区具体实现, 如 Heap ByteBuffer 还是 Direct ByteBuffer. flatbuffers 目前内部只提供 Heap ByteBuffer, 如果想使用 Direct ByteBuffer, 需要自己实现 ByteBufferFactory.

### write

如上面所述, 将 name, age 写入到 People flatbuffers地, 实际是将其值加入到缓冲区中. Flatbuffers 的写过程是从缓冲区高字节往低字节方向开始写.

对于 addName() 来说比较特殊,

``` java
public static void addName(FlatBufferBuilder builder, int nameOffset){
   builder.addOffset(0, nameOffset, 0); }

public static void addAge(FlatBufferBuilder builder, int age) { 
  builder.addInt(1, age, 0); }
```

addName 加入的是一个 int 值,即 name 在 缓冲区的偏移, 所以我们需要事先将 name 的真实值 "abcde" 通过 `createString("abcde")` 写入到缓冲区, 并返回一个相对于缓冲区高字节的偏移, 然后通过 `addName` 告知 People 从哪里去获得该字符串.

当将所有的值都写入缓冲区后, 需要调用 `People.endPeople` 也就是调用 `endTable` 来结束对 People 缓冲区的写入. 此时会为该 People 对象继续写入 "vtable", vtable 保存 People 成员变量 "name, age" 在缓冲区中的偏移. 这样当访问 "name", "age", 通过 vtable 就可以定位到它们在缓冲区中的 offset

最后调用 FlatBufferBuilder.finish 来结束对该 ByteBuffer 的写入, 并写入 vtable_loc (vtable的中心点)在缓冲区.

vtable_loc 是 vtable 的中心点, 通过 vtable_loc 可以获得 vtable_start/vtable_size, 以及 name/age 的偏移. 最后可以获得 People.

### read

read 过程是 write 的反过程, write是从 ByteBuffer 的高字节往下写, 而 read 则从 ByteBuffer 的低字节往上读. 从 write 的过程, 低字节是 People 的 vtable 信息. read 从低字节读取, 则很容易就读取到 People 的 vtable 信息,进而可以访问到 name/age 信息.

具体流程如下:

``` console
People 在编译成 Table 时,里面的成员变量已经确定好了其在 vtable中的 offset,
如 name 的 offset = 4, age 的 offset = 6

People 在 初始化时, 如果从0 (左侧的offset)开始计算出
首先获得  bb_pos = bb.getInt(0) = 12, 表示当前离 vtable_loc的距离
对象的读取都是从 vtable_loc (vtable 中心点) 获得 vtable 相关meta信息
vtable_start = vtable_loc - bb.getInt(bb.getInt(0))=12 - bb.getInt(12)=4
vtable_size = bb.getShort(vtable_start)=bb.getShort(4)= 8

name() 是可变长vector. 所以需要计算 offset, length
offset =vtable_loc + 4 + bb.getInt(vtable_loc + 4)=bb.getInt(16)+16=24
length=bb.getInt(offset)=bb.getInt(24)=5
则value_offset = offset + size_int(这个是length占的字节数)=24+4=28
value = utf8.decode(bb, value_offset, length)=decode(bb, 28, 5) = "abcde"

age() 是标量 vector, 
offset = bb.getShort(vtable_start + 6) = 8
value = bb.getInt(vtable_loc + offset) = bb.getInt(20) = 29
```

参考 [深入浅出 FlatBuffers 原理](https://zhuanlan.zhihu.com/p/391327093)
