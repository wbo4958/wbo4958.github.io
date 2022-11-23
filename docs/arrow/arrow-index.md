---
layout: page
title: Arrow
nav_order: 500 
has_children: true
permalink: /arrow/
---
# Arrow Contents

Arrow project 包含两部分, 一个是 Arrow Format, 另一个是 Arrow 的不同的实现.

Arrow Format 也叫做 Arrow Columnar Format 表示列式存储, 它是一个与具体实现无关的 "in-memory" 数据结构规范, 这里的 in-memory 不只表示CPU内存(比如 CUDF 实现,数据是保存到GPU memory 上的),也有可能是GPU内存或其它内存.

Arrow 不同的实现, 官方的 cpp/java/python/go/ ... 等.