---
layout: page
title: date/time
nav_order: 5
parent: Sql functions
grand_parent: Spark
---

# date/time
{: .no_toc}

本文主要是学习 date/time 相关类型与相关的函数. 本文基于 Spark 3.1.1.

## 目录
{: .no_toc .text-delta}

1. TOC
{:toc}

## date

- java.time.LocalTime
  
  表示一个没有时区时间,本地时间,即挂在墙上的时间. 主要是 `hour:minute:second.nano` 格式.

- java.time.LocalDate

  表示一个没有时区的日期,本地日期, 主要是 `hour-minute-second` 格式.
  
- java.time.LocalDateTime

  `LocalDate + LocalTime`
  
- java.util.Date
  
  表示一个 Date, 