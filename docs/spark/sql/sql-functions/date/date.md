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

## java 内置表示 date/time 的类型

- java.time.LocalTime
  
  表示一个没有时区时间,本地时间,即挂在墙上的时间. 主要是 `hour:minute:second.nano` 格式.

- java.time.LocalDate

  表示一个没有时区的日期,本地日期, 主要是 `hour-minute-second` 格式.
  
- java.time.LocalDateTime

  `LocalDate + LocalTime`的组合
  
- java.util.Date
  
  表示一个具有毫秒精度的时间, 新的实现已经放在了 Date 中的 CalendarDate了.

- Instant
  
  表示时间线上的一个瞬时点, 由 seconds (距1972-11-04T12:00的秒数) 和 nanos 表示

- ZoneId

  表示一个时区信息, 时区是相对于 Greenwitch 的偏移，比如中国 GMT+8. UTC 是世界时区, 它是全球民用时区和时区的基础, 这意味着没有任何国家或地区正式使用UTC作为当地时间. 那中国的时间为 UTC+8小时.

  ZoneId内置了各种时区信息如 Asia/ShangeHai.

- ZoneOffset
  
  时区的偏移, totalSeconds 表示该时区相对于 Greenwich 偏移了多少秒，比如 `GMT+8`, `totalSeconds= 8*60*60 = 28800`

## Spark 中表示 date/type 的类型

### 类型

- DateType

  表示日期, 范围 [0001-01-01, 9999-12-31]

- TimeStampType

  表示 time, 范围 [0001-01-01T00:00:00.000000Z, 9999-12-31T23:59:59.999999Z], "T"是一个分隔符,
  "Z" zulu 时间即 UTC+0.

- CalendarInterval

  表示 calendar intervals, 由 months/days/microseconds 表示.

### date/time 相关函数

date/time 相关的函数操作可以参考 [datetime.sql](  https://github.com/apache/spark/blob/branch-3.1/sql/core/src/test/resources/sql-tests/inputs/datetime.sql).

- **current_date/current_timestamp**

  ``` scala
  scala> val df = spark.sql("select current_date, current_timestamp")
  df: org.apache.spark.sql.DataFrame = [current_date(): date, current_timestamp(): timestamp]

  scala> df.explain(true)
  == Parsed Logical Plan ==
  'Project ['current_date, 'current_timestamp]
  +- OneRowRelation

  == Analyzed Logical Plan ==
  current_date(): date, current_timestamp(): timestamp
  Project [current_date(Some(Asia/Shanghai)) AS current_date()#119, current_timestamp() AS current_timestamp()#120]
  +- OneRowRelation

  == Optimized Logical Plan ==
  Project [18778 AS current_date()#119, 1622419436167000 AS current_timestamp()#120]
  +- OneRowRelation

  == Physical Plan ==
  *(1) Project [18778 AS current_date()#119, 1622419436167000 AS current_timestamp()#120]
  +- *(1) Scan OneRowRelation[]


  scala> df.show(false)
  +--------------+-----------------------+
  |current_date()|current_timestamp()    |
  +--------------+-----------------------+
  |2021-05-31    |2021-05-31 08:04:10.926|
  +--------------+-----------------------+
  ```

  current_date与current_timestamp分别计算 driver 的日期与时间,

  Spark Catalyst 中 CurrentDate/CurrentTimestamp 分别描述 current_date与
  current_timestamp. 且在 Optimizer 阶段已经将 driver 的时间计算出来了. 具体参考
  [ComputeCurrentTime](https://github.com/apache/spark/blob/branch-3.1/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/optimizer/finishAnalysis.scala#L73)

  这里需要注意的是 CurrentDate 也是通过 CurrentTimestamp 先获得 microseconds 然后再转换为 Date.

- **date_format**

  将 `date/timestamp/string` 根据 format 转换为所需要的string值，格式由第二个参数给定的日期格式指定,
  如 `dd.MM.yyyy`

  ``` scala
  val df = spark.sql("SELECT date_format('2020-05-26', 'dd')")
  df.explain(true)
  ```

  ``` console
  == Analyzed Logical Plan ==
  date_format(CAST(2020-05-26 AS TIMESTAMP), dd): string
  Project [date_format(cast(2020-05-26 as timestamp), dd, Some(Asia/Shanghai)) AS date_format(CAST(2020-05-26 AS TIMESTAMP), dd)#0]
  +- OneRowRelation

  == Optimized Logical Plan ==
  Project [26 AS date_format(CAST(2020-05-26 AS TIMESTAMP), dd)#0]
  +- OneRowRelation

  == Physical Plan ==
  *(1) Project [26 AS date_format(CAST(2020-05-26 AS TIMESTAMP), dd)#0]
  +- *(1) Scan OneRowRelation[]
  ```

  从上面的 explain 可以看出， Optimizer阶段已经将 date_format 计算出来了, 具体是在
  [ConstantFolding](https://github.com/apache/spark/blob/branch-3.1/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/optimizer/expressions.scala#L44)

  ``` scala
  val timestamp = Timestamp.valueOf("2021-05-17 13:29:30.123456")

  val dateFormatClass = DateFormatClass(Literal.create(timestamp),
    Literal.create("yyyy-MM-dd HH:mm:ss.SSSSSS"), Some("Asia/Shanghai"))

  val ret = dateFormatClass.eval(null)
  println(s"ret = ${ret}")
  ```

  打印结果如下,

  ``` console
  ret = 2021-05-17 13:29:30.123456
  ```

- **year/quarter/month/dayofweek/hour/minute/second**
  
  通过 DateTimeUtils 里相关函数实现相应的功能如

  ``` scala
  def getYear(days: Int): Int = daysToLocalDate(days).getYear
  ```

  getYear/getMonth/getQuarter/getDayInYear等都是将 days 转换为`LocalDate`, 然后再获得相关的值

  ``` scala
  def getHours(micros: Long, zoneId: ZoneId): Int = {
    getLocalDateTime(micros, zoneId).getHour
  }
  ```

  getHours/getMinutes/getSeconds/getMicroseconds ... 等将 micros 转换为 `LocalDateTime`, 然后再获
  得相关的值.

- add_months
  
  通过 `DateTimeUtils.dateAddMonths` 实现功能, 将 days 转换为 LocalDate, 然后 plusMonths, 最后再将
  LocalDate转换为 epoch day.

- date_add/date_sub

  ``` scala
  scala> val  df = spark.sql("select date_add('2020-02-02', 2)")
  df: org.apache.spark.sql.DataFrame = [date_add(CAST(2020-02-02 AS DATE), 2): date]

  scala> df.explain(true)
  == Parsed Logical Plan ==
  'Project [unresolvedalias('date_add(2020-02-02, 2), None)]
  +- OneRowRelation

  == Analyzed Logical Plan ==
  date_add(CAST(2020-02-02 AS DATE), 2): date
  Project [date_add(cast(2020-02-02 as date), 2) AS date_add(CAST(2020-02-02 AS DATE), 2)#74]
  +- OneRowRelation

  == Optimized Logical Plan ==
  Project [18296 AS date_add(CAST(2020-02-02 AS DATE), 2)#74]
  +- OneRowRelation

  == Physical Plan ==
  *(1) Project [18296 AS date_add(CAST(2020-02-02 AS DATE), 2)#74]
  +- *(1) Scan OneRowRelation[]
  ```

  日期相加减, 同样也是在 Optimizer 中 ConstantFolding 里计算出来.

- unix_timestamp
  
  获得unix timestamp

  ``` scala
  scala> spark.sql("select unix_timestamp()").show()
  +--------------------------------------------------------+
  |unix_timestamp(current_timestamp(), yyyy-MM-dd HH:mm:ss)|
  +--------------------------------------------------------+
  |                                              1622598154|
  +--------------------------------------------------------+
  ```

  ``` scala
  scala> spark.sql("select unix_timestamp('2012-02-11', 'yyyy-MM-dd')").show()
  +--------------------------------------+
  |unix_timestamp(2012-02-11, yyyy-MM-dd)|
  +--------------------------------------+
  |                            1328889600|
  +--------------------------------------+
  ```
  
  对于 DateType -> 调用 daysToMicros(t.asInstanceOf[Int], zoneId), 将 days 转换为 microseconds
  
  对于 TimestampType -> 直接获得其 value.

  对于 StringType -> 调用TimestampFormatter.parse解析.

- from_unixtime

  将 unix timestamp 格式成可读格式.

  ``` scala
  scala> spark.sql("SELECT from_unixtime(1226446340)").show()
  +--------------------------------------------------------------+
  |from_unixtime(CAST(1226446340 AS BIGINT), yyyy-MM-dd HH:mm:ss)|
  +--------------------------------------------------------------+
  |                                           2008-11-12 07:32:20|
  +--------------------------------------------------------------+
  ```

- to_timestamp
  
  将 time 格式的字符串解析为 timestamp

  ``` scala
  scala> spark.sql("select to_timestamp('2019 40', 'yyyy mm')").show()
  +----------------------------------+
  |to_timestamp('2019 40', 'yyyy mm')|
  +----------------------------------+
  |               2019-01-01 00:40:00|
  +----------------------------------+
  ```
