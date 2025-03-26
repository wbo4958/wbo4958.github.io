---
layout: page
title: Spark Connect Classloader
nav_order: 21
parent: connect
grand_parent: spark
---

# Spark Connect ClassLoader

ClassLoader 是 Java 虚拟机（JVM）中的核心组件，负责在运行时动态加载类到内存中。在 Apache Spark 这样的分布式计算框架中，ClassLoader 的管理至关重要，用于处理依赖关系、隔离不同应用部分，并确保正确加载库版本。本文将学习 Spark 在 spark-submit 和 Spark Connect 中的 ClassLoader 管理机制。

## 目录
{: .no_toc .text-delta}

1. TOC
{:toc}

## ClassLoader 的基础概念

在 Java 中，ClassLoader 遵循层次结构，每个 ClassLoader 都有父 ClassLoader。当请求加载类时，ClassLoader 会先委托给父 ClassLoader，之后才尝试自己加载。这种模型有助于避免类冲突，并确保核心 Java 类由引导 ClassLoader 加载。

Spark 作为基于 JVM 的框架，依赖 ClassLoader 来管理应用依赖。Spark 应用通常需要额外的库，正确管理这些依赖对于防止版本冲突和确保执行顺畅至关重要。

## Spark-submit 中的 ClassLoader 管理

![spark-submit-classloader](/docs/spark/connect/classloader/spark-connect-spark-submit-classloader.drawio.svg)

当使用 spark-submit 启动 Spark 应用时，ClassLoader 的设置过程如下：

获取 ClassLoader： SparkSubmit 类调用 getSubmitClassLoader 方法获取合适的 ClassLoader，　该 ClassLoader 一定是 URLClassLoader.
设置线程上下文 ClassLoader： 通过 `Thread.currentThread.setContextClassLoader(loader）` 将获取的 ClassLoader 设置为当前线程的上下文 ClassLoader。
添加 JAR 到类路径： 将 --jars 选项指定的额外 JAR 文件通过 `addJarToClasspath` 方法添加到 ClassLoader 的 classpath 中。

getSubmitClassLoader　通过　`spark.driver.userClassPathFirst`　决定 ClassLoader 类型。

- 如果为 `true`： 使用 ChildFirstURLClassLoader，优先从用户添加的 JAR 中加载类，然后再检查系统 ClassLoader。
- 如果为 `false`： 使用 MutableURLClassLoader，先检查系统 ClassLoader，然后再查找用户添加的 JAR。

## Spark Connect Session-specific ClassLoader

![connect-session-specific-classloader](/docs/spark/connect/classloader/spark-connect-Session-classloader.drawio.svg)

Spark Connect 是 Apache Spark 3.4 引入的功能，提供解耦的客户端-服务器架构，允许从任何应用远程连接 Spark 集群。这种架构需要更复杂的 ClassLoader 管理，以确保不同会话之间的隔离，并处理会话特定的依赖。

在 Spark Connect 中，每个 Session 都有自己的 ClassLoader 来管理（JAR、文件等），防止 session 冲突。主要由 `ArtifactManager` 进行管理：

Spark Connect 中所有的操作都会在 `withSession` block 中执行,

``` scala
sessionHolder.withSession { session =>
   ...
}
```

而 withSession　会一层一层的调用到 `ArtifactManager.withClassLoaderIfNeeded`. 而该函数的作用就是会在当前执行的 code block 之前创建一个新的 ClassLoader，　当 code block 执行完后又会恢复成执行前的 ClassLoader.

新的 ClassLoader 主要将 Session-specific 的 jars 以及 `artifacts/SESSION-ID/classes` 加入到该 ClassLoader 的 classpath 中去，这样就做到了 Session isolation.

## Executor 端的 ClassLoader

