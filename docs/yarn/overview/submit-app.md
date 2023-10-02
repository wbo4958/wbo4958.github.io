---
layout: page
title: Yarn Overview
nav_order: 1
parent: YARN
---

# Yarn Overview
{: .no_toc}

本文基于 hadoop-3.3.6 学习 Yarn.　通过向 yarn 提交一个简单的 Yarn application 学习 yarn 的基本工作流程,　本文不会注重太多细节,　只关注 yarn 是如何分配 container, 以及 yarn 是最终在哪里启动 container.

## 目录
{: .no_toc .text-delta}

1. TOC
{:toc}

## 测试代码

本文参考 [simple-yarn-app](https://github.com/hortonworks/simple-yarn-app), 采用 Managed 模式提交 application, 也就是 ResourceMananger 会为　ApplicationMaster 分配一个 Container, 然后在 NodeManager 上启动该 Container 运行 ApplicationMaster, 而 ApplicationMaster 再负责与 RM/NM 通信, 请求资源,分配启动Container并运行指定命令.

## Overview

上图描述了 Yarn 是如何启动 ApplicationMaster 的,　分为 A, B, C, D 4个阶段

### A. NodeManager 向 ResourceManager 注册

- A_1: NodeManager 启动时会创建 NodeStatusUpdaterImpl, 该类用于将 Node 的 Resource, port 等信息通过 `registerNodeManager` 向 RM 进行注册.
- A_2: RM 收到 Node 注册信息,　首先创建　RMNodeImpl track 该 Node. 然后发送 STARTED 信息触发　RMNodeImpl 的状态机
- A_3: 触发 AddNodeTransition, 上报 Node Running,　接着向　scheduler 发送　`NodeAddedSchedulerEvent`　事件.
- A_4: Scheduler 处理 NODE_ADDED 事件, 为该 Node 生成一个 Scheduler　中的 `FiCaSchedulerNode`.

### B. Client 向 ResourceManager 提交 Application.

- B_1 ~ B_2: Client 通过 YarnClient.createApplication 创建 Yarn Application, 并创建 ContainerLaunchContext 保存 launch ApplicationManagner 所需要的 context. 最后生成 ApplicationSubmissionContext, 并设置好 Application Submission context.
- B_3: Client 通过 YarnClient 提交该 Yarn Application.
- B_4: ResourceManager 收到 Client 请求后通过 RMAppManager 创建一个 RMMAppImpl 用来表示该 Yarn Application. 接着 RMAppManager 发送 START 事件触发 RMAppImpl 的状态机.
- B_5 ~ B_6: AddApplicationToSchedulerTransition 将 application 加入到 scheduler, 并将 application 加入到 scheduler 的　Queue 中.　最后通知 RMMAppImpl APP_ACCEPTED 事件.
- B_7 ~ B_9: RMAppImpl 通过 StartAppAttemptTransition, 创建 RMAppAttemptImpl 表示 App　的一次 Attempt 提交. 接着发送 START 事件,　触发 RMAppAttemptImpl 状态机
- B_10: RMAppAttemptImpl收到 START 事件后,　触发AttemptStartedTransition状态机向 Scheduler 提交 Application Attempt. 接着 scheduler 通知 RMAppAttemptImpl `ATTEMPT_ADDED` 事件.
- B_11 ~ B_16: RMAppAttemptImpl 收到 ATTEMPT_ADDED 事件后触发 ScheduleTransition 状态机向 scheduler 申请分配 Container. Scheduler 将 pending request resource 加入到 Queue 的 pending resource 里.

### C. NodeManager 与 ResourceManager 的心跳连接

NodeManager 在向 ResourceManager 注册好后,　NodeManager通过心跳机器向ResourceManager 上报 NodeManager 的状态信息.

- C_1 ~ C_3: NodeManager 通过某一个心跳将node状态信息上报给 RM.
- C_4: Queue 检测到有 pending resource request, 准备开始 allocate Container 给该 Application
- C_5 ~ C_7: ContainerAllocator 为该 App 创建一个 container.
- C_8 ~ C_9: 同时创建 RMContainerImpl 表示 RM 端的　Application container. 
- C_10 ~ C_1１: 分配完 Container 后,　开始 commit resource request. 最后将新分配的 Container 加入到 FiCaSchedulerApp 的　`newlyAllocatedContainers`中. 最后向 RMContainerImpl 发送 START 事件.
- C_12: RMContainerImpl 收到 START 事件,　触发 ContainerStartedTransition 状态机,　最后向 RMAttemptImpl 发送 CONTAINER_ALLOCATED 事件.
- C_13 ~ C_15: RMAttemptImpl 经过几轮状态转换后,　调用 launchAttempt　向 ApplicationManagerLauncher 发送　LAUNCH 事件.
- C_16 ~ C_18: ApplicationManagerLauncher处理　LAUNCH 事件.创建一个 AMLauncher Runnable 通过 Container 中的 node 信息建立起与　NodeManager 之间的连接,　最终通知　NodeManager 启动 Container.

### D. NodeManager 启动 AM Container.

TODO