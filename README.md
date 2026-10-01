# Nexalithic

> **The Unshakable Foundation for Secure High-Concurrency Messaging.**

[![Java 21](https://img.shields.io/badge/Java-21-007396?logo=openjdk&logoColor=white)](https://openjdk.org/projects/jdk/21/)

Nexalithic 是一个基于 Java 21 的应用层网络框架，围绕客户端、服务端、会话与消息处理构建，并采用 Maven 多模块结构组织代码。

## 核心能力

- 基于 Java NIO 的 Channel、Selector 与 Session 事件循环；
- 面向业务包与信令包的分层消息模型；
- Payload 注册、消息处理器映射与拦截器管线；
- 客户端连接管理，以及服务端接入、握手和会话管理；
- 可扩展的安全策略、事件总线与异步任务机制；
- 时间轮、限流、负载均衡和对象复用等基础设施。

## 项目结构

| 模块 | 说明 |
| --- | --- |
| `NexalithicCore` | 公共协议模型、I/O 抽象、消息处理、任务与基础设施 |
| `NexalithicServer` | 连接接入、握手、安全策略、会话和服务端生命周期 |
| `NexalithicClient` | 客户端连接、会话、安全策略和客户端生命周期 |

## 构建

环境要求：

- JDK 21
- Maven

在项目根目录运行：

```shell
mvn verify
```

---

## 命名由来

**Nexalithic** 是一个由 **Nexus**（枢纽/连接）与 **Megalithic**（史前巨石建筑）融合而成的合成词。

* **Nex (Nexus)**：代表项目作为海量并发连接分发中枢的核心地位。
* **Lithic (Stone Age)**：寓意“石器时代”的纯粹与稳固，使架构在流量洪峰中如同巨石般岿然不动。

---
