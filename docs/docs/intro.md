---
sidebar_position: 1
title: Welcome to rr Framework
slug: /
---

# rr Framework

## 欢迎来到 rr 框架

**rr** 是一个为 Rust 设计的分布式多 Raft 共识框架。它提供了高效的、容错的分布式系统构建块，支持多个独立的 Raft 集群在单个网络中的共存。该框架包含 8 个核心模块：

- **rr-core**: 核心数据结构和共识逻辑
- **rr-config**: 配置管理和参数调优
- **rr-storage**: 持久化存储层，支持多种后端
- **rr-raft**: Raft 协议实现
- **rr-network**: 网络通信和 RPC 框架
- **rr-router**: 请求路由和负载均衡
- **rr-client**: 客户端库和 API 绑定
- **rr-server**: 服务器实现和部署工具

开始探索我们的 [架构文档](/architecture/overview) 以了解系统设计，或查看 [入门指南](/guide/getting-started)。

---

## Welcome to rr Framework

**rr** is a distributed multi-Raft consensus framework designed for Rust. It provides efficient, fault-tolerant building blocks for distributed systems, supporting the coexistence of multiple independent Raft clusters within a single network. The framework comprises eight core modules:

- **rr-core**: Core data structures and consensus logic
- **rr-config**: Configuration management and parameter tuning
- **rr-storage**: Persistent storage layer with multiple backend support
- **rr-raft**: Raft protocol implementation
- **rr-network**: Network communication and RPC framework
- **rr-router**: Request routing and load balancing
- **rr-client**: Client library and API bindings
- **rr-server**: Server implementation and deployment tools

Start exploring our [architecture documentation](/architecture/overview) to understand the system design, or check out our [getting-started guide](/guide/getting-started).
