# Cross Stream Relay (跨聊天流互通插件)

`cross_stream_relay` 是 Neo-MoFox 框架的核心插件之一，旨在打破不同聊天流（群聊、私聊）之间的信息孤岛，赋予 AI 跨流感知与主动迁移的能力。

## 核心能力

### 1. 跨流摘要 (Cross-Stream Summary)
- **自动维护**：插件会自动根据聊天进度，为每个活跃的聊天流生成并更新客观摘要。
- **全局感知**：通过 System Reminder 注入，AI 在任何聊天流中都能看到其他流的最新动态摘要。
- **隐私控制**：支持黑白名单模式，可精确控制哪些群聊或私聊参与互通。

### 2. 每日短期记忆 (Daily Memory)
- **全量总结**：针对群聊场景，每日定时或按交互轮次生成全量聊天总结。
- **深度回溯**：提供比摘要更详细的上下文，支持 AI 跨群查询历史记忆。
- **自动归档**：支持跨天自动归档，保持记忆的实时性与整洁。

### 3. 跨流意识迁移 (Relay / Consciousness Migration)
- **主动转告**：AI 可以通过 `relay_to_stream` Action 主动将当前的对话意图、背景和目标传递到另一个流。
- **冷流唤醒**：如果目标流处于非活跃状态，插件会自动启动目标流的 Loop 并注入虚拟系统消息。
- **无缝衔接**：通过“记忆凭证”机制，目标流的 AI 能立刻理解来源流的上下文，实现像人类一样的自然切换。

## 安装与配置

### 依赖要求
- Neo-MoFox 框架版本 >= 1.0.0
- 建议使用具备强推理能力的模型（如 GPT-4o, Claude 3.5 等）以获得最佳的迁移效果。

### 快速开始
1. 将插件目录放置于 `plugins/cross_stream_relay`。
2. 在 `config/plugins/cross_stream_relay/config.toml` 中进行基础配置。
3. 启动 Bot，AI 将自动开始维护摘要。

## 开发者指南

### 提供的组件

#### Tools
- `get_stream_summary`: 获取指定流的摘要。
- `get_stream_raw_context`: 获取指定流的原始聊天记录。
- `get_daily_memory`: 查询群聊的每日全量总结。
- `find_target_stream`: 跨流转告的前置查找工具。

#### Actions
- `update_stream_summary`: 人工纠偏当前流摘要。
- `relay_to_stream`: 执行跨流意识迁移。

#### Commands
- `/短期记忆`: 管理与查询每日总结。
- `/relay`: 跨流转告状态查询与测试。

## 开源协议

本项目采用 **GNU Affero General Public License v3.0 (AGPL-3.0)** 协议开源。
