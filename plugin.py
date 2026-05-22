"""cross_stream_relay 插件入口。

跨聊天流互通插件：把"被动总览"（摘要 / 短期记忆）与"主动交接"
（relay 转告 + 冷流唤醒）合并到同一个插件中。
"""

from __future__ import annotations

import asyncio

from src.app.plugin_system.api.log_api import get_logger
from src.app.plugin_system.base import BasePlugin, register_plugin
from src.core.prompt import get_system_reminder_store
from src.kernel.concurrency import get_task_manager

from .action import RelayToStreamAction, UpdateStreamSummaryAction
from .command import RelayCommand, ShortMemoryCommand
from .config import CrossStreamRelayConfig
from .daily_memory import run_archive_loop
from .event_handler import (
    ACTOR_REMINDER_BUCKET as RELAY_REMINDER_BUCKET,
    AutoSummaryHandler,
    RELAY_DECORATOR_REMINDER_NAME,
    RelayMessageDecorator,
)
from .service import (
    ACTOR_REMINDER_BUCKET,
    ACTOR_REMINDER_NAME,
    sync_actor_reminder,
)
from .tool import (
    FindTargetStreamTool,
    GetDailyMemoryTool,
    GetStreamRawContextTool,
)

logger = get_logger("cross_stream_relay")


@register_plugin
class CrossStreamRelayPlugin(BasePlugin):
    """跨聊天流互通插件：被动总览 + 主动交接。

    能力线 1（被动总览）：
      - 自动维护各聊天流摘要并通过 SystemReminder 注入 actor bucket
      - 群聊每日短期记忆全量总结 + 跨天归档守护循环
      - 支持人工纠偏摘要 / 强制生成短期记忆

    能力线 2（主动交接）：
      - find_target_stream 反查目标流元组
      - relay_to_stream 把转告便条注入目标流并冷启动
      - relay_message_decorator 在目标 chatter 处理前注入辅助 reminder
    """

    plugin_name = "cross_stream_relay"
    plugin_description = "跨聊天流互通插件：摘要 / 短期记忆 / 跨流转告"
    plugin_version = "1.0.0"

    configs: list[type] = [CrossStreamRelayConfig]
    dependent_components: list[str] = []

    _archive_stop_event: asyncio.Event | None = None

    def get_components(self) -> list[type]:
        """返回插件组件列表。"""

        if isinstance(self.config, CrossStreamRelayConfig) and not self.config.plugin.enabled:
            logger.info("cross_stream_relay 已在配置中禁用")
            return []

        # 把 usage_guide_prompt 注入到 RelayToStreamAction.action_description
        # 必须在 get_components() 里更新，确保框架注册组件时读到的是完整描述
        if isinstance(self.config, CrossStreamRelayConfig):
            guide = self.config.relay.usage_guide_prompt
            if guide and guide.strip():
                base_desc = (
                    "跨聊天流转告能力。允许你主动向另一个聊天流（群聊或私聊）发送信息，并唤醒目标流的对话逻辑。"
                    "该能力常用于跨流带话、私聊确认群聊细节、或在不同窗口间自然切换对话状态。"
                    "调用前请先用 find_target_stream 工具确认目标流的 stream_id 和 target_id。"
                    "relay_content 是你在目标流的开场白，请以自然的口吻发起对话，避免机械化复述。\n\n"
                )
                RelayToStreamAction.action_description = base_desc + guide.strip()

        # 把 usage_guide_prompt 注入到 RelayToStreamAction.action_description
        # 必须在 get_components() 里更新，确保框架注册组件时读到的是完整描述
        if isinstance(self.config, CrossStreamRelayConfig):
            guide = self.config.relay.usage_guide_prompt
            if guide and guide.strip():
                base_desc = (
                    "跨聊天流意识迁移工具。允许你主动将当前的对话意图、背景和目标传递到另一个聊天流（群聊或私聊），"
                    "并唤醒目标流的对话逻辑。该工具的核心目的是实现「跨流无缝衔接」。"
                    "重要：如果你之前调用了 find_target_stream 工具，你必须立即使用其返回的参数调用此 Action，"
                    "严禁在获取目标 ID 后不执行迁移。这是实现跨流互通的最后一步，也是最关键的一步。\n\n"
                )
                RelayToStreamAction.action_description = base_desc + guide.strip()

        return [
            # Tools
            GetStreamRawContextTool,
            GetDailyMemoryTool,
            FindTargetStreamTool,
            # Actions
            UpdateStreamSummaryAction,
            RelayToStreamAction,
            # Event Handlers
            AutoSummaryHandler,
            RelayMessageDecorator,
            # Commands
            ShortMemoryCommand,
            RelayCommand,
        ]

    async def on_plugin_loaded(self) -> None:
        """插件加载后：重建 actor reminder + 启动跨天归档守护循环。"""

        if isinstance(self.config, CrossStreamRelayConfig) and not self.config.plugin.enabled:
            return

        # 重建 actor reminder（保证 chatter 启动时已能看到跨流摘要）
        await sync_actor_reminder(self, current_chat_type="group")

        # 启动跨天归档守护循环
        if isinstance(self.config, CrossStreamRelayConfig) and self.config.daily_memory.enabled:
            self._archive_stop_event = asyncio.Event()
            get_task_manager().create_task(
                run_archive_loop(self, self._archive_stop_event),
                name="cross_stream_relay_daily_archive_loop",
                daemon=True,
            )

    async def on_plugin_unloaded(self) -> None:
        """插件卸载时停止守护循环并清理 actor reminder。"""

        if self._archive_stop_event is not None:
            self._archive_stop_event.set()
            self._archive_stop_event = None

        store = get_system_reminder_store()
        try:
            store.delete(ACTOR_REMINDER_BUCKET, ACTOR_REMINDER_NAME)
        except Exception as error:
            logger.debug(f"清理 ACTOR_REMINDER_NAME 失败（可忽略）: {error}")
        try:
            store.delete(RELAY_REMINDER_BUCKET, RELAY_DECORATOR_REMINDER_NAME)
        except Exception as error:
            logger.debug(f"清理 RELAY_DECORATOR_REMINDER_NAME 失败（可忽略）: {error}")
