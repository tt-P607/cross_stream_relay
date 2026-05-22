"""cross_stream_relay 插件事件处理器。

包含两个 EventHandler：

1. AutoSummaryHandler（接自原 context_bridge）
   订阅 ON_MESSAGE_RECEIVED / ON_MESSAGE_SENT，按消息批次自动维护跨流摘要与每日短期记忆。

2. RelayMessageDecorator（新增）
   订阅 ON_CHATTER_STEP，在 chatter 步进前扫描 unread_messages 中的虚拟转告消息（带
   ``_relay_intent_id`` 等 extra 元数据），把它们渲染成 SystemReminder 注入到 actor bucket，
   提示当前 chatter 这是跨流转告而非用户消息。处理后立即清掉 reminder，避免下一次 tick 重复注入。
"""

from __future__ import annotations

from typing import Any

from src.app.plugin_system.api import prompt_api
from src.app.plugin_system.api.event_api import EventDecision
from src.app.plugin_system.api.log_api import get_logger
from src.app.plugin_system.base import BaseEventHandler
from src.app.plugin_system.types import EventType, Message
from src.kernel.concurrency import get_task_manager

from .config import CrossStreamRelayConfig
from .daily_memory import register_bot_message, register_inbound_message
from .service import collect_message_for_auto_summary, sync_actor_reminder

logger = get_logger("cross_stream_relay.event_handler")

ACTOR_REMINDER_BUCKET = "actor"
RELAY_DECORATOR_REMINDER_NAME = "跨聊天流转告"


class AutoSummaryHandler(BaseEventHandler):
    """收集消息并按批次触发自动摘要更新，同时驱动每日短期记忆。

    行为完全继承自原 context_bridge.event_handler.ContextBridgeAutoSummaryHandler。
    """

    handler_name = "auto_summary_handler"
    handler_description = "按消息批次自动刷新跨流摘要，并按轮次/闲置时间触发每日短期记忆"
    weight = 0
    intercept_message = False
    init_subscribe = [EventType.ON_MESSAGE_RECEIVED, EventType.ON_MESSAGE_SENT]

    async def execute(
        self,
        event_name: str,
        params: dict[str, Any],
    ) -> tuple[EventDecision, dict[str, Any]]:
        """异步收集消息并在后台触发摘要 + 短期记忆更新。"""

        if isinstance(self.plugin.config, CrossStreamRelayConfig):
            if not self.plugin.config.plugin.enabled:
                return EventDecision.SUCCESS, params

        message = params.get("message")
        if not isinstance(message, Message):
            return EventDecision.SUCCESS, params

        # 收到消息时先以当前聊天流视角同步一次 reminder（注入今日短期记忆）
        await sync_actor_reminder(
            self.plugin,
            current_chat_type=message.chat_type,
            current_stream_id=str(message.stream_id or ""),
        )

        direction = "outbound" if event_name == EventType.ON_MESSAGE_SENT.value else "inbound"

        async def _run_summary() -> None:
            try:
                if isinstance(self.plugin.config, CrossStreamRelayConfig):
                    if not self.plugin.config.plugin.auto_summary_enabled:
                        return
                await collect_message_for_auto_summary(
                    self.plugin,
                    message,
                    direction=direction,
                )
            except Exception as error:
                logger.error(
                    f"自动摘要更新失败: stream_id={message.stream_id}, error={error}",
                    exc_info=True,
                )

        async def _run_daily_memory() -> None:
            try:
                if direction == "outbound":
                    await register_bot_message(self.plugin, message)
                    # 总结后立即刷新 reminder，使本群当日短期记忆尽快可见
                    await sync_actor_reminder(
                        self.plugin,
                        current_chat_type=message.chat_type,
                        current_stream_id=str(message.stream_id or ""),
                    )
                else:
                    await register_inbound_message(self.plugin, message)
            except Exception as error:
                logger.error(
                    f"每日短期记忆更新失败: stream_id={message.stream_id}, error={error}",
                    exc_info=True,
                )

        task_manager = get_task_manager()
        stream_short = (str(message.stream_id) or "unknown")[:8]
        task_manager.create_task(
            _run_summary(),
            name=f"cross_stream_relay_auto_summary_{stream_short}",
            daemon=True,
        )
        task_manager.create_task(
            _run_daily_memory(),
            name=f"cross_stream_relay_daily_memory_{stream_short}",
            daemon=True,
        )
        return EventDecision.SUCCESS, params


class RelayMessageDecorator(BaseEventHandler):
    """跨流转告辅助 reminder 注入器。

    订阅 ON_CHATTER_STEP，在 chatter 步进前扫描 ``StreamContext.unread_messages``
    中带 ``_relay_intent_id`` extra 元数据的虚拟消息，把它们渲染成一段
    SystemReminder 注入到 actor bucket，提示当前 chatter 这是跨流转告而非用户消息。

    每次注入后立即清除该 reminder，避免下一次 tick 误判（reminder 是 store 级的，
    不会随 prompt 自动清理；此处通过 ``prompt_api.add_system_reminder`` 覆写空内容
    实现"删除"——并通过判定 unread 是否仍含转告消息来决定是否再次写入）。
    """

    handler_name = "relay_message_decorator"
    handler_description = "为跨流转告虚拟消息注入辅助 SystemReminder，避免被误读为用户消息"
    weight = 0
    intercept_message = False
    init_subscribe = [EventType.ON_CHATTER_STEP]

    async def execute(
        self,
        event_name: str,
        params: dict[str, Any],
    ) -> tuple[EventDecision, dict[str, Any]]:
        """扫描 unread 中的转告消息并按需注入 reminder。
        
        注意：当前版本已禁用 SystemReminder 注入，因为虚拟消息的 content 中已包含完整信息。
        保留此 EventHandler 是为了未来可能的扩展需求（如统计、日志等）。
        """

        # 当前版本不再注入 SystemReminder，直接返回
        # 原因：虚拟消息的 content 已包含【跨流转告】标记和完整信息
        # 避免与 KFC 等使用自定义 context_manager 的 chatter 冲突
        return EventDecision.SUCCESS, params
