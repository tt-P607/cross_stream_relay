"""cross_stream_relay 插件 Action 组件。

包含两个 Action：

1. UpdateStreamSummaryAction（接自原 context_bridge.update_context_bridge_summary）
   人工纠偏当前流摘要。

2. RelayToStreamAction（新增，核心能力）
   把转告便条以虚拟 Message 形式注入目标流的 unread_messages 队列，
   冷态时自动启动目标流的 loop。
"""

from __future__ import annotations

import time
from typing import Annotated, Any
from uuid import uuid4

from src.app.plugin_system.api import stream_api
from src.app.plugin_system.api.log_api import get_logger
from src.app.plugin_system.base import BaseAction
from src.app.plugin_system.types import Message, MessageType

from .config import CrossStreamRelayConfig
from .service import sync_actor_reminder, upsert_summary

logger = get_logger("cross_stream_relay.action")


class UpdateStreamSummaryAction(BaseAction):
    """为当前聊天流写入最新摘要并同步 reminder。"""

    action_name = "update_stream_summary"
    action_description = (
        "为当前聊天流写入最新摘要，并同步到跨聊天流 system reminder。"
        "插件默认会自动根据消息批次更新摘要，因此这个 action 主要用于人工纠偏。"
        "只有当你判断当前摘要已经偏离真实情况、包含事实错误，或需要在全站范围立刻记录某个约定时才调用。"
        "你的输入必须是一段完整的、客观的第三人称摘要（限制 300 字内）。"
        "摘要必须是覆盖旧摘要后的最新版本，必须保留：相关人物的客观事实、核心话题、背景、约束、待办和下一步。"
        "不要写流水账，不要写未经确认的猜测。"
    )

    chatter_allow: list[str] = []

    async def execute(
        self,
        summary: Annotated[
            str,
            "当前聊天流的最新纠偏摘要。只有在现有客观摘要明显失真或遗漏关键事实时才调用；必须保持第三人称客观视角，提供涵盖最新事实、约束及待办的完整新摘要（会直接覆盖旧摘要而不是追加）。",
        ],
    ) -> tuple[bool, str]:
        """写入当前聊天流摘要并刷新 actor reminder。"""

        try:
            changed = await upsert_summary(self.plugin, self.chat_stream, summary)
            await sync_actor_reminder(self.plugin)
        except ValueError as error:
            return False, str(error)

        stream_name = self.chat_stream.stream_name or self.chat_stream.stream_id[:8]
        if changed:
            return True, f"已更新聊天流 {stream_name} 的跨流摘要并同步 reminder"
        return True, f"聊天流 {stream_name} 的摘要无变化，已保持现有内容并同步 reminder"


def _trim_relay_content(content: str, max_chars: int) -> str:
    """按上限截断 relay_content，超出时附 '...(已截断)' 标记。"""

    if max_chars <= 0 or len(content) <= max_chars:
        return content
    if max_chars <= 16:
        return content[:max_chars]
    return content[: max_chars - 16].rstrip() + "...(已截断)"


def _format_context_messages(messages: list[dict]) -> str:
    """将消息列表格式化为带 sender_id 的可读字符串。

    格式：[时间] 昵称(ID): 内容
    确保目标流能通过 ID 精确识别每位发言者身份。

    Args:
        messages: 消息字典列表，每条含 time / sender_name / sender_id / processed_plain_text

    Returns:
        格式化后的多行文本
    """
    import time as _time

    now_ts = _time.time()
    lines: list[str] = []

    for msg in messages:
        ts = float(msg.get("time") or 0.0)
        delta = max(0.0, now_ts - ts)
        if delta < 60:
            ts_text = "刚刚"
        elif delta < 3600:
            ts_text = f"{int(delta // 60)}分钟前"
        elif delta < 86400:
            ts_text = f"{int(delta // 3600)}小时前"
        else:
            ts_text = f"{int(delta // 86400)}天前"

        sender_name = str(msg.get("sender_name") or "未知用户")
        sender_id = str(msg.get("sender_id") or "")
        # 附带 ID，格式：昵称(ID)，若 sender_id 与 sender_name 相同则不重复
        if sender_id and sender_id != sender_name:
            sender_label = f"{sender_name}({sender_id})"
        else:
            sender_label = sender_name

        content = str(msg.get("processed_plain_text") or msg.get("content") or "")
        lines.append(f"[{ts_text}] {sender_label}: {content}")

    return "\n".join(lines)


class RelayToStreamAction(BaseAction):
    """跨聊天流转告：将信息主动传递到另一个聊天流，并唤醒目标流的对话逻辑。"""

    action_name = "relay_to_stream"
    action_description = (
        "跨聊天流意识迁移工具。允许你主动将当前的对话意图、背景和目标传递到另一个聊天流（群聊或私聊），"
        "并唤醒目标流的对话逻辑。该工具的核心目的是实现「跨流无缝衔接」。"
        "重要：如果你之前调用了 find_target_stream 工具，你必须立即使用其返回的参数调用此 Action，"
        "严禁在获取目标 ID 后不执行迁移。这是实现跨流互通的最后一步，也是最关键的一步。"
    )

    chatter_allow: list[str] = []

    async def execute(
        self,
        target_stream_id: Annotated[
            str,
            "目标聊天流 ID（64 位 hex）。通常不需要手动填写，除非你从 find_target_stream 获取了它。",
        ] = "",
        relay_content: Annotated[
            str,
            "你在目标流的开场白。必须包含来源说明和话题承接，语气自然。",
        ] = "",
        context_message_count: Annotated[
            int,
            "带几条原始消息过去作为记忆凭证。默认 10 条，最多 50 条。"
            "如果你判断当前话题有更多有价值的上下文需要传递，可以显式指定更大的值（如 20、30）。",
        ] = 10,
        opening_hint: Annotated[
            str,
            "给目标流自己的私密指令。必须包含迁移目的、后续执行建议和情感基调同步。",
        ] = "",
        target_platform: Annotated[
            str,
            "目标平台标识，默认 'qq'。",
        ] = "qq",
        target_user_id: Annotated[
            str,
            "对方的 QQ 号（私聊必填）。从摘要的【QQ: xxx】获取。",
        ] = "",
        target_group_id: Annotated[
            str,
            "目标群号（群聊可选）。从摘要的【群号: xxx】获取。",
        ] = "",
    ) -> tuple[bool, str]:
        """执行跨流转告。"""

        config = self.plugin.config if isinstance(self.plugin.config, CrossStreamRelayConfig) else None
        if config is None:
            return False, "插件配置异常，无法执行跨流转告"
        if not config.relay.enabled:
            return False, "跨流转告功能已在配置中禁用"

        if not relay_content or not relay_content.strip():
            return False, "relay_content 不能为空，请写下你的开场白"

        # 限制上下文消息数量（默认 10，最多 50）
        context_count = max(0, min(int(context_message_count), 50))

        # 截断转告内容
        max_chars = max(64, int(config.relay.max_relay_chars))
        trimmed_content = _trim_relay_content(relay_content.strip(), max_chars)

        # 解析目标流
        origin_stream_id = self.chat_stream.stream_id
        chat_stream = await self._resolve_target_stream(
            target_stream_id=target_stream_id.strip(),
            target_platform=target_platform.strip(),
            target_user_id=target_user_id.strip(),
            target_group_id=target_group_id.strip(),
        )
        if isinstance(chat_stream, str):
            return False, chat_stream

        # 自检：禁止给自己转告
        if chat_stream.stream_id == origin_stream_id and not config.relay.allow_self_relay:
            return False, "目标流就是当前流，禁止自我转告。"

        # 构造虚拟消息元数据
        intent_id = uuid4().hex[:16]
        origin_stream_name = self.chat_stream.stream_name or origin_stream_id[:8]
        origin_chat_type = self.chat_stream.chat_type or ""
        origin_platform = self.chat_stream.platform or ""

        extra_kwargs: dict[str, Any] = {
            "_relay_intent_id": intent_id,
            "_relay_origin_stream_id": origin_stream_id,
            "_relay_origin_stream_name": origin_stream_name,
            "_relay_origin_chat_type": origin_chat_type,
            "_relay_origin_platform": origin_platform,
            "_relay_created_at": time.time(),
        }
        if opening_hint.strip():
            extra_kwargs["_relay_opening_hint"] = opening_hint.strip()

        # 确定虚拟消息的 sender_id
        target_user_id_resolved = ""
        virtual_sender_id = "0"

        if chat_stream.chat_type == "private":
            if target_user_id.strip():
                target_user_id_resolved = target_user_id.strip()
            else:
                try:
                    info = await stream_api.get_stream_info(chat_stream.stream_id)
                    if isinstance(info, dict):
                        target_user_id_resolved = str(info.get("person_id") or info.get("user_id") or "")
                except Exception:
                    pass
            
            if target_user_id_resolved:
                virtual_sender_id = target_user_id_resolved
                extra_kwargs["target_user_id"] = target_user_id_resolved
            else:
                logger.warning(f"[relay] 私聊场景但无法获取 target_user_id (stream_id={chat_stream.stream_id[:8]})")

        elif chat_stream.chat_type == "group":
            try:
                info = await stream_api.get_stream_info(chat_stream.stream_id)
                if isinstance(info, dict):
                    gid = str(info.get("group_id") or "")
                    if gid:
                        extra_kwargs["target_group_id"] = gid
            except Exception:
                pass

        # 获取原始上下文消息，格式化时附带 sender_id 以便目标流识别身份
        context_messages_text = ""
        if context_count > 0:
            try:
                from src.app.plugin_system.api import message_api
                recent_messages = await message_api.get_recent_messages(
                    stream_id=origin_stream_id,
                    hours=24,
                    limit=context_count,
                    limit_mode="latest",
                    filter_bot=False,
                )
                if recent_messages:
                    context_messages_text = _format_context_messages(recent_messages)
            except Exception as error:
                logger.warning(f"[relay] 获取原始上下文消息失败: {error}")

        # 构造完整的转告消息内容
        trigger_prefix = ""
        if chat_stream.chat_type == "group":
            from src.core.config import get_core_config
            nickname = get_core_config().personality.nickname or "小狐狸"
            trigger_prefix = f"@{nickname} "

        full_content_lines = []
        if context_messages_text:
            full_content_lines.extend([
                f"【来自「{origin_stream_name}」的记忆凭证】",
                context_messages_text,
                "",
                "---",
                "",
            ])
        
        full_content_lines.append(f"{trigger_prefix}{trimmed_content}")
        
        if opening_hint.strip():
            full_content_lines.append("")
            full_content_lines.append(f"[心理暗示: {opening_hint.strip()}]")
        
        full_content = "\n".join(full_content_lines)
        final_platform = chat_stream.platform or origin_platform or "qq"

        virtual_message = Message(
            message_id=f"relay_{intent_id}",
            platform=final_platform,
            stream_id=chat_stream.stream_id,
            sender_id=virtual_sender_id,
            sender_name=f"来自「{origin_stream_name}」的转告",
            sender_role="system",
            content=full_content,
            processed_plain_text=full_content,
            message_type=MessageType.TEXT,
            chat_type=chat_stream.chat_type or "",
            time=time.time(),
            **extra_kwargs,
        )

        # 注入并启动
        try:
            chat_stream.context.add_unread_message(virtual_message)
            from src.core.transport.distribution.stream_loop_manager import get_stream_loop_manager
            await get_stream_loop_manager().start_stream_loop(chat_stream.stream_id)
        except Exception as error:
            logger.error(f"[relay] 注入虚拟消息失败: {error}")
            return False, f"注入虚拟消息失败: {error}"

        target_label = chat_stream.stream_name or chat_stream.stream_id[:8]
        return True, f"已成功将你的意识迁移至「{target_label}」，目标流的你稍后会自然续接对话。"

    async def _resolve_target_stream(
        self,
        *,
        target_stream_id: str,
        target_platform: str,
        target_user_id: str,
        target_group_id: str,
    ) -> Any | str:
        """解析目标聊天流。"""
        if target_stream_id:
            try:
                return await stream_api.get_or_create_stream(
                    stream_id=target_stream_id,
                    platform=target_platform,
                    user_id=target_user_id,
                    group_id=target_group_id,
                )
            except Exception as error:
                return f"获取目标流失败: {error}"

        if not target_platform:
            return "未提供 stream_id 时，必须提供 target_platform。"
        
        chat_type = "group" if target_group_id else "private"
        try:
            return await stream_api.get_or_create_stream(
                platform=target_platform,
                user_id=target_user_id,
                group_id=target_group_id,
                chat_type=chat_type,
            )
        except Exception as error:
            return f"构造目标流失败: {error}"
