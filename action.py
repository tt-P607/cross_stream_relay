"""cross_stream_relay 插件 Action 组件。

包含三个 Action：

1. UpdateStreamSummaryAction（接自原 context_bridge.update_context_bridge_summary）
   人工纠偏当前流摘要。

2. RelayToStreamAction（核心能力）
   把转告便条以虚拟 Message 形式注入目标流的 unread_messages 队列，
   冷态时自动启动目标流的 loop。

3. RelayReplyAction（回执能力）
   目标流的 AI 处理完跨流转告后，把处理结果以虚拟消息推回来源流，
   使发起转告的那一侧能感知到"那边搞定了"。
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
    associated_types: list[str] = ["text"]

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
    associated_types: list[str] = ["text"]

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


class RelayReplyAction(BaseAction):
    """跨流转告回执：目标流处理完转告后，把结果推回来源流。

    与 RelayToStreamAction 配对使用：
      - RelayToStreamAction：A流 → B流（发起转告）
      - RelayReplyAction：B流 → A流（反馈回执）

    回执以虚拟消息形式注入来源流的 unread_messages 队列，
    带有 ``_relay_reply`` 标记以区别于普通转告，避免无限循环。

    来源流 ID 由插件自动从当前流的历史消息中提取（转告消息的 extra 元数据），
    不需要也不应该由模型手动指定。
    """

    action_name = "relay_reply"
    action_description = (
        "跨流转告回执工具。当你在目标流处理完一条来自其他流的跨流转告后，"
        "用此工具把处理结果反馈给发起转告的那个流。\n"
        "\n"
        "就像帮人办完事后跟对方说一声「搞定了」一样自然。\n"
        "\n"
        "使用场景：你在 B 流收到了来自 A 流的转告（消息中带有「来自xxx的转告」标记），"
        "处理完毕后——无论是成功回复了用户、还是判断暂不合适开口——"
        "都应当调用此工具把结果告诉 A 流的那个你，让那边知道进展。\n"
        "\n"
        "通过 origin_group_id（来源群号）或 origin_user_id（来源 QQ号）指定回执发往哪里，"
        "通常从转告消息内容中的 [群名](群号) 或 [昵称](QQ号) 标记获取。"
        "如果两个都留空，插件会尝试从当前流的历史消息中自动查找来源流。"
    )

    chatter_allow: list[str] = []
    associated_types: list[str] = ["text"]

    async def execute(
        self,
        reply_content: Annotated[
            str,
            "回执内容：你在目标流处理转告后的结果摘要。比如做了什么回复、用户什么反应、"
            "或者判断暂不合适开口的理由。简洁明了即可。",
        ],
        origin_group_id: Annotated[
            str,
            "来源群号（群聊场景）。从转告消息内容中的 [群名](群号) 标记获取。私聊场景留空。",
        ] = "",
        origin_user_id: Annotated[
            str,
            "来源 QQ号（私聊场景）。从转告消息内容中的 [昵称](QQ号) 标记获取。群聊场景留空。",
        ] = "",
        origin_platform: Annotated[
            str,
            "来源平台标识，默认 'qq'。",
        ] = "qq",
    ) -> tuple[bool, str]:
        """执行跨流转告回执。通过群号/QQ号指定来源流，和 relay_to_stream 对称。"""

        config = self.plugin.config if isinstance(self.plugin.config, CrossStreamRelayConfig) else None
        if config is None:
            return False, "插件配置异常，无法执行回执"
        if not config.relay.enabled:
            return False, "跨流转告功能已在配置中禁用"

        if not reply_content or not reply_content.strip():
            return False, "reply_content 不能为空，请简述处理结果"

        # 截断回执内容
        max_chars = max(64, int(config.relay.max_relay_chars))
        trimmed_reply = _trim_relay_content(reply_content.strip(), max_chars)

        current_stream_id = str(self.chat_stream.stream_id)
        gid = origin_group_id.strip()
        uid = origin_user_id.strip()
        platform = origin_platform.strip() or "qq"

        # 解析来源流：优先用模型传入的群号/QQ号，留空时自动从历史提取
        if gid or uid:
            chat_type = "group" if gid else "private"
            try:
                origin_chat_stream = await stream_api.get_or_create_stream(
                    platform=platform,
                    user_id=uid,
                    group_id=gid,
                    chat_type=chat_type,
                )
            except Exception as error:
                return False, f"根据群号/QQ号获取来源流失败: {error}"
        else:
            # 自动从当前流历史消息中查找来源流 stream_id
            resolved_origin_id = self._find_relay_origin()
            if not resolved_origin_id:
                return False, (
                    "未提供来源群号/QQ号，且未能从当前流历史消息中自动找到转告来源。"
                    "请从转告消息内容中找到 [群名](群号) 或 [昵称](QQ号) 后传入。"
                )
            try:
                origin_chat_stream = await stream_api.get_or_create_stream(
                    stream_id=resolved_origin_id,
                    platform=platform,
                )
            except Exception as error:
                return False, f"获取来源流失败: {error}"

        resolved_origin_id = str(origin_chat_stream.stream_id)

        # 防止给自己回执（除非允许自我转告）
        if resolved_origin_id == current_stream_id and not config.relay.allow_self_relay:
            return False, "来源流就是当前流，无需回执。"

        origin_stream_name = str(
            getattr(origin_chat_stream, "stream_name", "") or resolved_origin_id[:8]
        )
        origin_platform_resolved = str(
            getattr(origin_chat_stream, "platform", "") or platform
        )

        # 构造回执虚拟消息
        current_stream_name = str(
            getattr(self.chat_stream, "stream_name", "") or current_stream_id[:8]
        )
        reply_id = uuid4().hex[:16]

        full_content = (
            f"【来自「{current_stream_name}」的回执】\n"
            f"你之前发来的转告已收到并处理。\n"
            f"处理结果：{trimmed_reply}"
        )

        extra_kwargs: dict[str, Any] = {
            "_relay_reply": True,
            "_relay_reply_id": reply_id,
            "_relay_reply_origin_stream_id": resolved_origin_id,
            "_relay_reply_from_stream_id": current_stream_id,
            "_relay_reply_from_stream_name": current_stream_name,
            "_relay_created_at": time.time(),
        }

        virtual_message = Message(
            message_id=f"relay_reply_{reply_id}",
            platform=origin_platform_resolved,
            stream_id=resolved_origin_id,
            sender_id="0",
            sender_name=f"来自「{current_stream_name}」的回执",
            sender_role="system",
            content=full_content,
            processed_plain_text=full_content,
            message_type=MessageType.TEXT,
            chat_type=str(getattr(origin_chat_stream, "chat_type", "") or ""),
            time=time.time(),
            **extra_kwargs,
        )

        # 注入并启动来源流
        try:
            origin_chat_stream.context.add_unread_message(virtual_message)
            from src.core.transport.distribution.stream_loop_manager import get_stream_loop_manager
            await get_stream_loop_manager().start_stream_loop(resolved_origin_id)
        except Exception as error:
            logger.error(f"[relay_reply] 注入回执虚拟消息失败: {error}")
            return False, f"注入回执失败: {error}"

        return True, (
            f"已将回执发送至「{origin_stream_name}」，"
            f"那边的你会知道这边已经处理好了。"
        )

    def _find_relay_origin(self) -> str:
        """从当前流的历史消息中查找最近的转告来源流 ID。

        依次扫描 unread_messages 和 history_messages 中带
        ``_relay_origin_stream_id`` extra 的消息（跳过回执消息自身），
        返回最近一条的来源流 ID。找不到则返回空字符串。
        """

        chat_stream = self.chat_stream
        context = getattr(chat_stream, "context", None)
        if context is None:
            return ""

        unreads = getattr(context, "unread_messages", []) or []
        history = getattr(context, "history_messages", []) or []

        for msg in reversed(unreads):
            origin_id = self._extract_origin_id(msg)
            if origin_id:
                return origin_id

        for msg in reversed(history):
            origin_id = self._extract_origin_id(msg)
            if origin_id:
                return origin_id

        return ""

    @staticmethod
    def _extract_origin_id(msg: Any) -> str:
        """从消息的 extra 中提取来源流 ID，跳过回执消息自身。"""

        extra = getattr(msg, "extra", None)
        if not isinstance(extra, dict):
            return ""
        if extra.get("_relay_reply"):
            return ""
        return str(extra.get("_relay_origin_stream_id") or "")
