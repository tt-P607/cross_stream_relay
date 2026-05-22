"""cross_stream_relay 插件的摘要存储与 reminder 同步逻辑。

整合自原 context_bridge/service.py，配置类改为 CrossStreamRelayConfig，
其余行为保持不变；其中：
  - ACTOR_REMINDER_BUCKET / ACTOR_REMINDER_NAME 保持 ``actor`` / ``跨聊天流上下文摘要``，
    避免影响已注入到 actor bucket 的现有 chatter prompt 行为。
  - LLM 请求名称前缀 ``cross_stream_relay_auto_summary_*``。
"""

from __future__ import annotations

import asyncio
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from typing import Any

from src.app.plugin_system.api import llm_api, prompt_api, storage_api
from src.app.plugin_system.types import LLMPayload, ROLE, Text
from src.core.models.message import Message


from .config import CrossStreamRelayConfig

ACTOR_REMINDER_BUCKET = "actor"
ACTOR_REMINDER_NAME = "跨聊天流上下文摘要"

_stream_locks: dict[str, asyncio.Lock] = {}


@dataclass(slots=True)
class StreamSummaryRecord:
    """单个聊天流摘要记录。"""

    stream_id: str
    stream_name: str
    platform: str
    chat_type: str
    target_id: str
    summary: str
    updated_at: str


@dataclass(slots=True)
class PendingMessageRecord:
    """待自动摘要的消息快照。"""

    message_id: str
    sender_name: str
    text: str
    direction: str
    stream_name: str
    platform: str
    chat_type: str
    timestamp: float


def _get_config(plugin: Any) -> CrossStreamRelayConfig:
    """获取插件配置，缺失时返回默认配置。"""

    if isinstance(plugin.config, CrossStreamRelayConfig):
        return plugin.config
    return CrossStreamRelayConfig()


def _record_key(stream_id: str) -> str:
    """生成摘要存储键。"""

    return f"summary_{stream_id}"


def _pending_key(stream_id: str) -> str:
    """生成待处理消息缓冲键。"""

    return f"pending_{stream_id}"


def _get_stream_lock(stream_id: str) -> asyncio.Lock:
    """按聊天流获取异步锁，避免并发重复摘要。"""

    lock = _stream_locks.get(stream_id)
    if lock is None:
        lock = asyncio.Lock()
        _stream_locks[stream_id] = lock
    return lock


def _trim_text(text: str, max_chars: int) -> str:
    """清洗并限制摘要长度。"""

    normalized = "\n".join(
        line.strip() for line in text.replace("\r\n", "\n").split("\n") if line.strip()
    ).strip()
    if not normalized:
        raise ValueError("summary 不能为空")

    if max_chars <= 0 or len(normalized) <= max_chars:
        return normalized

    if max_chars <= 3:
        return normalized[:max_chars]
    return normalized[: max_chars - 3].rstrip() + "..."


def _deserialize_record(data: dict[str, Any] | None) -> StreamSummaryRecord | None:
    """将 JSON 数据转换为摘要记录。"""

    if not data:
        return None

    summary = data.get("summary")
    stream_id = data.get("stream_id")
    if not isinstance(summary, str) or not summary.strip():
        return None
    if not isinstance(stream_id, str) or not stream_id.strip():
        return None

    return StreamSummaryRecord(
        stream_id=stream_id,
        stream_name=str(data.get("stream_name", "") or ""),
        platform=str(data.get("platform", "") or ""),
        chat_type=str(data.get("chat_type", "") or ""),
        target_id=str(data.get("target_id", "") or ""),
        summary=summary.strip(),
        updated_at=str(data.get("updated_at", "") or ""),
    )


def _deserialize_pending_record(data: dict[str, Any]) -> PendingMessageRecord | None:
    """将字典转换为待处理消息记录。"""

    text = data.get("text")
    if not isinstance(text, str) or not text.strip():
        return None

    return PendingMessageRecord(
        message_id=str(data.get("message_id", "") or ""),
        sender_name=str(data.get("sender_name", "") or ""),
        text=text.strip(),
        direction=str(data.get("direction", "unknown") or "unknown"),
        stream_name=str(data.get("stream_name", "") or ""),
        platform=str(data.get("platform", "") or ""),
        chat_type=str(data.get("chat_type", "") or ""),
        timestamp=float(data.get("timestamp", 0.0) or 0.0),
    )


def _stream_name_from_message(message: "Message", direction: str) -> str:
    """尽量从消息中推导聊天流名称。"""

    extra = message.extra if isinstance(message.extra, dict) else {}
    group_name = extra.get("group_name") or extra.get("target_group_name")
    if isinstance(group_name, str) and group_name.strip():
        return group_name.strip()

    if message.chat_type != "group":
        # 私聊：取对方（用户）的名称
        if direction == "inbound":
            # inbound 时，sender_name 是用户
            if message.sender_name and message.sender_name.strip():
                return message.sender_name.strip()
        else:
            # outbound 时，target_user_name 是用户
            target_user_name = extra.get("target_user_name")
            if isinstance(target_user_name, str) and target_user_name.strip():
                return target_user_name.strip()

    target_user_name = extra.get("target_user_name")
    if isinstance(target_user_name, str) and target_user_name.strip():
        return target_user_name.strip()

    if message.sender_name and message.sender_name.strip():
        return message.sender_name.strip()

    return ""


def _normalize_message_timestamp(message: "Message") -> float:
    """将消息时间统一归一化为时间戳。"""

    raw_time = message.time
    if isinstance(raw_time, datetime):
        return raw_time.timestamp()
    if isinstance(raw_time, (int, float)):
        return float(raw_time)
    return 0.0


def _message_to_pending_record(message: "Message", direction: str) -> PendingMessageRecord | None:
    """将 Message 转为待处理的轻量快照。"""

    text = message.processed_plain_text or str(message.content or "")
    normalized_text = _trim_text(text, 800)
    return PendingMessageRecord(
        message_id=str(message.message_id or ""),
        sender_name=str(message.sender_name or "未知发送者"),
        text=normalized_text,
        direction=direction,
        stream_name=_stream_name_from_message(message, direction),
        platform=str(message.platform or ""),
        chat_type=str(message.chat_type or ""),
        timestamp=_normalize_message_timestamp(message),
    )


async def _load_pending_messages(plugin: Any, stream_id: str) -> list[PendingMessageRecord]:
    """读取某个聊天流当前累计的待摘要消息。"""

    try:
        payload = await storage_api.load_json(plugin.plugin_name, _pending_key(stream_id))
    except Exception:
        return []
    if not payload:
        return []

    raw_items = payload.get("messages", [])
    if not isinstance(raw_items, list):
        return []

    result: list[PendingMessageRecord] = []
    for item in raw_items:
        if not isinstance(item, dict):
            continue
        record = _deserialize_pending_record(item)
        if record is not None:
            result.append(record)
    return result


async def _save_pending_messages(
    plugin: Any,
    stream_id: str,
    messages: list[PendingMessageRecord],
) -> None:
    """保存某个聊天流当前累计的待摘要消息。"""

    await storage_api.save_json(
        plugin.plugin_name,
        _pending_key(stream_id),
        {"messages": [asdict(item) for item in messages]},
    )


def _format_pending_messages(messages: list[PendingMessageRecord]) -> str:
    """格式化最近一批消息，供 utils 模型更新摘要/提取心智。"""

    lines: list[str] = []
    for index, item in enumerate(messages, start=1):
        lines.append(
            f"{index}. [{item.direction}] {item.sender_name}: {item.text}"
        )
    return "\n".join(lines)


async def _generate_updated_summary(
    plugin: Any,
    stream_id: str,
    previous_summary: str,
    messages: list[PendingMessageRecord],
) -> str:
    """使用 utils 模型根据旧摘要和最新消息更新摘要。"""

    config = _get_config(plugin)
    model_set = llm_api.get_model_set_by_task(config.plugin.auto_summary_task_name)
    request = llm_api.create_llm_request(
        model_set=model_set,
        request_name=f"cross_stream_relay_auto_summary_{stream_id[:8]}",
    )

    now_iso = datetime.now(UTC).isoformat(timespec="seconds")

    request.add_payload(
        LLMPayload(
            ROLE.SYSTEM,
            Text(
                "你是聊天流摘要维护器。你的任务是基于旧摘要和最新一批消息，"
                "输出一份覆盖后的新摘要。\n"
                "摘要必须保留：与 bot 强相关的内容、核心话题主体、背景上下文、已确认事实、"
                "用户偏好或约束、未完成事项、下一步。\n"
                "不要输出流水账，不要逐条复述消息，不要写未被确认的猜测。\n"
                "如果旧摘要中存在已经失效或被纠正的内容，你必须在新摘要中修正而不是保留。\n"
                "【重要】请在摘要的第一行以 [物理ID: xxx] 的格式标注该流的 QQ 号或群号（如果已知）。\n"
                "只输出摘要正文，不要加标题，不要加解释。"
            ),
        )
    )

    request.add_payload(
        LLMPayload(
            ROLE.USER,
            Text(
                f"【当前时间】: {now_iso}\n\n"
                "【旧摘要】\n"
                f"{previous_summary.strip() or '（暂无）'}\n\n"
                "【最新消息批次】\n"
                f"{_format_pending_messages(messages)}\n\n"
                "请输出更新后的完整摘要。"
            ),
        )
    )

    response = await request.send(stream=False)
    await response
    summary_text = str(response.message or "").strip()
    if not summary_text:
        raise ValueError("自动摘要生成了空结果")
    return summary_text


def _resolve_summary_meta(
    previous: StreamSummaryRecord | None,
    messages: list[PendingMessageRecord],
    stream_id: str,
) -> tuple[str, str, str, str, str]:
    """综合旧记录与最新消息，确定摘要存储的元信息。"""

    last_message = messages[-1] if messages else None
    stream_name = ""
    platform = ""
    chat_type = ""
    target_id = ""

    if previous is not None:
        stream_name = previous.stream_name
        platform = previous.platform
        chat_type = previous.chat_type
        target_id = previous.target_id

    if last_message is not None:
        if last_message.stream_name:
            stream_name = last_message.stream_name
        if last_message.platform:
            platform = last_message.platform
        if last_message.chat_type:
            chat_type = last_message.chat_type

    if not stream_name:
        stream_name = stream_id[:8]

    return stream_id, stream_name, platform, chat_type, target_id


def _build_stream_title(record: StreamSummaryRecord) -> str:
    """为聊天流生成便于模型识别的标题。"""

    if record.stream_name:
        return record.stream_name
    short_id = record.stream_id[:8]
    return f"{record.platform}:{record.chat_type}:{short_id}"


async def list_summary_records(plugin: Any) -> list[StreamSummaryRecord]:
    """读取插件持久化的全部聊天流摘要。"""

    keys = await storage_api.list_json(plugin.plugin_name)
    records: list[StreamSummaryRecord] = []
    for key in keys:
        if not key.startswith("summary_"):
            continue
        record = _deserialize_record(
            await storage_api.load_json(plugin.plugin_name, key)
        )
        if record is not None:
            records.append(record)

    records.sort(key=lambda item: item.updated_at, reverse=True)
    return records




async def _upsert_summary_record(
    plugin: Any,
    *,
    stream_id: str,
    stream_name: str,
    platform: str,
    chat_type: str,
    target_id: str,
    summary: str,
) -> bool:
    """按显式元信息写入摘要记录。"""

    config = _get_config(plugin)
    normalized_summary = _trim_text(summary, config.plugin.max_summary_chars)
    key = _record_key(stream_id)
    previous = _deserialize_record(await storage_api.load_json(plugin.plugin_name, key))

    record = StreamSummaryRecord(
        stream_id=stream_id,
        stream_name=stream_name,
        platform=platform,
        chat_type=chat_type,
        target_id=target_id,
        summary=normalized_summary,
        updated_at=datetime.now(UTC).isoformat(timespec="seconds"),
    )
    await storage_api.save_json(plugin.plugin_name, key, asdict(record))
    return previous is None or previous.summary != normalized_summary


def _extract_target_id(message: "Message") -> str:
    """从消息中提取稳定的聊天流目标 ID。

    私聊时优先从 extra["target_user_id"] 取对方 QQ 号，
    避免 ON_MESSAGE_SENT 时 sender_id 已被覆盖为 bot 自身 ID 的问题。
    群聊时取 group_id。
    """
    extra = message.extra if isinstance(message.extra, dict) else {}
    if message.chat_type == "group":
        return str(extra.get("group_id", "") or extra.get("target_group_id", ""))
    # 私聊：优先用框架标准字段 target_user_id（inbound/outbound 均稳定）
    target_user_id = str(extra.get("target_user_id") or "")
    if target_user_id:
        return target_user_id
    # 兜底：收到消息时 sender_id 是对方，尽量也能用
    return str(message.sender_id or "")


async def upsert_summary(plugin: Any, chat_stream: Any, summary: str) -> bool:
    """写入当前聊天流摘要，返回摘要内容是否发生变化。"""

    target_id = ""
    if hasattr(chat_stream, "message") and chat_stream.message:
        msg = chat_stream.message
        target_id = _extract_target_id(msg)

    return await _upsert_summary_record(
        plugin,
        stream_id=str(chat_stream.stream_id),
        stream_name=str(getattr(chat_stream, "stream_name", "") or ""),
        platform=str(getattr(chat_stream, "platform", "") or ""),
        chat_type=str(getattr(chat_stream, "chat_type", "") or ""),
        target_id=target_id,
        summary=summary,
    )


async def collect_message_for_auto_summary(
    plugin: Any,
    message: Message,
    *,
    direction: str,
) -> bool:
    """收集消息并在达到阈值后用 utils 模型自动更新摘要。"""

    from .privacy_filter import should_collect_message

    config = _get_config(plugin)
    if not config.plugin.auto_summary_enabled:
        return False

    target_id = _extract_target_id(message)
    if not should_collect_message(config, message.chat_type, target_id):
        return False

    batch_size = max(1, int(config.plugin.auto_summary_batch_size))
    stream_id = str(message.stream_id or "").strip()
    if not stream_id:
        return False

    pending_record = _message_to_pending_record(message, direction)
    if pending_record is None:
        return False

    async with _get_stream_lock(stream_id):
        pending_messages = await _load_pending_messages(plugin, stream_id)
        pending_messages.append(pending_record)

        previous_record = _deserialize_record(
            await storage_api.load_json(plugin.plugin_name, _record_key(stream_id))
        )
        changed = False

        while len(pending_messages) >= batch_size:
            current_batch = pending_messages[:batch_size]
            previous_summary = previous_record.summary if previous_record is not None else ""
            updated_summary = await _generate_updated_summary(
                plugin,
                stream_id,
                previous_summary,
                current_batch,
            )
            resolved_stream_id, stream_name, platform, chat_type, prev_target_id = _resolve_summary_meta(
                previous_record,
                current_batch,
                stream_id,
            )

            final_target_id = prev_target_id or target_id

            changed = (
                await _upsert_summary_record(
                    plugin,
                    stream_id=resolved_stream_id,
                    stream_name=stream_name,
                    platform=platform,
                    chat_type=chat_type,
                    target_id=final_target_id,
                    summary=updated_summary,
                )
            ) or changed
            previous_record = _deserialize_record(
                await storage_api.load_json(plugin.plugin_name, _record_key(stream_id))
            )
            pending_messages = pending_messages[batch_size:]

        await _save_pending_messages(plugin, stream_id, pending_messages)

    if changed:
        await sync_actor_reminder(
            plugin,
            current_chat_type=message.chat_type,
            current_stream_id=str(message.stream_id or ""),
        )

    return changed


def _format_stream_header(index: int, record: StreamSummaryRecord) -> str:
    """构建单条聊天流的标题行（暴露 target_id）。"""

    title = _build_stream_title(record)
    chat_type = record.chat_type or "unknown"
    platform = record.platform or "unknown"
    
    # 强化物理 ID 的展示，方便 LLM 跨流转告时核对
    if record.target_id:
        if chat_type == "group":
            id_part = f"【群号: {record.target_id}】"
        elif chat_type == "private":
            id_part = f"【QQ: {record.target_id}】"
        else:
            id_part = f"【ID: {record.target_id}】"
    else:
        id_part = "【ID: 未知】"
        
    return (
        f"{index}. {title} {id_part} [{platform}:{chat_type}] "
        f"更新: {record.updated_at or 'unknown'}"
    )


def build_actor_reminder(
    plugin: Any,
    records: list[StreamSummaryRecord],
    visible_stream_limit: int,
    current_chat_type: str = "",
    current_stream_id: str = "",
    today_memory_summary: str = "",
    today_memory_date: str = "",
) -> str:
    """构建注入到 actor bucket 的跨聊天流摘要 reminder。

    Args:
        plugin: 插件实例
        records: 聊天流摘要列表
        visible_stream_limit: 可见聊天流上限
        current_chat_type: 当前正在渲染 reminder 的聊天类型
        current_stream_id: 当前 stream_id（用于在标题处标注"当前流"）
        today_memory_summary: 当前群当日短期记忆正文（仅群聊上下文注入）
        today_memory_date: 当前群当日短期记忆对应日期
    """

    from .privacy_filter import should_show_in_reminder

    config = _get_config(plugin)

    lines = [
        "【跨聊天流互通核心指令】",
        "你现在拥有像人类一样在不同聊天场景（群聊、私聊）之间自由穿梭并保持记忆连贯的能力。",
        "你的目标是实现「跨流无缝衔接」：当用户在 A 群叫你去私聊找他，或者在私聊中让你去 B 群带话时，你应当能够完美续上之前的对话逻辑和意图。",
        "",
        "你维护的是一份跨聊天流上下文索引，用于在不同流之间共享高价值的上下文。你必须遵守以下规则：",
        "1. 跨流转告：当你需要从当前流切换到另一个流执行任务时，请先调用 tool-find_target_stream 找到目标流，然后调用 action-relay_to_stream 将你的意图、关键原话和执行提示推送过去。目标流的你（Chatter）会收到这条转告并自然地继续对话。",
        "2. 自动摘要：插件会自动更新各流摘要。只有当摘要偏离事实或有关键新信息（如约定的私聊、跨流待办）需要立刻记录时，才调用 action-update_stream_summary。",
        "3. 摘要内容：必须保留与你强相关的设定、核心话题、关键背景、已确认事实、用户偏好、未完成的跨流事项及下一步计划。",
        "4. 严禁流水账：摘要应精炼且具备导向性，方便另一个流的你快速进入状态。",
        "",
        f"以下为最近可见的聊天流摘要（最多 {max(0, visible_stream_limit)} 条，按更新时间倒序）：",
    ]

    filtered_records = [
        r for r in records
        if should_show_in_reminder(config, r.chat_type, r.target_id, current_chat_type)
    ]

    visible_records = filtered_records[: max(0, visible_stream_limit)]
    if not visible_records:
        lines.append("- 当前没有可见的聊天流摘要。")
    else:
        for index, record in enumerate(visible_records, start=1):
            header = _format_stream_header(index, record)
            if current_stream_id and record.stream_id == current_stream_id:
                header += "  ← 当前聊天流"
            lines.append(header)
            lines.append(f"   摘要：{record.summary}")
            lines.append("")
        # 收尾去除最后多余空行
        if lines and lines[-1] == "":
            lines.pop()

    if today_memory_summary:
        lines.extend([
            "",
            f"【本群今日短期记忆 · {today_memory_date}】",
            today_memory_summary,
            "（说明：上方摘要为持续跟进的精简版；这份『短期记忆』是 actor 模型对当日群聊的全量总结，"
            "用于补全摘要可能遗漏的细节。每发若干轮或闲置一段时间后会全量刷新覆盖。）",
        ])

    lines.extend([
        "",
        "可用工具：",
        "- tool-get_stream_raw_context: 查询【其他聊天流】的原始聊天记录（不能用于本流，本流上下文已直接可见）",
        "- tool-get_daily_memory: 跨群查询某群最近几天（默认含今天）的短期记忆，用以补全摘要细节",
        "- tool-find_target_stream: 把名字/索引/QQ 号反查成 stream_id 等元组，供 relay_to_stream 使用",
        "- action-relay_to_stream: 把一段转告便条直接推送到目标流，并在目标流冷态时主动唤醒",
    ])

    return "\n".join(lines)


async def sync_actor_reminder(
    plugin: Any,
    current_chat_type: str = "",
    current_stream_id: str = "",
) -> str:
    """同步 actor bucket 中的跨聊天流摘要 reminder。"""

    config = _get_config(plugin)
    
    # 如果配置禁用了 reminder 注入，直接返回空字符串
    if not config.plugin.inject_summary_reminder:
        return ""

    today_memory_summary = ""
    today_memory_date = ""
    if (
        current_chat_type == "group"
        and current_stream_id
        and config.daily_memory.enabled
        and config.daily_memory.inject_into_reminder
    ):
        from .daily_memory import get_today_memory_for_stream
        record = await get_today_memory_for_stream(plugin, current_stream_id)
        if record is not None:
            today_memory_summary = record.summary
            today_memory_date = record.memory_date

    reminder_text = build_actor_reminder(
        plugin,
        await list_summary_records(plugin),
        visible_stream_limit=config.plugin.visible_stream_limit,
        current_chat_type=current_chat_type,
        current_stream_id=current_stream_id,
        today_memory_summary=today_memory_summary,
        today_memory_date=today_memory_date,
    )
    try:
        prompt_api.add_system_reminder(
            ACTOR_REMINDER_BUCKET,
            ACTOR_REMINDER_NAME,
            reminder_text,
        )
    except Exception as error:
        # 某些 chatter（如 KFC）使用自定义 context_manager，不支持动态 reminder 注入
        # 这种情况下静默失败，不影响主流程
        from src.app.plugin_system.api.log_api import get_logger
        logger = get_logger("cross_stream_relay.service")
        logger.debug(
            f"[sync_actor_reminder] 注入 reminder 失败（可能是 chatter 使用自定义 context_manager）: {error}"
        )
    return reminder_text
