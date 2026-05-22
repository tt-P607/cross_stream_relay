"""cross_stream_relay 每日短期记忆模块。

整合自原 context_bridge/daily_memory.py，配置类改为 CrossStreamRelayConfig。
LLM 请求名称前缀更新为 ``cross_stream_relay_daily_memory_*``。

与 service.py 中的"摘要"机制并行运行：
  - 摘要：utils 模型，按少量批次小步刷新，覆盖任意聊天流（群/私）
  - 短期记忆：actor 模型，对当天全部消息做一次性全量总结，仅群聊

触发条件（任一满足即触发，触发后两个进度都重置）：
  1. bot 完成 N 轮交互（一轮 = 收到 inbound 后 bot 首次发出 outbound）
  2. 距离上次总结超过空闲时间（默认 3 小时）

跨天处理：
  - 事件触发：每次新消息进入时若发现日期已变，先用 actor 模型重做昨天的全量归档
  - 守护循环：plugin 启动后台任务每 60 秒扫描所有 state，处理无活动也要归档的群

存储键：
  - daily_state_{stream_id}                  : 群聊计数与日期状态
  - daily_memory_{stream_id}_{YYYY-MM-DD}    : 当日全量总结记录
"""

from __future__ import annotations

import asyncio
import time
from dataclasses import asdict, dataclass
from datetime import date, datetime, timedelta
from typing import TYPE_CHECKING, Any

from src.app.plugin_system.api import adapter_api, llm_api, message_api, storage_api
from src.app.plugin_system.api.log_api import get_logger
from src.app.plugin_system.types import LLMPayload, ROLE, Text
from src.core.config import get_core_config
from src.core.models.message import Message

from .config import CrossStreamRelayConfig

if TYPE_CHECKING:
    pass

logger = get_logger("cross_stream_relay.daily_memory")

_state_locks: dict[str, asyncio.Lock] = {}


@dataclass(slots=True)
class DailyMemoryRecord:
    """单条每日短期记忆记录。"""

    stream_id: str
    group_id: str
    group_name: str
    platform: str
    chat_type: str
    memory_date: str
    summary: str
    message_count: int
    updated_at: str
    last_summarized_ts: float = 0.0  # 上次摘要已覆盖到的最后一条消息的时间戳，用于增量更新


@dataclass(slots=True)
class DailyState:
    """单个聊天流的当日计数状态。"""

    stream_id: str
    group_id: str
    group_name: str
    platform: str
    chat_type: str
    current_date: str
    round_count: int
    last_summary_at: float
    last_event_direction: str


def _state_key(stream_id: str) -> str:
    """生成状态存储键。"""

    return f"daily_state_{stream_id}"


def _memory_key(stream_id: str, memory_date: str) -> str:
    """生成短期记忆存储键。"""

    return f"daily_memory_{stream_id}_{memory_date}"


def _state_lock(stream_id: str) -> asyncio.Lock:
    """按 stream_id 取得异步锁。"""

    lock = _state_locks.get(stream_id)
    if lock is None:
        lock = asyncio.Lock()
        _state_locks[stream_id] = lock
    return lock


def _today_str() -> str:
    """返回本地日期字符串（YYYY-MM-DD）。"""

    return date.today().isoformat()


def _yesterday_str_of(d: str) -> str:
    """根据给定日期字符串返回它的前一天。"""

    parsed = datetime.strptime(d, "%Y-%m-%d").date()
    return (parsed - timedelta(days=1)).isoformat()


def _get_config(plugin: Any) -> CrossStreamRelayConfig:
    """获取插件配置，缺失时返回默认配置。"""

    if isinstance(plugin.config, CrossStreamRelayConfig):
        return plugin.config
    return CrossStreamRelayConfig()


def _load_state(data: dict[str, Any] | None) -> DailyState | None:
    """反序列化状态文件。"""

    if not data:
        return None
    stream_id = data.get("stream_id")
    if not isinstance(stream_id, str) or not stream_id.strip():
        return None
    return DailyState(
        stream_id=stream_id,
        group_id=str(data.get("group_id", "") or ""),
        group_name=str(data.get("group_name", "") or ""),
        platform=str(data.get("platform", "") or ""),
        chat_type=str(data.get("chat_type", "") or ""),
        current_date=str(data.get("current_date", "") or _today_str()),
        round_count=int(data.get("round_count", 0) or 0),
        last_summary_at=float(data.get("last_summary_at", 0.0) or 0.0),
        last_event_direction=str(data.get("last_event_direction", "") or ""),
    )


def _load_record(data: dict[str, Any] | None) -> DailyMemoryRecord | None:
    """反序列化每日总结记录。"""

    if not data:
        return None
    summary = data.get("summary")
    stream_id = data.get("stream_id")
    if not isinstance(summary, str) or not summary.strip():
        return None
    if not isinstance(stream_id, str) or not stream_id.strip():
        return None
    return DailyMemoryRecord(
        stream_id=stream_id,
        group_id=str(data.get("group_id", "") or ""),
        group_name=str(data.get("group_name", "") or ""),
        platform=str(data.get("platform", "") or ""),
        chat_type=str(data.get("chat_type", "") or ""),
        memory_date=str(data.get("memory_date", "") or ""),
        summary=summary.strip(),
        message_count=int(data.get("message_count", 0) or 0),
        updated_at=str(data.get("updated_at", "") or ""),
        last_summarized_ts=float(data.get("last_summarized_ts", 0.0) or 0.0),
    )


def _trim_text(text: str, max_chars: int) -> str:
    """归一化并按上限截断文本。"""

    normalized = "\n".join(
        line.strip() for line in text.replace("\r\n", "\n").split("\n") if line.strip()
    ).strip()
    if not normalized:
        return ""
    if max_chars <= 0 or len(normalized) <= max_chars:
        return normalized
    if max_chars <= 3:
        return normalized[:max_chars]
    return normalized[: max_chars - 3].rstrip() + "..."


def _extract_group_meta(message: Message) -> tuple[str, str]:
    """从消息中提取群号与群名。"""

    extra = message.extra if isinstance(message.extra, dict) else {}
    group_id = str(extra.get("group_id", "") or extra.get("target_group_id", "") or "")
    group_name = str(extra.get("group_name", "") or extra.get("target_group_name", "") or "")
    return group_id, group_name


def _date_bounds(memory_date: str) -> tuple[float, float]:
    """返回某日期的本地零点起止时间戳。"""

    start = datetime.strptime(memory_date, "%Y-%m-%d")
    end = start + timedelta(days=1)
    return start.timestamp(), end.timestamp()


async def _fetch_day_messages(
    stream_id: str,
    memory_date: str,
    since_ts: float = 0.0,
) -> list[dict[str, Any]]:
    """拉取某 stream 当日消息（按时间升序）。

    Args:
        stream_id: 聊天流 ID。
        memory_date: 日期字符串 YYYY-MM-DD。
        since_ts: 仅拉取这个时间戳之后（不含）的消息。0 表示当日全量。
    """

    start_ts, end_ts = _date_bounds(memory_date)
    # 增量起点：取 max(当日 0 点, since_ts + 1 微秒) 避免重复拉取已总结过的消息
    effective_start = max(start_ts, since_ts + 1e-6) if since_ts > 0 else start_ts
    if effective_start >= end_ts:
        return []
    messages = await message_api.get_messages_by_time_in_chat(
        stream_id=stream_id,
        start_time=effective_start,
        end_time=end_ts,
        limit=0,
        limit_mode="earliest",
        filter_bot=False,
        filter_command=True,
    )
    return messages


def _extract_message_timestamp(raw_message: dict[str, Any]) -> float:
    """从消息字典中提取 unix 时间戳（用于水位线维护）。"""

    raw_time = raw_message.get("time") or raw_message.get("timestamp") or 0.0
    if isinstance(raw_time, datetime):
        return raw_time.timestamp()
    try:
        return float(raw_time)
    except (TypeError, ValueError):
        return 0.0


def _build_persona_prompt() -> str:
    """从 core.toml [personality] 段构造人设上下文，用于注入到短期记忆总结之前。

    取自字段：bot_nickname / personality_core / personality_side / identity / background_story。
    任一字段缺失都会被跳过；若全部为空则返回空串。
    """

    try:
        cfg = get_core_config()
    except Exception:
        return ""

    persona = getattr(cfg, "personality", None)
    if persona is None:
        return ""

    bot_nickname = (getattr(persona, "bot_nickname", "") or "").strip()
    personality_core = (getattr(persona, "personality_core", "") or "").strip()
    personality_side = (getattr(persona, "personality_side", "") or "").strip()
    identity = (getattr(persona, "identity", "") or "").strip()
    background_story = (getattr(persona, "background_story", "") or "").strip()

    parts: list[str] = ["【你的人设（请始终以此身份的视角与口吻进行回忆）】"]
    if bot_nickname:
        parts.append(f"- 名字：{bot_nickname}")
    if identity:
        parts.append(f"- 身份：{identity}")
    if personality_core:
        parts.append(f"- 核心人格：{personality_core}")
    if personality_side:
        parts.append(f"- 人格侧面：{personality_side}")
    if background_story:
        parts.append(f"- 背景故事（仅作为内在背景知识，回忆中不要主动复述）：{background_story}")

    if len(parts) == 1:
        return ""
    return "\n".join(parts)


async def _generate_full_day_summary(
    plugin: Any,
    state: DailyState,
    memory_date: str,
    *,
    force_full: bool = False,
) -> DailyMemoryRecord | None:
    """对指定日期生成短期记忆。

    模式：
      - 增量更新（默认）：读取已有记录的 last_summarized_ts 作为水位线，
        只拉取水位线之后的新消息，让 LLM 在「上次摘要」基础上把新内容融合进去。
      - 全量重做（force_full=True）：忽略水位线，重新读取当天全部消息从头总结。

    Args:
        plugin: 插件实例
        state: 当前 stream 状态
        memory_date: 目标日期
        force_full: 是否强制全量重做（用于人工触发或跨天兜底）
    """

    config = _get_config(plugin)

    # ── 读取已有记录（用于增量基线 / 水位线）──
    existing_record: DailyMemoryRecord | None = None
    if not force_full:
        existing_record = await get_memory(plugin, state.stream_id, memory_date)

    since_ts = 0.0
    if existing_record is not None and existing_record.last_summarized_ts > 0:
        since_ts = existing_record.last_summarized_ts

    raw_messages = await _fetch_day_messages(state.stream_id, memory_date, since_ts=since_ts)
    if not raw_messages:
        if existing_record is None:
            logger.debug(
                f"[daily_memory] stream={state.stream_id} date={memory_date} 当日无消息，跳过总结"
            )
        else:
            logger.debug(
                f"[daily_memory] stream={state.stream_id} date={memory_date} "
                f"自上次水位线 {since_ts} 以来无新消息，跳过更新"
            )
        return existing_record

    formatted = await message_api.build_readable_messages_to_str(
        messages=raw_messages,
        replace_bot_name=False,
        merge_messages=False,
        timestamp_mode="absolute",
        truncate=False,
    )
    if not formatted.strip():
        return existing_record

    # 计算新水位线（取本次新增消息的最大时间戳）
    new_watermark = max(
        (_extract_message_timestamp(m) for m in raw_messages),
        default=since_ts,
    )

    # ── 取 bot 自身身份（用于第一人称视角）──
    bot_name = ""
    bot_id = ""
    if state.platform:
        try:
            bot_info = await adapter_api.get_bot_info_by_platform(state.platform)
        except Exception:
            bot_info = None
        if bot_info:
            bot_name = str(bot_info.get("bot_name") or "")
            bot_id = str(bot_info.get("bot_id") or "")

    # ── 从 core.toml [personality] 取人设（核心人格 + 人格侧面 + 身份 + 背景）──
    persona_prompt = _build_persona_prompt()

    model_set = llm_api.get_model_set_by_task(config.daily_memory.task_name)
    request = llm_api.create_llm_request(
        model_set=model_set,
        request_name=f"cross_stream_relay_daily_memory_{state.stream_id[:8]}_{memory_date}",
    )

    bot_self_intro = "你"
    if bot_name and bot_id:
        bot_self_intro = f"你（{bot_name}，平台账号 {bot_id}）"
    elif bot_name:
        bot_self_intro = f"你（{bot_name}）"
    elif bot_id:
        bot_self_intro = f"你（账号 {bot_id}）"

    char_limit = max(200, int(config.daily_memory.max_summary_chars))

    is_incremental = existing_record is not None and not force_full

    if is_incremental:
        # 增量模式：把上次摘要 + 这一段新消息塞给 LLM，让它合并出新摘要
        system_prompt = (
            f"{bot_self_intro}是这个群聊的参与者之一。"
            "你之前已经为今天写过一份短期记忆，"
            "现在群里又出现了一段新消息，你需要把这段新消息融合到原有的回忆里，"
            "输出更新后的【完整一天】的短期记忆。\n"
            "\n"
            "硬性要求：\n"
            "1. 必须用第一人称（『我』），保持原有回忆的时序结构与人格语气，"
            "新消息按时间顺序自然衔接到原有内容里，不要打乱原本的结构。\n"
            "2. 提到任何人时，都必须给出他的【完整昵称】，并紧跟一个括号写出他的"
            "【平台账号 ID / QQ 号】，例如：阿喵（123456789）。提到自己时使用『我』。\n"
            "3. 必须保留旧摘要里的核心信息（已确认事实、未完成事项、关键冲突），"
            "除非新消息显式纠正或推翻了它们，此时你必须修正而不是保留旧错误。\n"
            "4. 新消息中的关键内容（话题转折、新承诺、新决议、新冲突、新待办）必须被纳入。\n"
            "5. 可以表达主观判断与感受，但不可以编造没有出现的内容。\n"
            "6. 时间脉络保持粗粒度（『上午』『中午前后』『下午三四点』『傍晚』『深夜』），"
            "不需要精确到分钟。\n"
            f"7. 输出字数【上限】是 {char_limit} 个字符（含标点），这是上限不是配额。"
            "如果信息不多就直接收尾，不要为了凑字数注水。"
            "如果合并后超出字数，请按重要性取舍：优先保留人物、决议、未完成事项、冲突；"
            "省略次要寒暄。\n"
            "\n"
            "输出形式：直接输出回忆正文，可以分段；不要标题，不要『以下是总结』之类的前言，"
            "不要 JSON 或 Markdown 标题语法，不要逐条复述聊天记录。"
            "不要在文中说『新增了什么』或『增量部分』，要让最终结果看起来就是一份完整的、自然书写的当日回忆。"
        )
    else:
        # 全量模式：从零开始
        system_prompt = (
            f"{bot_self_intro}是这个群聊的参与者之一。"
            "现在你需要为「自己」整理一份当天发生在这个群里的主观短期记忆，"
            "就像一个人晚上躺下来回忆白天发生过什么那样去写。\n"
            "\n"
            "硬性要求：\n"
            "1. 必须用第一人称（『我』），写出你自己的视角和感受、判断、未做完的事，"
            "不要切换成第三人称、不要写成新闻播报或会议纪要。\n"
            "2. 提到任何人时，都必须给出他的【完整昵称】，并紧跟一个括号写出他的"
            "【平台账号 ID / QQ 号】，例如：阿喵（123456789）。"
            "提到自己时使用『我』，无须再写自己的 ID。\n"
            "3. 写作必须有时间脉络：按发生先后顺序展开，使用粗略但可识别的时段描述，"
            "比如『今天上午刚醒来的时候』『中午前后』『下午三四点』『傍晚』『深夜十二点之后』。"
            "不需要也不要精确到分钟或秒，但不能完全打乱时序。\n"
            "4. 必须涵盖：当天主要话题与转折、关键人物的发言/态度/情绪、"
            "我自己说过的话/承诺/暂时没回的人、群里达成的事实或决议、"
            "悬而未决的问题或下次需要继续的事项。\n"
            "5. 可以表达主观判断与感受（『我觉得』『我担心』『我没太听懂』等），"
            "但不可以编造没有出现的内容，不可以隐藏关键冲突。\n"
            "6. 如果当天某些讨论可能对其他群也有补全价值（例如人物关系、专有名词、"
            "事件背景），请显式写明，便于将来跨群查阅。\n"
            f"7. 输出字数【上限】是 {char_limit} 个字符（含标点），这是上限不是配额。"
            "如果当天信息不多，能讲清楚事情就直接收尾，不要为了凑字数而注水、复述或反复展开。"
            "只有当一天信息确实多到塞不下时，才需要按重要性取舍：优先保留人物、决议、未完成事项、冲突；省略次要寒暄。\n"
            "\n"
            "输出形式：直接输出回忆正文，可以分段；不要标题，不要『以下是总结』之类的前言，"
            "不要 JSON 或 Markdown 标题语法，不要逐条复述聊天记录。"
        )

    if persona_prompt:
        system_prompt = persona_prompt + "\n\n---\n\n" + system_prompt

    request.add_payload(LLMPayload(ROLE.SYSTEM, Text(system_prompt)))

    if is_incremental and existing_record is not None:
        user_text = (
            f"【群聊】{state.group_name or '未知'}（群号 {state.group_id or '未知'}）\n"
            f"【日期】{memory_date}\n"
            f"【已总结消息数】{existing_record.message_count}\n"
            f"【本次新增消息数】{len(raw_messages)}\n\n"
            "【上一次写出的当日短期记忆（原文）】\n"
            f"{existing_record.summary}\n\n"
            "【本次新增的群聊消息（按时间顺序，仅这一段是新内容）】\n"
            f"{formatted}\n\n"
            "请把上面这段新消息自然融合到原有回忆里，输出更新后的【完整一天】的短期记忆。"
        )
    else:
        user_text = (
            f"【群聊】{state.group_name or '未知'}（群号 {state.group_id or '未知'}）\n"
            f"【日期】{memory_date}\n"
            f"【消息总数】{len(raw_messages)}\n\n"
            "下面是当天群里发生的全部聊天记录，按时间顺序：\n\n"
            "【当日完整聊天记录】\n"
            f"{formatted}\n\n"
            "请你以第一人称、按时序，写出今天在这个群里你自己的主观短期记忆。"
        )

    request.add_payload(LLMPayload(ROLE.USER, Text(user_text)))

    response = await request.send(stream=False)
    await response
    summary_text = str(response.message or "").strip()
    if not summary_text:
        logger.warning(
            f"[daily_memory] stream={state.stream_id} date={memory_date} LLM 返回空，跳过"
        )
        return existing_record

    summary_text = _trim_text(summary_text, config.daily_memory.max_summary_chars)

    # 累积消息计数：增量模式下要叠加在旧计数之上
    cumulative_count = len(raw_messages)
    if is_incremental and existing_record is not None:
        cumulative_count += existing_record.message_count

    record = DailyMemoryRecord(
        stream_id=state.stream_id,
        group_id=state.group_id,
        group_name=state.group_name,
        platform=state.platform,
        chat_type=state.chat_type,
        memory_date=memory_date,
        summary=summary_text,
        message_count=cumulative_count,
        updated_at=datetime.now().isoformat(timespec="seconds"),
        last_summarized_ts=new_watermark,
    )
    await storage_api.save_json(
        plugin.plugin_name,
        _memory_key(state.stream_id, memory_date),
        asdict(record),
    )
    mode_label = "增量更新" if is_incremental else "全量总结"
    logger.info(
        f"[daily_memory] stream={state.stream_id} date={memory_date} {mode_label}完成，"
        f"本次新增 {len(raw_messages)} 条，累计 {cumulative_count} 条，{len(summary_text)} 字"
    )
    return record


async def _save_state(plugin: Any, state: DailyState) -> None:
    """持久化状态。"""

    await storage_api.save_json(plugin.plugin_name, _state_key(state.stream_id), asdict(state))


async def _load_state_of(plugin: Any, stream_id: str) -> DailyState | None:
    """读取指定 stream 的状态。"""

    return _load_state(await storage_api.load_json(plugin.plugin_name, _state_key(stream_id)))


def _is_group_allowed(config: CrossStreamRelayConfig, group_id: str) -> bool:
    """根据现有群名单判断是否允许参与短期记忆。"""

    from .privacy_filter import _check_list  # 复用同一份名单逻辑

    if not group_id:
        return True
    return _check_list(group_id, config.privacy.group_list_type, config.privacy.group_list)


async def register_bot_message(plugin: Any, message: Message) -> None:
    """处理一条 bot 发送的群消息：更新状态并按需触发总结。"""

    config = _get_config(plugin)
    if not config.daily_memory.enabled:
        return
    if message.chat_type != "group":
        return

    stream_id = str(message.stream_id or "").strip()
    if not stream_id:
        return

    group_id, group_name = _extract_group_meta(message)
    if not _is_group_allowed(config, group_id):
        return

    today = _today_str()

    async with _state_lock(stream_id):
        state = await _load_state_of(plugin, stream_id)
        if state is None:
            state = DailyState(
                stream_id=stream_id,
                group_id=group_id,
                group_name=group_name,
                platform=str(message.platform or ""),
                chat_type="group",
                current_date=today,
                round_count=0,
                last_summary_at=0.0,
                last_event_direction="",
            )
        else:
            # 元信息以最新为准，便于群名变化后下一次总结使用最新名
            if group_id:
                state.group_id = group_id
            if group_name:
                state.group_name = group_name
            if message.platform:
                state.platform = str(message.platform)

        # ── 跨天处理：先归档昨天，再开始今天的累计 ──
        if state.current_date and state.current_date != today:
            previous_date = state.current_date
            try:
                await _generate_full_day_summary(plugin, state, previous_date)
            except Exception as exc:
                logger.warning(
                    f"[daily_memory] 跨天归档失败 stream={stream_id} date={previous_date}: {exc}"
                )
            state.current_date = today
            state.round_count = 0
            state.last_summary_at = 0.0
            state.last_event_direction = ""

        # ── 计算"轮次"：仅当 inbound→outbound 切换时 +1 ──
        if state.last_event_direction != "outbound":
            state.round_count += 1
        state.last_event_direction = "outbound"

        rounds = state.round_count
        idle_seconds = time.time() - (state.last_summary_at or 0.0)
        idle_threshold = max(60, int(config.daily_memory.trigger_idle_seconds))
        rounds_threshold = max(1, int(config.daily_memory.trigger_rounds))

        # 首次启动且 last_summary_at == 0：仅按轮次触发，不触发"空闲"
        idle_trigger = state.last_summary_at > 0 and idle_seconds >= idle_threshold
        rounds_trigger = rounds >= rounds_threshold

        should_trigger = idle_trigger or rounds_trigger

        if should_trigger:
            try:
                await _generate_full_day_summary(plugin, state, today)
            except Exception as exc:
                logger.warning(
                    f"[daily_memory] 当日全量总结失败 stream={stream_id}: {exc}"
                )
            state.round_count = 0
            state.last_summary_at = time.time()

        await _save_state(plugin, state)


async def register_inbound_message(plugin: Any, message: Message) -> None:
    """收到一条 inbound 时仅更新方向标记，便于之后正确识别"轮次"。"""

    config = _get_config(plugin)
    if not config.daily_memory.enabled:
        return
    if message.chat_type != "group":
        return

    stream_id = str(message.stream_id or "").strip()
    if not stream_id:
        return

    group_id, group_name = _extract_group_meta(message)
    if not _is_group_allowed(config, group_id):
        return

    async with _state_lock(stream_id):
        state = await _load_state_of(plugin, stream_id)
        if state is None:
            state = DailyState(
                stream_id=stream_id,
                group_id=group_id,
                group_name=group_name,
                platform=str(message.platform or ""),
                chat_type="group",
                current_date=_today_str(),
                round_count=0,
                last_summary_at=0.0,
                last_event_direction="inbound",
            )
        else:
            if group_id:
                state.group_id = group_id
            if group_name:
                state.group_name = group_name
            if message.platform:
                state.platform = str(message.platform)
            state.last_event_direction = "inbound"
        await _save_state(plugin, state)


async def list_all_states(plugin: Any) -> list[DailyState]:
    """枚举所有已知的状态记录。"""

    keys = await storage_api.list_json(plugin.plugin_name)
    states: list[DailyState] = []
    for key in keys:
        if not key.startswith("daily_state_"):
            continue
        state = _load_state(await storage_api.load_json(plugin.plugin_name, key))
        if state is not None:
            states.append(state)
    return states


async def archive_yesterday_for_all(plugin: Any) -> None:
    """守护循环周期任务：扫描所有状态，处理跨天归档（适用于无新消息的群）。"""

    config = _get_config(plugin)
    if not config.daily_memory.enabled:
        return

    today = _today_str()
    states = await list_all_states(plugin)

    for state in states:
        if not state.current_date or state.current_date == today:
            continue
        if not _is_group_allowed(config, state.group_id):
            continue
        async with _state_lock(state.stream_id):
            # 重新读取，避免锁外被并发修改
            fresh = await _load_state_of(plugin, state.stream_id)
            if fresh is None or fresh.current_date == today:
                continue
            previous_date = fresh.current_date
            try:
                await _generate_full_day_summary(plugin, fresh, previous_date)
            except Exception as exc:
                logger.warning(
                    f"[daily_memory] 守护循环归档失败 stream={fresh.stream_id} "
                    f"date={previous_date}: {exc}"
                )
            fresh.current_date = today
            fresh.round_count = 0
            fresh.last_summary_at = 0.0
            fresh.last_event_direction = ""
            await _save_state(plugin, fresh)


async def run_archive_loop(plugin: Any, stop_event: asyncio.Event) -> None:
    """跨天归档守护循环。"""

    config = _get_config(plugin)
    interval = max(15, int(config.daily_memory.archive_check_interval_seconds))
    logger.info(f"[daily_memory] 跨天归档守护循环启动，间隔 {interval}s")

    while not stop_event.is_set():
        try:
            await archive_yesterday_for_all(plugin)
        except Exception as exc:
            logger.warning(f"[daily_memory] 守护循环异常：{exc}", exc_info=True)
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=interval)
        except asyncio.TimeoutError:
            continue
        except Exception:
            break
    logger.info("[daily_memory] 跨天归档守护循环已退出")


async def list_recent_memories(
    plugin: Any,
    stream_id: str,
    max_days: int,
) -> list[DailyMemoryRecord]:
    """读取某 stream 在最近 max_days 天内的短期记忆（含今天，按日期倒序）。"""

    if max_days <= 0:
        return []
    keys = await storage_api.list_json(plugin.plugin_name)
    prefix = f"daily_memory_{stream_id}_"

    today = date.today()
    earliest_allowed = today - timedelta(days=max_days - 1)

    records: list[DailyMemoryRecord] = []
    for key in keys:
        if not key.startswith(prefix):
            continue
        date_part = key[len(prefix):]
        try:
            d = datetime.strptime(date_part, "%Y-%m-%d").date()
        except ValueError:
            continue
        if d < earliest_allowed or d > today:
            continue
        record = _load_record(await storage_api.load_json(plugin.plugin_name, key))
        if record is not None:
            records.append(record)
    records.sort(key=lambda r: r.memory_date, reverse=True)
    return records


async def get_memory(
    plugin: Any,
    stream_id: str,
    memory_date: str,
) -> DailyMemoryRecord | None:
    """读取指定日期的短期记忆（不做天数限制，由调用方控制）。"""

    return _load_record(
        await storage_api.load_json(plugin.plugin_name, _memory_key(stream_id, memory_date))
    )


async def get_today_memory_for_stream(
    plugin: Any,
    stream_id: str,
) -> DailyMemoryRecord | None:
    """专用于 reminder 注入：读取本群今天的短期记忆。"""

    return await get_memory(plugin, stream_id, _today_str())


async def force_generate_today(
    plugin: Any,
    stream_id: str,
) -> DailyMemoryRecord | None:
    """手动触发：立即对指定 stream 全量重做今天的短期记忆并覆盖。

    与日常增量更新不同，此方法会忽略水位线，从当日 0 点重新读取所有消息。
    返回新生成的记录；若当日无消息或生成失败则返回 None。
    """

    config = _get_config(plugin)
    if not config.daily_memory.enabled:
        return None

    today = _today_str()
    async with _state_lock(stream_id):
        state = await _load_state_of(plugin, stream_id)
        if state is None:
            logger.info(
                f"[daily_memory] force_generate_today：stream={stream_id} 尚无状态记录，跳过"
            )
            return None
        if not _is_group_allowed(config, state.group_id):
            logger.info(
                f"[daily_memory] force_generate_today：群 {state.group_id} 不在配置允许范围内"
            )
            return None
        if state.chat_type and state.chat_type != "group":
            return None

        record = await _generate_full_day_summary(plugin, state, today, force_full=True)
        if record is not None:
            state.last_summary_at = time.time()
            state.round_count = 0
            state.current_date = today
            await _save_state(plugin, state)
        return record
