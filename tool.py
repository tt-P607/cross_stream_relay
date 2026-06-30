"""cross_stream_relay 插件 Tool 组件。

包含 3 个 Tool：

1. GetStreamRawContextTool：查询任意聊天流的原始聊天记录，
   支持时间段 + 关键词 + 发送者三条件任意组合查询。
2. GetDailyMemoryTool（接自原 context_bridge.get_context_bridge_daily_memory）
3. FindTargetStreamTool：把名字/索引/QQ 号/群号反查成完整目标流元组，
   供 RelayToStreamAction 使用。

已废弃：GetStreamSummaryTool（功能由 SystemReminder 注入的跨流摘要 + find_target_stream 完整覆盖）
"""

from __future__ import annotations

from datetime import date, datetime, timedelta
from typing import Annotated

from src.app.plugin_system.api import message_api, stream_api
from src.app.plugin_system.api.log_api import get_logger
from src.app.plugin_system.base import BaseTool

from .config import CrossStreamRelayConfig
from .daily_memory import (
    DailyMemoryRecord,
    get_memory,
    list_recent_memories,
)
from .service import StreamSummaryRecord, _build_stream_title, list_summary_records

logger = get_logger("cross_stream_relay.tool")


class GetStreamRawContextTool(BaseTool):
    """查询任意聊天流的原始聊天记录，支持时间段+关键词+发送者组合查询。"""

    tool_name = "get_stream_raw_context"
    tool_description = (
        "查询任意聊天流的真实原始聊天记录（从数据库读取，不是摘要）。\n"
        "支持时间段、关键词、发送者三条件任意组合筛选。\n"
        "\n"
        "stream_identifier 用群号/QQ号/流名称指定目标流。\n"
        "时间格式 'YYYY-MM-DD HH:MM'。\n"
        "\n"
        "典型组合：\n"
        "- 无筛选：返回最近 message_count 条\n"
        "- keyword：筛选含关键词的消息\n"
        "- sender：只看某个人的发言\n"
        "- start_time + end_time：查指定时段\n"
        "- 三条件全传：查某时段内某人说的含关键词的话"
    )

    chatter_allow: list[str] = []

    async def execute(
        self,
        stream_identifier: Annotated[
            str,
            "目标聊天流标识符：群号、QQ号或流名称（模糊匹配）",
        ],
        message_count: Annotated[
            int,
            "最终返回消息上限，默认 20 条，可按需调大",
        ] = 20,
        start_time: Annotated[
            str,
            "可选筛选：时间段起点，格式 'YYYY-MM-DD HH:MM'，留空表示不限",
        ] = "",
        end_time: Annotated[
            str,
            "可选筛选：时间段终点，格式同上，留空表示到现在",
        ] = "",
        keyword: Annotated[
            str,
            "可选筛选：关键词（不区分大小写），留空表示不过滤",
        ] = "",
        sender: Annotated[
            str,
            "可选筛选：发送者，匹配用户名或QQ号（模糊匹配），留空表示不过滤",
        ] = "",
    ) -> tuple[bool, str]:
        """获取指定聊天流的原始聊天记录，支持多条件组合查询。"""

        try:
            if message_count < 1:
                return False, "消息数量必须大于 0"
            if message_count > 100:
                return False, "单次最多获取 100 条消息"

            # 解析目标流
            target_record = await self._resolve_stream(stream_identifier)
            if isinstance(target_record, str):
                return False, target_record

            # 解析时间条件
            start_ts = self._parse_time(start_time.strip()) if start_time.strip() else None
            end_ts = self._parse_time(end_time.strip()) if end_time.strip() else None
            if start_ts is not None and end_ts is not None and start_ts >= end_ts:
                return False, "起始时间必须早于结束时间"

            kw = keyword.strip().lower() if keyword.strip() else ""
            sd = sender.strip().lower() if sender.strip() else ""

            has_time_filter = start_ts is not None or end_ts is not None
            has_content_filter = bool(kw) or bool(sd)

            # 查询策略：
            # - 有时间范围 → DB 时间范围查询，limit 放大以补偿内存侧过滤
            # - 无时间范围 → get_recent_messages 取最近消息
            fetch_limit = message_count * 5 if has_content_filter else message_count

            if has_time_filter:
                # 用 get_messages_by_time_in_chat 做时间范围查询
                # end_time 为空时用当前时间
                from time import time as _now
                actual_end = end_ts if end_ts is not None else _now()
                messages = await message_api.get_messages_by_time_in_chat(
                    stream_id=target_record.stream_id,
                    start_time=start_ts if start_ts is not None else 0.0,
                    end_time=actual_end,
                    limit=fetch_limit,
                    limit_mode="latest",
                    filter_bot=False,
                )
            else:
                # 无时间范围，取最近消息
                messages = await message_api.get_recent_messages(
                    stream_id=target_record.stream_id,
                    hours=24 * 365,
                    limit=fetch_limit,
                    limit_mode="latest",
                    filter_bot=False,
                )

            # 内存侧过滤：关键词 + 发送者
            if has_content_filter:
                messages = self._filter_messages(messages, kw, sd)

            # 截断到最终上限
            if len(messages) > message_count:
                messages = messages[-message_count:]

            return True, await self._format_result(
                target_record,
                messages,
                start_time=start_time.strip(),
                end_time=end_time.strip(),
                keyword=keyword.strip(),
                sender=sender.strip(),
                requested_limit=message_count,
            )

        except ValueError as error:
            return False, str(error)
        except Exception as error:
            logger.error(f"get_stream_raw_context 查询失败: {error}")
            return False, f"获取原始上下文失败: {error}"

    async def _resolve_stream(
        self,
        identifier: str,
    ) -> StreamSummaryRecord | str:
        """解析聊天流标识符，返回记录或错误信息。

        纯数字 → 按群号/QQ号匹配 target_id
        字符串 → 模糊匹配流名称
        """

        ident = identifier.strip()
        if not ident:
            return "请提供有效的聊天流标识符"

        records = await list_summary_records(self.plugin)
        if not records:
            return "当前没有任何聊天流记录"

        # 纯数字 → 按 target_id（群号/QQ号）精确匹配
        if ident.isdigit():
            for r in records:
                if r.target_id and str(r.target_id) == ident:
                    return r
            return f"未找到群号/QQ号为 '{ident}' 的聊天流"

        # 字符串 → 模糊匹配流名称
        matched = [
            r for r in records
            if ident.lower() in _build_stream_title(r).lower()
        ]
        if len(matched) == 1:
            return matched[0]
        if len(matched) == 0:
            return f"未找到匹配 '{identifier}' 的聊天流"

        # 多匹配：列出候选供精确化
        lines = [f"找到 {len(matched)} 个匹配的聊天流，请用更精确的群号/QQ号或名称："]
        for idx, r in enumerate(matched[:8], start=1):
            lines.append(
                f"{idx}. {_build_stream_title(r)} "
                f"[{r.platform}:{r.chat_type}] (ID {r.target_id or '未知'})"
            )
        if len(matched) > 8:
            lines.append(f"... 还有 {len(matched) - 8} 条未列出")
        return "\n".join(lines)

    @staticmethod
    def _parse_time(time_str: str) -> float:
        """解析 'YYYY-MM-DD HH:MM' 格式时间为时间戳。"""

        from datetime import datetime as _dt
        try:
            return _dt.strptime(time_str, "%Y-%m-%d %H:%M").timestamp()
        except ValueError:
            raise ValueError(
                f"时间格式不正确：'{time_str}'，应为 'YYYY-MM-DD HH:MM'"
            )

    @staticmethod
    def _filter_messages(
        messages: list[dict],
        keyword: str,
        sender: str,
    ) -> list[dict]:
        """在内存侧按关键词和发送者过滤消息。"""

        result = []
        for msg in messages:
            # 发送者匹配：检查 sender_name 和 sender_id
            if sender:
                msg_sender = str(msg.get("sender_name") or "").lower()
                msg_sender_id = str(msg.get("sender_id") or "").lower()
                if sender not in msg_sender and sender not in msg_sender_id:
                    continue
            # 关键词匹配：检查消息文本
            if keyword:
                text = str(
                    msg.get("processed_plain_text")
                    or msg.get("content")
                    or ""
                ).lower()
                if keyword not in text:
                    continue
            result.append(msg)
        return result

    @staticmethod
    async def _format_result(
        target: StreamSummaryRecord,
        messages: list[dict],
        *,
        start_time: str,
        end_time: str,
        keyword: str,
        sender: str,
        requested_limit: int,
    ) -> str:
        """格式化查询结果输出。"""

        from datetime import datetime as _dt

        # 构建查询条件描述
        conditions = []
        if start_time:
            conditions.append(f"起 {start_time}")
        if end_time:
            conditions.append(f"止 {end_time}")
        if keyword:
            conditions.append(f"关键词'{keyword}'")
        if sender:
            conditions.append(f"发送者含'{sender}'")
        cond_desc = " | ".join(conditions) if conditions else "无筛选（取最近）"

        lines = [
            f"聊天流: {_build_stream_title(target)}",
            f"平台/类型: {target.platform or 'unknown'} / {target.chat_type or 'unknown'}",
            f"群号/QQ: {target.target_id or '未知'}",
            f"查询条件: {cond_desc}",
            f"返回: {len(messages)} 条 (上限 {requested_limit})",
        ]

        if messages:
            # 计算实际时间范围
            times = [float(m.get("time") or 0.0) for m in messages if m.get("time")]
            if times:
                earliest = _dt.fromtimestamp(min(times)).strftime("%Y-%m-%d %H:%M")
                latest = _dt.fromtimestamp(max(times)).strftime("%Y-%m-%d %H:%M")
                lines.append(f"时间跨度: {earliest} ~ {latest}")

            # 参与者统计
            from collections import Counter
            senders = Counter(
                str(m.get("sender_name") or m.get("sender_id") or "未知")
                for m in messages
            )
            top_senders = ", ".join(
                f"{name}({count}条)" for name, count in senders.most_common(5)
            )
            lines.append(f"参与者: {top_senders}")

            lines.extend(["", "── 消息记录 ──", ""])

            formatted_text = await message_api.build_readable_messages_to_str(
                messages=messages,
                replace_bot_name=False,
                merge_messages=False,
                timestamp_mode="absolute",
                truncate=True,
            )
            lines.append(formatted_text)
        else:
            lines.extend(["", "该条件下没有匹配的消息"])

        return "\n".join(lines)


class GetDailyMemoryTool(BaseTool):
    """跨群查询某群最近若干天的"短期记忆"（每日全量总结）。"""

    tool_name = "get_daily_memory"
    tool_description = (
        "查询某个群的「短期记忆」。短期记忆是 actor 模型对该群当天全部聊天的全量总结，"
        "比摘要更详细，用于在摘要信息不足时补全细节。"
        "支持跨群查询：你可以在 A 群中查询 B 群的短期记忆，用以理解你之前在 B 群的语境。\n"
        "限制：只能查最近若干天（含今天），更早的记忆已归档但工具不可见；仅群聊有短期记忆，"
        "私聊不会生成。"
    )

    chatter_allow: list[str] = []

    async def execute(
        self,
        stream_identifier: Annotated[
            str,
            "目标群标识符：群号或群名（模糊匹配）。仅支持群聊。",
        ],
        days: Annotated[
            int,
            "查询的最近天数（含今天）。默认 1（仅今天）。最大值受插件配置 daily_memory.max_query_days 限制。",
        ] = 1,
        date_filter: Annotated[
            str,
            "可选：指定具体日期（格式 YYYY-MM-DD），优先级高于 days。仅返回该日的记忆。",
        ] = "",
    ) -> tuple[bool, str]:
        """跨群查询短期记忆。"""

        try:
            config = self.plugin.config
            if not isinstance(config, CrossStreamRelayConfig):
                return False, "插件配置异常"
            if not config.daily_memory.enabled:
                return False, "短期记忆功能已在配置中禁用"

            max_query_days = max(1, int(config.daily_memory.max_query_days))

            target_record = await self._resolve_target(stream_identifier)
            if isinstance(target_record, str):
                return False, target_record
            if target_record.chat_type != "group":
                return False, (
                    f"短期记忆仅支持群聊，"
                    f"目标 [{target_record.chat_type}] {_build_stream_title(target_record)} 不是群聊"
                )

            if date_filter.strip():
                requested_date = date_filter.strip()
                try:
                    parsed = datetime.strptime(requested_date, "%Y-%m-%d").date()
                except ValueError:
                    return False, f"日期格式不正确：{requested_date}（应为 YYYY-MM-DD）"
                today = date.today()
                earliest = today - timedelta(days=max_query_days - 1)
                if parsed > today or parsed < earliest:
                    return False, (
                        f"该日期 {requested_date} 不在可查范围内（{earliest.isoformat()} ~ {today.isoformat()}）"
                    )
                record = await get_memory(self.plugin, target_record.stream_id, requested_date)
                if record is None:
                    return False, f"{_build_stream_title(target_record)} 在 {requested_date} 没有短期记忆"
                return True, self._format_record(target_record, [record])

            requested_days = max(1, min(int(days), max_query_days))
            records = await list_recent_memories(
                self.plugin,
                target_record.stream_id,
                requested_days,
            )
            if not records:
                return False, (
                    f"{_build_stream_title(target_record)} 最近 {requested_days} 天没有短期记忆"
                )
            return True, self._format_record(target_record, records)

        except Exception as error:
            return False, f"获取短期记忆失败: {error}"

    async def _resolve_target(
        self,
        identifier: str,
    ) -> StreamSummaryRecord | str:
        """根据标识符在已知聊天流中定位目标记录。返回字符串表示错误信息。

        纯数字 → 按群号匹配 target_id
        字符串 → 模糊匹配群名
        """

        ident = identifier.strip()
        if not ident:
            return "请提供有效的群标识符"

        records = await list_summary_records(self.plugin)
        if not records:
            return "目前没有任何聊天流记录可供解析"

        # 纯数字 → 按 target_id（群号）精确匹配
        if ident.isdigit():
            for r in records:
                if r.chat_type == "group" and r.target_id and str(r.target_id) == ident:
                    return r
            return f"未找到群号为 '{ident}' 的群聊"

        # 字符串 → 模糊匹配群名
        matched = [
            r for r in records
            if r.chat_type == "group" and ident.lower() in _build_stream_title(r).lower()
        ]
        if len(matched) == 1:
            return matched[0]
        if len(matched) > 1:
            preview_lines = [f"找到 {len(matched)} 个匹配的群，请用更精确的群号或名称："]
            for idx, r in enumerate(matched[:6], start=1):
                preview_lines.append(
                    f"{idx}. {_build_stream_title(r)} (群号 {r.target_id or '未知'})"
                )
            return "\n".join(preview_lines)

        return f"未找到匹配 '{identifier}' 的群聊"

    def _format_record(
        self,
        target: StreamSummaryRecord,
        records: list[DailyMemoryRecord],
    ) -> str:
        """格式化输出。"""

        lines = [
            f"群聊: {_build_stream_title(target)}",
            f"群号: {target.target_id or '未知'}",
            f"平台: {target.platform or 'unknown'}",
            "",
        ]
        for record in records:
            lines.extend([
                f"── {record.memory_date} ──",
                f"消息总数: {record.message_count}    更新于: {record.updated_at}",
                "",
                record.summary,
                "",
            ])
        return "\n".join(lines).rstrip()


class FindTargetStreamTool(BaseTool):
    """跨流转告前置工具：把名字/索引/QQ 号/群号反查成完整目标流元组。

    返回结果中包含：
      - stream_id：32 位 hex
      - stream_name：群名 / "xxx的私聊"
      - platform / chat_type
      - target_id：群号或对方 QQ 号
      - is_in_memory：是否已加载到内存（False 表示是冷态流，relay 时会冷启动）
      - summary：当前摘要片段（可选，方便 LLM 二次决策是否要交接）

    标识符语义：
      - 数字（且 <=4 位）优先按"摘要列表索引号"匹配
      - 数字（且 5 位以上）优先按 target_id（群号 / QQ 号）匹配
      - 字符串按 stream_name 模糊匹配
      - 64 位 hex 字符串视为已知 stream_id，直接通过

    LLM 拿到这个元组后，应当再调用 ``relay_to_stream`` Action 实施转告。
    """

    tool_name = "find_target_stream"
    tool_description = (
        "跨流转告的前置查找工具。用于将用户名、群名、QQ号、群号或索引号反查为跨流转告所需的完整元组（stream_id, target_id 等）。"
        "当你决定要进行意识迁移但不知道目标流的确切 ID 时使用此工具。"
        "找到目标后，你必须立即调用 relay_to_stream Action 来完成实际的跨流转告。"
    )

    chatter_allow: list[str] = []

    async def execute(
        self,
        identifier: Annotated[
            str,
            "目标流标识符：可以是聊天流名（模糊匹配）、索引号（短数字）、QQ 号 / 群号（长数字）、"
            "或已知的完整 stream_id（64 位 hex）。",
        ],
        chat_type_hint: Annotated[
            str,
            "可选消歧提示：传 'private' / 'group' 帮助在多结果时优先选择对应类型。留空表示不限。",
        ] = "",
    ) -> tuple[bool, str]:
        """反查目标流，返回结构化字符串。"""

        ident = identifier.strip()
        if not ident:
            return False, "请提供有效的目标流标识符"

        normalized_hint = chat_type_hint.strip().lower() if chat_type_hint else ""
        if normalized_hint and normalized_hint not in ("private", "group"):
            normalized_hint = ""

        try:
            records = await list_summary_records(self.plugin)
        except Exception as error:
            return False, f"读取聊天流索引失败: {error}"

        # 优先：完整 stream_id（64 位 hex）
        if len(ident) == 64 and all(c in "0123456789abcdef" for c in ident.lower()):
            return await self._build_result_from_stream_id(ident, records)

        # 索引号
        if ident.isdigit() and 1 <= len(ident) <= 4:
            idx = int(ident) - 1
            candidates = self._filter_by_hint(records, normalized_hint)
            if 0 <= idx < len(candidates):
                return self._format_result(candidates[idx], in_memory_hint=None)
            # 不在索引范围内，继续作为 target_id 处理（fallthrough）

        # target_id（群号 / QQ 号）：匹配字符串完全相等的 target_id
        if ident.isdigit():
            for r in records:
                if normalized_hint and r.chat_type != normalized_hint:
                    continue
                if r.target_id and str(r.target_id) == ident:
                    return self._format_result(r, in_memory_hint=None)
            # 数字但 target_id 没命中：可能是用户从未交互过的私聊对象
            # 提示 LLM 调用 Action 时显式提供 target_platform / target_user_id
            return self._format_result_unknown(
                identifier=ident,
                hint=normalized_hint,
                reason="数字标识符未在已知聊天流中命中。这可能是 bot 从未联系过的对象。",
            )

        # 名字模糊匹配
        candidates = self._filter_by_hint(records, normalized_hint)
        matched = [
            r for r in candidates
            if ident.lower() in _build_stream_title(r).lower()
        ]
        if len(matched) == 1:
            return self._format_result(matched[0], in_memory_hint=None)
        if len(matched) == 0:
            return False, (
                f"未找到匹配 '{identifier}' 的聊天流。如果对方从未与 bot 交互过，"
                "请直接把对方 QQ 号、目标平台等信息作为 relay_to_stream 的 target_user_id / target_platform 参数显式传入。"
            )

        preview_lines = [f"找到 {len(matched)} 个匹配的聊天流，请进一步精确："]
        for idx, r in enumerate(matched[:8], start=1):
            preview_lines.append(
                f"{idx}. {_build_stream_title(r)} "
                f"[{r.platform}:{r.chat_type}] (id {r.target_id or '未知'})"
            )
        if len(matched) > 8:
            preview_lines.append(f"... 还有 {len(matched) - 8} 条未列出")
        return False, "\n".join(preview_lines)

    @staticmethod
    def _filter_by_hint(
        records: list[StreamSummaryRecord],
        chat_type_hint: str,
    ) -> list[StreamSummaryRecord]:
        """按 chat_type_hint 过滤（空 hint 则不过滤）。"""

        if not chat_type_hint:
            return records
        return [r for r in records if r.chat_type == chat_type_hint]

    async def _build_result_from_stream_id(
        self,
        stream_id: str,
        records: list[StreamSummaryRecord],
    ) -> tuple[bool, str]:
        """根据完整 stream_id 直接生成结果（即使没在索引中也能给出框架内信息）。"""

        for r in records:
            if r.stream_id == stream_id:
                return self._format_result(r, in_memory_hint=None)

        # 尝试从 StreamManager 的 stream_info 拿元信息
        try:
            info = await stream_api.get_stream_info(stream_id)
        except Exception:
            info = None

        if info is None:
            return self._format_result_unknown(
                identifier=stream_id,
                hint="",
                reason="该 stream_id 在摘要索引和 StreamManager 中均无记录，可能是从未交互的目标。",
            )

        return self._format_known_stream_info(stream_id, info)

    def _format_known_stream_info(self, stream_id: str, info: dict) -> tuple[bool, str]:
        """把 StreamManager.get_stream_info 的输出格式化为统一结果。"""

        chat_type = str(info.get("chat_type") or "")
        platform = str(info.get("platform") or "")
        group_id = str(info.get("group_id") or "")
        # 私聊 person_id 不直接是 user_id，建议 LLM 显式传 target_user_id

        lines = [
            "找到目标流（来自 StreamManager 但暂无摘要）：",
            f"stream_id: {stream_id}",
            f"stream_name: {info.get('group_name') or '(未知)'}",
            f"platform: {platform or 'unknown'}",
            f"chat_type: {chat_type or 'unknown'}",
            f"target_id: {group_id if chat_type == 'group' and group_id else '(私聊对象需调用 Action 时显式传 target_user_id)'}",
            "is_in_memory: False（流未在内存中，relay 时会冷启动）",
            "summary: (暂无摘要)",
        ]
        return True, "\n".join(lines)

    def _format_result(
        self,
        record: StreamSummaryRecord,
        in_memory_hint: bool | None,
    ) -> tuple[bool, str]:
        """把 StreamSummaryRecord 格式化为统一结果。"""

        is_in_memory = in_memory_hint
        if is_in_memory is None:
            try:
                is_in_memory = record.stream_id in stream_api.get_all_stream_ids()
            except Exception:
                is_in_memory = False

        lines = [
            "CRITICAL: 找到目标流！你必须立即执行下一步：调用 relay_to_stream Action 完成意识迁移。",
            "",
            "【下一步调用参数建议】",
            f"- target_stream_id: \"{record.stream_id}\"",
            f"- target_platform: \"{record.platform or 'qq'}\"",
            f"- target_user_id: \"{record.target_id if record.chat_type == 'private' else ''}\"",
            f"- target_group_id: \"{record.target_id if record.chat_type == 'group' else ''}\"",
            "",
            "【目标流详情】",
            f"名称: {_build_stream_title(record)}",
            f"platform: {record.platform or 'unknown'}",
            f"chat_type: {record.chat_type or 'unknown'}",
            f"is_in_memory: {bool(is_in_memory)}（{'热态' if is_in_memory else '冷态，会自动启动'}）",
            f"updated_at: {record.updated_at or 'unknown'}",
            "",
            "【当前摘要】",
            record.summary or "(暂无摘要)",
        ]
        return True, "\n".join(lines)

    def _format_result_unknown(
        self,
        identifier: str,
        hint: str,
        reason: str,
    ) -> tuple[bool, str]:
        """给"未在索引中命中"的目标提供建议输出。"""

        lines = [
            f"在已知聊天流索引中未找到与 '{identifier}' 匹配的记录。",
            f"原因：{reason}",
            "",
            "如果你确认对方存在于某个平台（例如 QQ 私聊），可以这样做：",
            "- 直接调用 relay_to_stream，并显式传入：",
            "    target_platform=（如 'qq'）",
            "    target_user_id=（对方 QQ 号）   # 私聊场景",
            "    target_group_id=（群号）        # 群聊场景",
            "  Action 会用 platform + user_id/group_id 自动生成 stream_id 并冷启动该流。",
            "如果你只是想找一个已经聊过的对象，请用更精确的名称重试 find_target_stream。",
        ]
        if hint:
            lines.append(f"（你提供的 chat_type_hint={hint}）")
        return False, "\n".join(lines)
