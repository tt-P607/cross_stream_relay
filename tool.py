"""cross_stream_relay 插件 Tool 组件。

包含 4 个 Tool：

1. GetStreamSummaryTool（接自原 context_bridge.get_context_bridge_summary）
2. GetStreamRawContextTool（接自原 context_bridge.get_context_bridge_raw_context）
3. GetDailyMemoryTool（接自原 context_bridge.get_context_bridge_daily_memory）
4. FindTargetStreamTool（新增）：把名字/索引/QQ 号/群号反查成完整目标流元组，
   供 RelayToStreamAction 使用。
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


class GetStreamSummaryTool(BaseTool):
    """获取指定聊天流的摘要内容。"""

    tool_name = "get_stream_summary"
    tool_description = (
        "获取一个或多个聊天流的摘要内容。"
        "你可以通过聊天流名称或索引号来查询摘要。"
        "摘要包含该聊天流的核心话题、背景上下文、已确认事实、用户偏好等关键信息。"
        "支持同时查询多个聊天流，用逗号分隔标识符即可。"
    )

    chatter_allow: list[str] = []

    async def execute(
        self,
        stream_identifiers: Annotated[
            str,
            "聊天流标识符，可以是单个或多个（用逗号分隔）。支持聊天流名称（如群名、私聊对方名称）或索引号（如 '1', '2' 或 '1,3,5'）",
        ],
    ) -> tuple[bool, str]:
        """获取指定聊天流的摘要。"""

        try:
            records = await list_summary_records(self.plugin)
            if not records:
                return False, "当前没有任何聊天流摘要记录"

            identifiers = [s.strip() for s in stream_identifiers.split(",") if s.strip()]
            if not identifiers:
                return False, "请提供有效的聊天流标识符"

            results: list[str] = []
            not_found: list[str] = []

            for identifier in identifiers:
                record = self._find_record(identifier, records)
                if record:
                    results.append(self._format_summary(record))
                else:
                    not_found.append(identifier)

            if not results and not_found:
                return False, f"未找到匹配的聊天流: {', '.join(not_found)}"

            output = "\n\n".join(results)
            if not_found:
                output += f"\n\n注意：以下标识符未找到匹配: {', '.join(not_found)}"

            return True, output

        except Exception as error:
            return False, f"获取摘要失败: {error}"

    def _find_record(self, identifier: str, records: list[StreamSummaryRecord]) -> StreamSummaryRecord | None:
        """查找单个聊天流记录。"""
        if identifier.isdigit():
            index = int(identifier) - 1
            if 0 <= index < len(records):
                return records[index]
            return None

        matched_records = [
            r for r in records
            if identifier.lower() in _build_stream_title(r).lower()
        ]

        if len(matched_records) == 1:
            return matched_records[0]

        return None

    def _format_summary(self, record: StreamSummaryRecord) -> str:
        """格式化摘要输出。"""
        lines = [
            f"聊天流: {_build_stream_title(record)}",
            f"平台: {record.platform or 'unknown'}",
            f"类型: {record.chat_type or 'unknown'}",
            f"更新时间: {record.updated_at or 'unknown'}",
            "",
            "摘要内容:",
            record.summary,
        ]
        return "\n".join(lines)


class GetStreamRawContextTool(BaseTool):
    """获取指定聊天流的完整原始上下文信息。"""

    tool_name = "get_stream_raw_context"
    tool_description = (
        "获取指定聊天流的完整原始聊天记录。"
        "这个工具返回的是从数据库中提取的真实聊天消息，而不是摘要。"
        "你可以指定需要获取的消息数量，默认获取最近 20 条。"
        "适用于需要查看详细对话内容的场景。"
    )

    chatter_allow: list[str] = []

    async def execute(
        self,
        stream_identifier: Annotated[
            str,
            "聊天流标识符，可以是聊天流名称（如群名、私聊对方名称）或索引号（如 '1', '2'）",
        ],
        message_count: Annotated[
            int,
            "需要获取的消息数量，默认 20 条。可以根据需要调整，比如 30、50 或更多",
        ] = 20,
    ) -> tuple[bool, str]:
        """获取指定聊天流的原始聊天记录。"""

        try:
            if message_count < 1:
                return False, "消息数量必须大于 0"
            if message_count > 200:
                return False, "单次最多获取 200 条消息，请分批查询"

            records = await list_summary_records(self.plugin)
            if not records:
                return False, "当前没有任何聊天流记录"

            target_record: StreamSummaryRecord | None = None

            if stream_identifier.isdigit():
                index = int(stream_identifier) - 1
                if 0 <= index < len(records):
                    target_record = records[index]
                else:
                    return False, f"索引号 {stream_identifier} 超出范围（共 {len(records)} 条记录）"
            else:
                matched_records = [
                    r for r in records
                    if stream_identifier.lower() in _build_stream_title(r).lower()
                ]

                if not matched_records:
                    return False, f"未找到匹配 '{stream_identifier}' 的聊天流"

                if len(matched_records) > 1:
                    result_lines = [f"找到 {len(matched_records)} 个匹配的聊天流："]
                    for idx, record in enumerate(matched_records[:5], start=1):
                        result_lines.append(
                            f"{idx}. {_build_stream_title(record)} "
                            f"[{record.platform}:{record.chat_type}]"
                        )
                    if len(matched_records) > 5:
                        result_lines.append(f"... 还有 {len(matched_records) - 5} 条")
                    result_lines.append("\n请使用更具体的名称或索引号查询")
                    return False, "\n".join(result_lines)

                target_record = matched_records[0]

            if target_record is None:
                return False, "未找到目标聊天流"

            messages = await message_api.get_recent_messages(
                stream_id=target_record.stream_id,
                hours=24 * 365,
                limit=message_count,
                limit_mode="latest",
                filter_bot=False,
            )

            result_lines = [
                f"聊天流: {_build_stream_title(target_record)}",
                f"平台: {target_record.platform or 'unknown'}",
                f"类型: {target_record.chat_type or 'unknown'}",
                f"Stream ID: {target_record.stream_id}",
                "",
                f"最近 {len(messages)} 条原始聊天记录:",
                "",
            ]

            if messages:
                formatted_text = await message_api.build_readable_messages_to_str(
                    messages=messages,
                    replace_bot_name=False,
                    merge_messages=False,
                    timestamp_mode="absolute",
                    truncate=False,
                )
                result_lines.append(formatted_text)
            else:
                result_lines.append("该聊天流暂无历史消息记录")

            result_lines.extend([
                "",
                "当前摘要:",
                target_record.summary,
            ])

            return True, "\n".join(result_lines)

        except Exception as error:
            return False, f"获取原始上下文失败: {error}"


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
            "目标群标识符：群名（模糊匹配）、索引号、或群号皆可。仅支持群聊。",
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
        """根据标识符在已知聊天流中定位目标记录。返回字符串表示错误信息。"""

        ident = identifier.strip()
        if not ident:
            return "请提供有效的群标识符"

        records = await list_summary_records(self.plugin)
        if not records:
            return "目前没有任何聊天流记录可供解析"

        if ident.isdigit() and len(ident) <= 4:
            idx = int(ident) - 1
            if 0 <= idx < len(records):
                return records[idx]

        for r in records:
            if r.chat_type == "group" and r.target_id and str(r.target_id) == ident:
                return r

        matched = [
            r for r in records
            if r.chat_type == "group" and ident.lower() in _build_stream_title(r).lower()
        ]
        if len(matched) == 1:
            return matched[0]
        if len(matched) > 1:
            preview_lines = [f"找到 {len(matched)} 个匹配的群，请用更精确的名称、群号或索引号："]
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
