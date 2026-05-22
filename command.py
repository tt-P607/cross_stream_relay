"""cross_stream_relay 插件命令组件。

包含两个 Command：

1. ShortMemoryCommand（接自原 context_bridge.command）
   ``/短期记忆`` 强制立即生成本群当日短期记忆。

2. RelayCommand（新增）
   ``/relay`` 调试用命令族：
     - /relay 状态     列出最近的转告日志
     - /relay 测试 <target_identifier> <relay_content>
                       手工触发一次跨流转告
     - /relay 清理     清空转告日志
"""

from __future__ import annotations

from src.app.plugin_system.api.log_api import get_logger
from src.app.plugin_system.api.send_api import send_text
from src.app.plugin_system.base import BaseCommand, cmd_route
from src.app.plugin_system.types import PermissionLevel

from .daily_memory import force_generate_today

logger = get_logger("cross_stream_relay.command")


class ShortMemoryCommand(BaseCommand):
    """手动强制生成当前群的当日短期记忆。

    支持的触发词：
        /短期记忆           — 默认动作 = 立即重新生成本群当日短期记忆
        /短期记忆 now       — 等价别名
        /短期记忆 立即       — 中文别名
        /short_memory       — 英文别名
        /short_memory now   — 英文别名

    仅主人（OWNER）可用。
    """

    command_name: str = "短期记忆"
    command_description: str = "立即重新生成本群当日短期记忆（仅主人可用）"
    permission_level: PermissionLevel = PermissionLevel.OWNER

    @classmethod
    def match(cls, parts: list[str]) -> int:
        """同时匹配中文 / 英文触发词。

        Args:
            parts: 命令片段列表。

        Returns:
            匹配长度，不匹配返回 0。
        """

        if not parts:
            return 0
        if parts[0] in ("短期记忆", "short_memory"):
            return 1
        return 0

    async def _reply(self, text: str) -> None:
        """向当前聊天流发送文本回复。"""

        await send_text(text, stream_id=self.stream_id)

    def _current_chat_type(self) -> str:
        """获取当前聊天类型。"""

        if self._message is None:
            return ""
        chat_type = self._message.extra.get("chat_type") if isinstance(self._message.extra, dict) else None
        return str(chat_type or "")

    async def _do_force_generate(self) -> tuple[bool, str]:
        """共享的执行逻辑：仅在群聊中触发，调用 force_generate_today 并回报结果。"""

        chat_type = self._current_chat_type()
        if chat_type and chat_type != "group":
            await self._reply("短期记忆功能仅在群聊中可用。")
            return False, "not group"

        record = await force_generate_today(self.plugin, self.stream_id)
        if record is None:
            await self._reply("✗ 无法生成短期记忆：当日无消息、群被排除或未启用此功能。")
            return False, "no record"

        preview = record.summary
        if len(preview) > 120:
            preview = preview[:117] + "..."
        await self._reply(
            f"✓ 已为本群（{record.group_name or record.group_id}）重新生成 {record.memory_date} 的短期记忆。\n"
            f"消息总数：{record.message_count}\n"
            f"摘要预览：{preview}"
        )
        logger.info(
            f"[command] short_memory 强制生成完成 stream={self.stream_id} date={record.memory_date}"
        )
        return True, "ok"

    @cmd_route()
    async def handle_default(self) -> tuple[bool, str]:
        """默认动作：立即生成本群当日短期记忆。"""

        return await self._do_force_generate()

    @cmd_route("now")
    async def handle_now(self) -> tuple[bool, str]:
        """显式触发立即生成。"""

        return await self._do_force_generate()

    @cmd_route("立即")
    async def handle_now_zh(self) -> tuple[bool, str]:
        """中文别名 立即。"""

        return await self._do_force_generate()


class RelayCommand(BaseCommand):
    """跨流转告调试命令族。

    支持子路由：
        /relay 状态 / /relay status   — 列出最近的转告日志
        /relay 清理 / /relay clear    — 清空转告日志（仅主人可用）

    仅主人（OWNER）可用。
    """

    command_name: str = "relay"
    command_description: str = "跨流转告调试与运维命令（仅主人可用）"
    permission_level: PermissionLevel = PermissionLevel.OWNER

    async def _reply(self, text: str) -> None:
        """向当前聊天流发送文本回复。"""

        await send_text(text, stream_id=self.stream_id)

    @cmd_route()
    async def handle_default(self) -> tuple[bool, str]:
        """默认动作：打印帮助。"""

        await self._reply(
            "/relay 调试命令：\n"
            "  /relay 状态  — 显示最近转告记录（暂未持久化日志，先打日志查看终端）\n"
            "  /relay 清理  — 重置 reminder 缓存\n"
            "（这是调试入口，正式跨流转告请由 LLM 通过 relay_to_stream Action 触发）"
        )
        return True, "ok"

    @cmd_route("状态")
    async def handle_status_zh(self) -> tuple[bool, str]:
        """中文别名 状态。"""

        return await self._handle_status()

    @cmd_route("status")
    async def handle_status_en(self) -> tuple[bool, str]:
        """英文别名 status。"""

        return await self._handle_status()

    async def _handle_status(self) -> tuple[bool, str]:
        """打印当前转告日志状态。"""

        # 当前阶段尚未实现 RelayLogStore（一次性同步动作不强制需要）。
        # 这里给出操作运维线索。
        await self._reply(
            "尚未启用持久化转告日志。要查看最近转告，请检查终端日志中"
            "「[relay] 转告完成 origin=...→target=...」与「[relay_decorator] 注入跨流转告 reminder...」的输出。"
        )
        return True, "ok"

    @cmd_route("清理")
    async def handle_clear_zh(self) -> tuple[bool, str]:
        """中文别名 清理。"""

        return await self._handle_clear()

    @cmd_route("clear")
    async def handle_clear_en(self) -> tuple[bool, str]:
        """英文别名 clear。"""

        return await self._handle_clear()

    async def _handle_clear(self) -> tuple[bool, str]:
        """清理 reminder 缓存（占位，未来扩展为清理日志）。"""

        await self._reply(
            "当前阶段未提供持久化日志，无需清理。"
            "如果想强制刷新跨流摘要 reminder，可在群里发任意一条消息触发自动同步。"
        )
        return True, "ok"
