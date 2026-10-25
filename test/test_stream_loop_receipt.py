"""跨流转告的流循环启动确认测试。"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from plugins.cross_stream_relay.action import _start_stream_loop


@pytest.mark.asyncio
async def test_start_stream_loop_starts_manager_and_confirms_stream(monkeypatch: pytest.MonkeyPatch) -> None:
    """管理器未运行时先启动，再按目标流的真实返回值报告结果。"""
    manager = SimpleNamespace(
        is_running=False,
        start=AsyncMock(),
        start_stream_loop=AsyncMock(return_value=True),
    )
    monkeypatch.setattr(
        "src.core.transport.distribution.stream_loop_manager.get_stream_loop_manager",
        lambda: manager,
    )

    assert await _start_stream_loop("target") is True
    manager.start.assert_awaited_once()
    manager.start_stream_loop.assert_awaited_once_with("target")


@pytest.mark.asyncio
async def test_start_stream_loop_propagates_start_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    """管理器启动失败不能被误报为目标流已运行。"""
    manager = SimpleNamespace(
        is_running=True,
        start=AsyncMock(),
        start_stream_loop=AsyncMock(return_value=False),
    )
    monkeypatch.setattr(
        "src.core.transport.distribution.stream_loop_manager.get_stream_loop_manager",
        lambda: manager,
    )

    assert await _start_stream_loop("target") is False
    manager.start.assert_not_awaited()
    manager.start_stream_loop.assert_awaited_once_with("target")