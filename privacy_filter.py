"""cross_stream_relay 隐私过滤逻辑。

整合自原 context_bridge/privacy_filter.py，配置类改为 CrossStreamRelayConfig。

根据黑白名单配置和私聊互通模式，决定：
1. 消息是否应被收集进摘要（collect 阶段过滤）
2. 某条聊天流摘要是否应注入到当前上下文的 actor reminder（render 阶段过滤）

私聊互通模式（private_bridge_mode）：
  - off      : 私聊流完全不参与跨流摘要，既不收集也不注入
  - one_way  : 私聊流的消息被收集、摘要正常更新，但摘要仅在 **该私聊流自身** 的 reminder
               中可见（可以看到群聊摘要），不会暴露给群聊或其他私聊
  - two_way  : 私聊流与群聊流完全互通（原始行为）
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .config import CrossStreamRelayConfig


def _normalize_id(value: str | int) -> str:
    """将 QQ 号统一转换为字符串进行比较。"""
    return str(value).strip()


def _check_list(
    target_id: str,
    list_type: str,
    id_list: list[str | int],
) -> bool:
    """根据黑白名单模式判断目标 ID 是否允许通过。

    Args:
        target_id: 待检测的群号或 QQ 号（字符串）
        list_type: "blacklist" 或 "whitelist"
        id_list: 黑/白名单列表

    Returns:
        True 表示允许（不被过滤），False 表示拒绝（被过滤掉）
    """
    normalized_list = {_normalize_id(v) for v in id_list}
    target = _normalize_id(target_id)

    if list_type == "whitelist":
        # 白名单：不在列表里则拒绝
        return target in normalized_list
    else:
        # 黑名单（默认）：在列表里则拒绝
        return target not in normalized_list


def should_collect_message(
    config: "CrossStreamRelayConfig",
    chat_type: str,
    target_id: str,
) -> bool:
    """判断该聊天流的消息是否应被收集进摘要缓冲。

    Args:
        config: 插件配置
        chat_type: 消息所属聊天类型（"group" / "private" / "discuss"）
        target_id: 群聊时为群号，私聊时为对方 QQ 号

    Returns:
        True 表示允许收集，False 表示跳过
    """
    priv = config.privacy

    if chat_type == "group":
        if not target_id:
            return True
        return _check_list(target_id, priv.group_list_type, priv.group_list)

    if chat_type == "private":
        mode = priv.private_bridge_mode
        if mode == "off":
            return False
        # one_way / two_way 都正常收集
        if not target_id:
            return True
        return _check_list(target_id, priv.private_list_type, priv.private_list)

    # discuss 或未知类型，默认允许
    return True


def should_show_in_reminder(
    config: "CrossStreamRelayConfig",
    record_chat_type: str,
    record_target_id: str,
    current_chat_type: str,
) -> bool:
    """判断某条聊天流摘要是否应注入到当前上下文的 actor reminder。

    核心逻辑：
    - off：私聊流摘要完全不注入任何 reminder
    - one_way：私聊流摘要仅注入到 **自身私聊流** 的 reminder；
               当前上下文是私聊时，可以看到群聊摘要；
               当前上下文是群聊时，看不到任何私聊摘要
    - two_way：完全双向互通，所有摘要都注入

    Args:
        config: 插件配置
        record_chat_type: 摘要记录所属的聊天类型
        record_target_id: 摘要记录的目标 ID（群号或 QQ 号）
        current_chat_type: 当前正在渲染 reminder 的聊天类型（可能为空）

    Returns:
        True 表示该摘要应出现在 reminder 中
    """
    priv = config.privacy

    # 先做黑白名单过滤（无论 mode 如何，不在允许范围的流摘要都不显示）
    if record_chat_type == "group":
        if record_target_id and not _check_list(
            record_target_id, priv.group_list_type, priv.group_list
        ):
            return False

    elif record_chat_type == "private":
        mode = priv.private_bridge_mode
        if mode == "off":
            return False

        if record_target_id and not _check_list(
            record_target_id, priv.private_list_type, priv.private_list
        ):
            return False

        if mode == "one_way":
            # one_way：私聊摘要只能注入到私聊流自身的 reminder
            # 当前上下文不是私聊时，不允许看到任何私聊摘要
            if current_chat_type != "private":
                return False

    return True
