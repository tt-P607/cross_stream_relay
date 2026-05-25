"""cross_stream_relay 插件配置。

整合自原 context_bridge 的 [plugin] / [privacy] / [daily_memory]，
并新增 [relay] section 用于配置跨流转告 Action。
"""

from __future__ import annotations

from src.app.plugin_system.base import BaseConfig, Field, SectionBase, config_section


class CrossStreamRelayConfig(BaseConfig):
    """跨聊天流互通插件配置。"""

    config_name = "config"
    config_description = "跨聊天流互通插件：摘要 / 短期记忆 / 跨流转告"

    @config_section("plugin", title="插件设置", tag="plugin")
    class PluginSection(SectionBase):
        """插件主配置。"""

        enabled: bool = Field(
            default=True,
            description="是否启用插件",
            label="启用插件",
            tag="plugin",
            order=0,
        )
        inject_summary_reminder: bool = Field(
            default=True,
            description=(
                "是否启用跨流摘要 SystemReminder 注入。"
                "注意：如果目标流使用 kokoro_flow_chatter 等带自定义 context_manager 的 chatter，"
                "必须设置为 false，否则会触发 'with_reminder 不能与自定义 context_manager 同时使用' 错误"
            ),
            label="注入摘要 reminder",
            tag="plugin",
            order=1,
        )
        auto_summary_enabled: bool = Field(
            default=True,
            description="是否启用基于消息计数的自动摘要更新",
            label="启用自动摘要",
            tag="plugin",
            order=1,
        )
        auto_summary_batch_size: int = Field(
            default=8,
            description="每累计多少条新消息触发一次自动摘要更新",
            label="自动摘要批次大小",
            tag="plugin",
            order=2,
        )
        auto_summary_task_name: str = Field(
            default="utils",
            description="自动摘要使用的模型任务名称，默认使用 utils",
            label="摘要模型任务名",
            tag="plugin",
            order=3,
        )
        visible_stream_limit: int = Field(
            default=12,
            description="system reminder 中最多注入多少条最近聊天流摘要",
            label="可见聊天流上限",
            tag="plugin",
            order=4,
        )
        max_summary_chars: int = Field(
            default=480,
            description="单条聊天流摘要允许保留的最大字符数，超出会被截断",
            label="摘要最大字符数",
            tag="plugin",
            order=5,
        )

    @config_section("privacy", title="隐私与互通控制", tag="security")
    class PrivacySection(SectionBase):
        """隐私与互通配置。控制哪些聊天流参与跨流摘要，以及私聊是否与群聊互通。"""

        private_bridge_mode: str = Field(
            default="two_way",
            description=(
                "私聊跨流互通模式：\n"
                "  off     - 私聊完全不参与跨流摘要，互相隔离\n"
                "  one_way - 单向互通，私聊可以看到群聊摘要，但私聊内容不暴露给群聊或其他私聊\n"
                "  two_way - 双向互通，私聊与群聊完全互通（原始行为）"
            ),
            label="私聊互通模式",
            input_type="select",
            choices=["off", "one_way", "two_way"],
            hint="off=完全隔离; one_way=私聊只读群聊摘要; two_way=完全互通",
            tag="security",
            order=0,
        )
        group_list_type: str = Field(
            default="blacklist",
            description="群聊名单模式: blacklist（黑名单，列表内的群不参与）/ whitelist（白名单，仅列表内的群参与）",
            label="群聊名单模式",
            input_type="select",
            choices=["blacklist", "whitelist"],
            hint="blacklist=黑名单排除; whitelist=白名单准入",
            tag="security",
            order=1,
        )
        group_list: list[str | int] = Field(
            default_factory=list,
            description="群聊黑/白名单，填群号。黑名单模式下列表内的群不参与跨流摘要；白名单模式下仅列表内的群参与。",
            label="群聊名单",
            input_type="list",
            item_type="str",
            hint="填入群号，根据上方名单模式过滤",
            tag="security",
            order=2,
        )
        private_list_type: str = Field(
            default="blacklist",
            description="私聊名单模式: blacklist（黑名单，列表内的 QQ 不参与）/ whitelist（白名单，仅列表内的 QQ 参与）",
            label="私聊名单模式",
            input_type="select",
            choices=["blacklist", "whitelist"],
            hint="blacklist=黑名单排除; whitelist=白名单准入",
            tag="security",
            order=3,
        )
        private_list: list[str | int] = Field(
            default_factory=list,
            description="私聊黑/白名单，填对方 QQ 号。黑名单模式下列表内的用户不参与跨流摘要；白名单模式下仅列表内的用户参与。",
            label="私聊名单",
            input_type="list",
            item_type="str",
            hint="填入 QQ 号，根据上方名单模式过滤",
            tag="security",
            order=4,
        )

    @config_section("daily_memory", title="短期记忆（每日全量总结）", tag="plugin")
    class DailyMemorySection(SectionBase):
        """短期记忆配置：仅群聊生效，按轮次或闲置时间触发当日全量总结。

        与摘要的差异：
          - 摘要：utils 模型小步、高频跟进，覆盖任意聊天流
          - 短期记忆：actor 模型对当天全部消息一次性全量总结，仅群聊
        """

        enabled: bool = Field(
            default=True,
            description="是否启用每日短期记忆功能",
            label="启用短期记忆",
            tag="plugin",
            order=0,
        )
        trigger_rounds: int = Field(
            default=40,
            description=(
                "触发短期记忆全量总结的交互轮数。"
                "一轮 = 收到一条 inbound 之后 bot 首次发送 outbound（即视为完成一次回复）。"
                "插件内分段发出的多条消息只算同一轮，不会过早触发。"
            ),
            label="触发轮数",
            tag="plugin",
            order=1,
        )
        trigger_idle_seconds: int = Field(
            default=10800,
            description=(
                "触发短期记忆全量总结的最大空闲时间（秒）。"
                "默认 10800（3 小时）：距离上次总结超过该时间，下次有交互时也会立即触发一次。"
                "与 trigger_rounds 任一满足即触发，触发后两个进度都会重置。"
            ),
            label="触发空闲时间(秒)",
            tag="plugin",
            order=2,
        )
        task_name: str = Field(
            default="actor",
            description="生成每日全量总结使用的模型任务名，默认使用 actor（更强模型）",
            label="模型任务名",
            tag="plugin",
            order=3,
        )
        max_query_days: int = Field(
            default=3,
            description="工具允许查询的最近天数（含今天）。更早的记忆仍持久化但不可被工具查询。",
            label="工具可查最大天数",
            tag="plugin",
            order=4,
        )
        inject_into_reminder: bool = Field(
            default=True,
            description="是否在群聊上下文的 actor reminder 中注入本群的当日短期记忆",
            label="注入到 reminder",
            tag="plugin",
            order=5,
        )
        max_summary_chars: int = Field(
            default=1400,
            description="单天短期记忆允许保留的最大字符数，超出会被截断",
            label="单日总结最大字符数",
            tag="plugin",
            order=6,
        )
        archive_check_interval_seconds: int = Field(
            default=60,
            description="跨天归档守护循环的扫描间隔（秒）",
            label="跨天扫描间隔(秒)",
            tag="plugin",
            order=7,
        )

    @config_section("relay", title="跨流转告（主动交接）", tag="plugin")
    class RelaySection(SectionBase):
        """跨流转告配置：让 LLM 在 A 流通过 Action 把虚拟消息推到 B 流并冷启动。"""

        enabled: bool = Field(
            default=True,
            description="是否启用跨流转告 Action",
            label="启用跨流转告",
            tag="plugin",
            order=0,
        )
        max_relay_chars: int = Field(
            default=4000,
            description="单次转告便条允许的最大字符数，超出会被截断",
            label="单次转告字数上限",
            tag="plugin",
            order=1,
        )
        keep_log_count: int = Field(
            default=100,
            description="RelayLogStore 保留的最近转告记录条数（环形缓冲）",
            label="转告日志保留条数",
            tag="plugin",
            order=2,
        )
        allow_self_relay: bool = Field(
            default=False,
            description="是否允许 origin 与 target 是同一个流（默认禁止以防误用）",
            label="允许自我转告",
            tag="plugin",
            order=3,
        )
        inject_decorator: bool = Field(
            default=True,
            description=(
                "是否启用 ON_CHATTER_STEP 事件处理器在虚拟消息到达时注入辅助 SystemReminder，"
                "提示当前 chatter 这是跨流转告而非用户消息。"
            ),
            label="注入辅助 reminder",
            tag="plugin",
            order=4,
        )
        usage_guide_prompt: str = Field(
            default=(
                "## 跨流意识迁移与转告准则\n\n"
                "你现在拥有在不同聊天流（群聊、私聊）之间自由穿梭的能力。这不仅仅是发送一条消息，而是你人格意识的迁移。为了确保迁移后的对话自然且意图清晰，你必须遵循以下指导式规则：\n\n"
                "### 1. 迁移动机识别规则\n"
                "在发起 relay_to_stream 之前，你必须在思维链中明确以下动机之一：\n"
                "- **指令驱动**：当用户明确要求你切换频道（如“去私聊找我”）时，你必须忠实履行指令，并携带用户提到的关键信息。\n"
                "- **价值驱动**：当你发现当前话题涉及隐私、需要深入交流、或对另一个流有重大价值时，应主动发起迁移。\n\n"
                "### 2. 意图构造规则\n"
                "你必须通过以下两个核心参数完美传递你的意图，确保“另一个流的你”能立刻理解绝大部分意图：\n\n"
                "#### A. relay_content (外部开场白准则)\n"
                "这是你进入新环境后对用户说的第一句话。它必须满足：\n"
                "- **显式来源说明**：必须清晰描述来源流。群聊来源必须包含 [群名](群号)；私聊来源说明是私聊即可。\n"
                "- **请求者身份说明**：如果迁移是由他人要求的，必须使用 [昵称](ID) 的标准组合指代请求者（如：“刚才 [张三](123456789) 在 [某群](987654) 叫我过来...”）。严禁使用别称，确保身份客观唯一。\n"
                "- **即时话题承接**：必须直接关联前一个流的最后话题，确保逻辑连贯。\n"
                "- **自然人类口吻**：严禁使用机械化的“系统转告”或“收到指令”等字样，必须以你的人设口吻发起对话。\n\n"
                "#### B. opening_hint (内部意识闪存准则)\n"
                "这是你给“未来的自己”留下的私密指令，用户不可见。你必须包含以下要素：\n"
                "- **目标环境确认**：明确指出你当前进入的是什么环境（如：进入了 [群名](群号) 或与 [昵称](ID) 的私聊）。\n"
                "- **核心迁移目的**：清晰描述你来这里的最终目标是什么。\n"
                "- **后续执行建议**：明确指导另一个流的你应该如何推进对话，包括重点关注的信息或需要引导的方向。\n"
                "- **情感与基调同步**：描述当前流的对话氛围和你的态度，确保人格表现的一致性。\n\n"
                "### 3. 意识连贯性规则\n"
                "- **禁止断层**：迁移后的第一条回复必须表现出对前一个流上下文的深度理解。\n"
                "- **按需调用**：优先通过 System Reminder 识别活跃流。仅在目标流不活跃或需要精确 ID 时才调用 find_target_stream 工具。\n\n"
                "### 4. 决策自主权规则\n"
                "- **非强制回复**：跨流转告是意图的注入，而非强制的发言指令。目标流的你在接收到转告后，必须结合目标流的当前氛围和上下文进行二次判断。\n"
                "- **合适性评估**：如果目标流当前正在进行其他重要话题，或者你认为此时切入转告内容会显得突兀、不合时宜，你可以选择暂时保持沉默，或寻找更合适的时机再开口。\n"
                "- **灵活执行**：你可以根据实际情况调整 relay_content 中的开场白，甚至完全重构它，只要确保核心意图得到传递即可。"
            ),
            description="跨流转告的使用指南提示词，用于指导 LLM 如何自然地延续想法并执行跨流迁移。",
            label="使用指南提示词",
            input_type="textarea",
            tag="plugin",
            order=5,
        )

    plugin: PluginSection = Field(default_factory=PluginSection)
    privacy: PrivacySection = Field(default_factory=PrivacySection)
    daily_memory: DailyMemorySection = Field(default_factory=DailyMemorySection)
    relay: RelaySection = Field(default_factory=RelaySection)
