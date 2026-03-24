"""Keyboards for the Telegram bot."""

import math

from aiogram.types import (
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    KeyboardButton,
    ReplyKeyboardMarkup,
)

PAGE_SIZE = 20


def _truncate_name(name: str, max_words: int = 2) -> str:
    words = name.split()
    return " ".join(words[:max_words]) + ("\u2026" if len(words) > max_words else "")


def _page_nav(items: list, page: int, callback_prefix: str) -> list:
    """Return navigation row [◀️ N/M ▶️] if items exceed PAGE_SIZE."""
    total_pages = max(1, math.ceil(len(items) / PAGE_SIZE))
    if total_pages <= 1:
        return []
    row = []
    if page > 0:
        row.append(InlineKeyboardButton(text="\u25c0\ufe0f", callback_data=f"{callback_prefix}{page - 1}"))
    row.append(InlineKeyboardButton(text=f"{page + 1}/{total_pages}", callback_data="noop"))
    if page < total_pages - 1:
        row.append(InlineKeyboardButton(text="\u25b6\ufe0f", callback_data=f"{callback_prefix}{page + 1}"))
    return [row]


def _paginate(items: list, page: int) -> list:
    """Return slice of items for the given page."""
    start = page * PAGE_SIZE
    return items[start : start + PAGE_SIZE]


def main_menu_kb(active_domain: dict | None = None, scope_label: str = "") -> ReplyKeyboardMarkup:
    if scope_label:
        domain_btn = _truncate_name(scope_label, max_words=3)
    elif active_domain:
        display = _truncate_name(active_domain['display_name'])
        domain_btn = f"{active_domain['emoji']} {display}"
    else:
        domain_btn = "\U0001f4da \u0418\u0441\u0442\u043e\u0447\u043d\u0438\u043a\u0438"
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text=domain_btn), KeyboardButton(text="\u2795 \u041d\u043e\u0432\u044b\u0439 \u0434\u0438\u0430\u043b\u043e\u0433")],
            [KeyboardButton(text="\U0001f4ac \u0414\u0438\u0430\u043b\u043e\u0433\u0438"), KeyboardButton(text="\u2699\ufe0f \u041d\u0430\u0441\u0442\u0440\u043e\u0439\u043a\u0438")],
        ],
        resize_keyboard=True,
    )


def period_kb() -> InlineKeyboardMarkup:
    buttons = [
        [InlineKeyboardButton(text="1 \u043d\u0435\u0434\u0435\u043b\u044f", callback_data="period:1w")],
        [InlineKeyboardButton(text="1 \u043c\u0435\u0441\u044f\u0446", callback_data="period:1m")],
        [InlineKeyboardButton(text="3 \u043c\u0435\u0441\u044f\u0446\u0430", callback_data="period:3m")],
        [InlineKeyboardButton(text="6 \u043c\u0435\u0441\u044f\u0446\u0435\u0432", callback_data="period:6m")],
        [InlineKeyboardButton(text="1 \u0433\u043e\u0434", callback_data="period:1y")],
        [InlineKeyboardButton(text="3 \u0433\u043e\u0434\u0430", callback_data="period:3y")],
        [InlineKeyboardButton(text="\u0412\u0441\u0435 \u0441\u043e\u043e\u0431\u0449\u0435\u043d\u0438\u044f", callback_data="period:all")],
    ]
    return InlineKeyboardMarkup(inline_keyboard=buttons)


def frequency_kb() -> InlineKeyboardMarkup:
    buttons = [
        [
            InlineKeyboardButton(text="5 \u043c\u0438\u043d", callback_data="freq:5"),
            InlineKeyboardButton(text="15 \u043c\u0438\u043d", callback_data="freq:15"),
            InlineKeyboardButton(text="30 \u043c\u0438\u043d", callback_data="freq:30"),
            InlineKeyboardButton(text="60 \u043c\u0438\u043d", callback_data="freq:60"),
        ],
        [
            InlineKeyboardButton(text="2\u0447", callback_data="freq:120"),
            InlineKeyboardButton(text="4\u0447", callback_data="freq:240"),
            InlineKeyboardButton(text="8\u0447", callback_data="freq:480"),
            InlineKeyboardButton(text="24\u0447", callback_data="freq:1440"),
        ],
    ]
    return InlineKeyboardMarkup(inline_keyboard=buttons)


def emoji_kb() -> InlineKeyboardMarkup:
    emojis = ["\U0001f525", "\U0001f916", "\U0001f3ac", "\U0001f9e0", "\U0001f4a1", "\U0001f4da", "\U0001f3a8", "\U0001f4b0"]
    buttons = [
        [InlineKeyboardButton(text=e, callback_data=f"emoji:{e}") for e in emojis[:4]],
        [InlineKeyboardButton(text=e, callback_data=f"emoji:{e}") for e in emojis[4:]],
    ]
    return InlineKeyboardMarkup(inline_keyboard=buttons)


def domain_list_kb(domains_list: list[dict]) -> InlineKeyboardMarkup:
    buttons = []
    for d in domains_list:
        label = f"{d['emoji']} {d['display_name']}"
        buttons.append([InlineKeyboardButton(text=label, callback_data=f"domain:view:{d['id']}")])
    buttons.append([InlineKeyboardButton(text="\u2795 \u0414\u043e\u0431\u0430\u0432\u0438\u0442\u044c \u0438\u0441\u0442\u043e\u0447\u043d\u0438\u043a", callback_data="domain:add")])
    buttons.append([InlineKeyboardButton(text="\u2b05 \u041d\u0430\u0437\u0430\u0434", callback_data="hub:back")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)


def domain_actions_kb(domain_id: str) -> InlineKeyboardMarkup:
    buttons = [
        [InlineKeyboardButton(text="\u0420\u0435\u0434\u0430\u043a\u0442\u0438\u0440\u043e\u0432\u0430\u0442\u044c", callback_data=f"domain:edit:{domain_id}")],
        [InlineKeyboardButton(text="\u0423\u0434\u0430\u043b\u0438\u0442\u044c", callback_data=f"domain:delete:{domain_id}")],
        [InlineKeyboardButton(text="\u2b05 \u041d\u0430\u0437\u0430\u0434", callback_data="domain:back")],
    ]
    return InlineKeyboardMarkup(inline_keyboard=buttons)


def confirm_delete_kb(domain_id: str) -> InlineKeyboardMarkup:
    buttons = [
        [
            InlineKeyboardButton(text="\u0414\u0430, \u0443\u0434\u0430\u043b\u0438\u0442\u044c", callback_data=f"confirm_delete:{domain_id}"),
            InlineKeyboardButton(text="\u041e\u0442\u043c\u0435\u043d\u0430", callback_data="cancel_delete"),
        ],
    ]
    return InlineKeyboardMarkup(inline_keyboard=buttons)


def conversation_list_kb(convs: list[dict]) -> InlineKeyboardMarkup:
    """Build inline keyboard with conversation list.

    Each conv dict may contain ``domain_emoji`` (from JOIN with domains).
    Falls back to ``\U0001f4ac`` if not present.
    """
    buttons = []
    for c in convs:
        title = c.get("title") or "\u0411\u0435\u0437 \u043d\u0430\u0437\u0432\u0430\u043d\u0438\u044f"
        emoji = c.get("domain_emoji") or "\U0001f4ac"
        label = f"{emoji} {title[:40]}"
        buttons.append([
            InlineKeyboardButton(text=label, callback_data=f"conv:resume:{c['id']}"),
            InlineKeyboardButton(text="\u274c", callback_data=f"conv:delete:{c['id']}"),
        ])
    return InlineKeyboardMarkup(inline_keyboard=buttons)


def settings_kb(is_admin: bool = False) -> InlineKeyboardMarkup:
    buttons = [
        [InlineKeyboardButton(text="\U0001f50d \u0420\u0435\u0436\u0438\u043c \u043f\u043e\u0438\u0441\u043a\u0430", callback_data="settings:search_mode")],
        [InlineKeyboardButton(text="\U0001f4f0 \u0414\u0430\u0439\u0434\u0436\u0435\u0441\u0442", callback_data="settings:digest")],
    ]
    if is_admin:
        buttons.append(
            [InlineKeyboardButton(text="\U0001f9ea \u0422\u0435\u0441\u0442\u044b", callback_data="settings:tests")]
        )
    return InlineKeyboardMarkup(inline_keyboard=buttons)


def tests_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="\U0001f4dd \u0414\u043e\u0431\u0430\u0432\u0438\u0442\u044c \u043f\u0430\u0440\u0443", callback_data="eval:add")],
        [
            InlineKeyboardButton(text="\U0001f4cb Golden \u043f\u0430\u0440\u044b", callback_data="eval:list"),
            InlineKeyboardButton(text="\u25b6\ufe0f \u0417\u0430\u043f\u0443\u0441\u043a", callback_data="eval:run"),
        ],
        [InlineKeyboardButton(text="\U0001f4ca \u0420\u0435\u0437\u0443\u043b\u044c\u0442\u0430\u0442\u044b", callback_data="eval:status")],
        [InlineKeyboardButton(text="\u25c0\ufe0f \u041d\u0430\u0437\u0430\u0434", callback_data="eval:back")],
    ])


# ------------------------------------------------------------------ Sources Hub & Lists

def sources_hub_kb(
    orphan_domains: list[dict],
    groups: list[dict],
    active_scope: str = "domain",
    active_scope_id: str = "",
    total_domain_count: int = 0,
    page: int = 0,
) -> InlineKeyboardMarkup:
    """Sources hub: orphan sources + lists + all."""
    buttons = []

    # Combine orphan sources + groups into one paginated list
    items = []
    for d in orphan_domains:
        check = "\u2705 " if active_scope == "domain" and str(d["id"]) == active_scope_id else ""
        label = f"{check}{d['emoji']} {d['display_name']}"
        items.append(InlineKeyboardButton(text=label, callback_data=f"scope:channel:{d['id']}"))
    for g in groups:
        check = "\u2705 " if active_scope == "group" and str(g["id"]) == active_scope_id else ""
        count = g.get("member_count")
        suffix = f" ({count})" if count else ""
        label = f"{check}\U0001f4c1 {g['name']}{suffix}"
        items.append(InlineKeyboardButton(text=label, callback_data=f"scope:group:{g['id']}"))

    for btn in _paginate(items, page):
        buttons.append([btn])
    buttons.extend(_page_nav(items, page, "pg:hub:"))

    # All sources
    total = total_domain_count or len(orphan_domains)
    check = "\u2705 " if active_scope == "all" else ""
    buttons.append([InlineKeyboardButton(
        text=f"{check}\U0001f30d \u0412\u0441\u0435 \u0438\u0441\u0442\u043e\u0447\u043d\u0438\u043a\u0438 ({total})",
        callback_data="scope:all",
    )])

    # Management section
    buttons.append([
        InlineKeyboardButton(text="\u2795 \u0418\u0441\u0442\u043e\u0447\u043d\u0438\u043a\u0438", callback_data="hub:add_sources"),
        InlineKeyboardButton(text="\u270f\ufe0f \u0420\u0435\u0434\u0430\u043a\u0442\u0438\u0440\u043e\u0432\u0430\u0442\u044c", callback_data="hub:manage"),
    ])

    return InlineKeyboardMarkup(inline_keyboard=buttons)


def skip_list_name_kb() -> InlineKeyboardMarkup:
    """Skip naming a list for batch-added sources."""
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="\u27a1\ufe0f \u0411\u0435\u0437 \u0441\u043f\u0438\u0441\u043a\u0430", callback_data="batch:no_list")],
    ])


def add_sources_kb() -> InlineKeyboardMarkup:
    """Sub-menu for adding sources."""
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="\U0001f4c2 \u0418\u043c\u043f\u043e\u0440\u0442 \u0438\u0437 \u043f\u0430\u043f\u043a\u0438 TG", callback_data="hub:folders")],
        [InlineKeyboardButton(text="\u2b05 \u041d\u0430\u0437\u0430\u0434", callback_data="hub:back")],
    ])


def folder_list_kb(folders: list[dict]) -> InlineKeyboardMarkup:
    """Inline keyboard listing TG folders for import."""
    buttons = []
    for f in folders:
        label = f"\U0001f4c1 {f['title']} ({len(f['peers'])} \u043a\u0430\u043d\u0430\u043b\u043e\u0432)"
        buttons.append([InlineKeyboardButton(text=label, callback_data=f"folder_import:{f['id']}")])
    buttons.append([InlineKeyboardButton(text="\u2b05 \u041d\u0430\u0437\u0430\u0434", callback_data="hub:back")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)


def group_list_kb(groups: list[dict]) -> InlineKeyboardMarkup:
    """Inline keyboard listing user's source lists."""
    buttons = []
    for g in groups:
        count = g.get("member_count")
        suffix = f" ({count})" if count else ""
        label = f"{g['emoji']} {g['name']}{suffix}"
        buttons.append([InlineKeyboardButton(text=label, callback_data=f"group:view:{g['id']}")])
    buttons.append([InlineKeyboardButton(text="\u2795 \u0421\u043e\u0437\u0434\u0430\u0442\u044c \u0441\u043f\u0438\u0441\u043e\u043a", callback_data="groups:create")])
    buttons.append([InlineKeyboardButton(text="\u2b05 \u041d\u0430\u0437\u0430\u0434", callback_data="hub:back")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)


def list_detail_kb(group_id: str, group_domains: list[dict], page: int = 0) -> InlineKeyboardMarkup:
    """Detail view for a list with per-source remove + sync status."""
    buttons = []
    for d in _paginate(group_domains, page):
        msg_count = d.get("message_count") or 0
        status = f"\u2705 {msg_count}" if d.get("last_synced_at") else "\u23f3"
        label = f"{d['emoji']} {d['display_name']} [{status}]"
        buttons.append([
            InlineKeyboardButton(
                text=label,
                callback_data=f"domain:view:{d['id']}",
            ),
            InlineKeyboardButton(text="\u2716\ufe0f", callback_data=f"grp_rm:{d['id']}"),
        ])
    buttons.extend(_page_nav(group_domains, page, f"pg:ld:{group_id}:"))
    buttons.append([InlineKeyboardButton(text="\U0001f50d \u0418\u0441\u043a\u0430\u0442\u044c \u0432 \u0441\u043f\u0438\u0441\u043a\u0435", callback_data=f"scope:group:{group_id}")])
    buttons.append([InlineKeyboardButton(text="\u2795 \u0414\u043e\u0431\u0430\u0432\u0438\u0442\u044c \u0438\u0441\u0442\u043e\u0447\u043d\u0438\u043a\u0438", callback_data=f"grp_pick:{group_id}")])
    buttons.append([InlineKeyboardButton(text="\U0001f5d1 \u0423\u0434\u0430\u043b\u0438\u0442\u044c \u0441\u043f\u0438\u0441\u043e\u043a", callback_data=f"group:delete:{group_id}")])
    buttons.append([InlineKeyboardButton(text="\u2b05 \u041d\u0430\u0437\u0430\u0434", callback_data="hub:manage")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)


def source_picker_kb(
    group_id: str,
    all_domains: list[dict],
    group_domain_ids: set[str],
    page: int = 0,
) -> InlineKeyboardMarkup:
    """Toggle sources in/out of a list."""
    buttons = []
    for d in _paginate(all_domains, page):
        in_group = str(d["id"]) in group_domain_ids
        check = "\u2705" if in_group else "\u2b1c"
        label = f"{check} {d['emoji']} {d['display_name']}"
        buttons.append([InlineKeyboardButton(text=label, callback_data=f"grp_toggle:{d['id']}")])
    buttons.extend(_page_nav(all_domains, page, f"pg:sp:{group_id}:"))
    buttons.append([InlineKeyboardButton(text="\u2705 \u0413\u043e\u0442\u043e\u0432\u043e", callback_data=f"grp_pick_done:{group_id}")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)


def manage_kb(orphan_domains: list[dict], groups: list[dict], page: int = 0) -> InlineKeyboardMarkup:
    """Unified manage view: individual sources + lists."""
    buttons = []

    # Combine orphans + groups into a single paginated list
    items = []
    for d in orphan_domains:
        mc = d.get("message_count") or 0
        status = f"\u2705 {mc}" if d.get("last_synced_at") else "\u23f3"
        label = f"{d['emoji']} {d['display_name']} [{status}]"
        items.append(InlineKeyboardButton(text=label, callback_data=f"domain:view:{d['id']}"))
    for g in groups:
        count = g.get("member_count")
        suffix = f" ({count})" if count else ""
        label = f"\U0001f4c1 {g['name']}{suffix}"
        items.append(InlineKeyboardButton(text=label, callback_data=f"group:view:{g['id']}"))

    if not items:
        buttons.append([InlineKeyboardButton(text="(\u043f\u0443\u0441\u0442\u043e)", callback_data="noop")])
    else:
        for btn in _paginate(items, page):
            buttons.append([btn])
        buttons.extend(_page_nav(items, page, "pg:manage:"))

    buttons.append([InlineKeyboardButton(text="\u2795 \u0421\u043e\u0437\u0434\u0430\u0442\u044c \u0441\u043f\u0438\u0441\u043e\u043a", callback_data="groups:create")])
    buttons.append([InlineKeyboardButton(text="\u2b05 \u041d\u0430\u0437\u0430\u0434", callback_data="hub:back")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)


def domain_edit_kb(domain_id: str) -> InlineKeyboardMarkup:
    """Edit options for a domain."""
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="\u23f1 \u0427\u0430\u0441\u0442\u043e\u0442\u0430 \u0441\u0438\u043d\u0445\u0440\u043e\u043d\u0438\u0437\u0430\u0446\u0438\u0438", callback_data=f"dedit:freq:{domain_id}")],
        [InlineKeyboardButton(text="\U0001f4c5 \u0413\u043b\u0443\u0431\u0438\u043d\u0430 \u0441\u0438\u043d\u0445\u0440\u043e\u043d\u0438\u0437\u0430\u0446\u0438\u0438", callback_data=f"dedit:depth:{domain_id}")],
        [InlineKeyboardButton(text="\U0001f600 \u042d\u043c\u043e\u0434\u0437\u0438", callback_data=f"dedit:emoji:{domain_id}")],
        [InlineKeyboardButton(text="\u2b05 \u041d\u0430\u0437\u0430\u0434", callback_data=f"domain:view:{domain_id}")],
    ])


def edit_freq_kb(domain_id: str) -> InlineKeyboardMarkup:
    """Frequency options for editing a domain."""
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="5 \u043c\u0438\u043d", callback_data=f"efreq:{domain_id}:5"),
            InlineKeyboardButton(text="15 \u043c\u0438\u043d", callback_data=f"efreq:{domain_id}:15"),
            InlineKeyboardButton(text="30 \u043c\u0438\u043d", callback_data=f"efreq:{domain_id}:30"),
            InlineKeyboardButton(text="60 \u043c\u0438\u043d", callback_data=f"efreq:{domain_id}:60"),
        ],
        [
            InlineKeyboardButton(text="2\u0447", callback_data=f"efreq:{domain_id}:120"),
            InlineKeyboardButton(text="4\u0447", callback_data=f"efreq:{domain_id}:240"),
            InlineKeyboardButton(text="8\u0447", callback_data=f"efreq:{domain_id}:480"),
            InlineKeyboardButton(text="24\u0447", callback_data=f"efreq:{domain_id}:1440"),
        ],
        [InlineKeyboardButton(text="\u2b05 \u041d\u0430\u0437\u0430\u0434", callback_data=f"domain:view:{domain_id}")],
    ])


def edit_depth_kb(domain_id: str) -> InlineKeyboardMarkup:
    """Depth options for editing a domain."""
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="1 \u043d\u0435\u0434\u0435\u043b\u044f", callback_data=f"edepth:{domain_id}:1w")],
        [InlineKeyboardButton(text="1 \u043c\u0435\u0441\u044f\u0446", callback_data=f"edepth:{domain_id}:1m")],
        [InlineKeyboardButton(text="3 \u043c\u0435\u0441\u044f\u0446\u0430", callback_data=f"edepth:{domain_id}:3m")],
        [InlineKeyboardButton(text="6 \u043c\u0435\u0441\u044f\u0446\u0435\u0432", callback_data=f"edepth:{domain_id}:6m")],
        [InlineKeyboardButton(text="1 \u0433\u043e\u0434", callback_data=f"edepth:{domain_id}:1y")],
        [InlineKeyboardButton(text="\u0412\u0441\u0435", callback_data=f"edepth:{domain_id}:all")],
        [InlineKeyboardButton(text="\u2b05 \u041d\u0430\u0437\u0430\u0434", callback_data=f"domain:view:{domain_id}")],
    ])


def edit_emoji_kb(domain_id: str) -> InlineKeyboardMarkup:
    """Emoji options for editing a domain."""
    emojis = ["\U0001f525", "\U0001f916", "\U0001f3ac", "\U0001f9e0", "\U0001f4a1", "\U0001f4da", "\U0001f3a8", "\U0001f4b0"]
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=e, callback_data=f"eemoji:{domain_id}:{e}") for e in emojis[:4]],
        [InlineKeyboardButton(text=e, callback_data=f"eemoji:{domain_id}:{e}") for e in emojis[4:]],
        [InlineKeyboardButton(text="\u2b05 \u041d\u0430\u0437\u0430\u0434", callback_data=f"domain:view:{domain_id}")],
    ])


_SCOPE_LABELS = {
    "all": "\U0001f30d Все источники",
    "domain": "\U0001f4da Источник",
    "group": "\U0001f4c1 Список",
}


def _digest_scope_label(config: dict) -> str:
    scope_type = config.get("scope_type", "all")
    scope_name = config.get("scope_name")  # joined from handler
    base = _SCOPE_LABELS.get(scope_type, scope_type)
    if scope_name and scope_type != "all":
        return f"{base}: {scope_name}"
    return base


def digest_settings_kb(config: dict | None = None) -> InlineKeyboardMarkup:
    """Digest settings keyboard."""
    if config and config.get("is_active"):
        scope_label = _digest_scope_label(config)
        return InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text=f"\U0001f4cb {scope_label}", callback_data="digest:scope")],
            [InlineKeyboardButton(text=f"\u23f0 \u0427\u0430\u0441: {config['send_hour_utc']}:00 UTC", callback_data="digest:hour")],
            [InlineKeyboardButton(text="\U0001f50d \u041f\u0440\u0435\u0432\u044c\u044e", callback_data="digest:preview")],
            [InlineKeyboardButton(text="\u23f8 \u041e\u0442\u043a\u043b\u044e\u0447\u0438\u0442\u044c", callback_data="digest:disable")],
            [InlineKeyboardButton(text="\u2b05 \u041d\u0430\u0437\u0430\u0434", callback_data="digest:back")],
        ])
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="\u25b6\ufe0f \u0412\u043a\u043b\u044e\u0447\u0438\u0442\u044c \u0434\u0430\u0439\u0434\u0436\u0435\u0441\u0442", callback_data="digest:enable")],
        [InlineKeyboardButton(text="\u2b05 \u041d\u0430\u0437\u0430\u0434", callback_data="digest:back")],
    ])


def digest_scope_kb(
    domains: list[dict],
    groups: list[dict],
    current_scope_type: str = "all",
    current_scope_id: str = "",
    page: int = 0,
) -> InlineKeyboardMarkup:
    """Select digest scope: individual source, list, or all."""
    buttons = []

    # Combine sources + groups
    items = []
    for d in domains:
        check = "\u2705 " if current_scope_type == "domain" and str(d["id"]) == current_scope_id else ""
        label = f"{check}{d['emoji']} {d['display_name']}"
        items.append(InlineKeyboardButton(text=label, callback_data=f"dscope:d:{d['id']}"))
    for g in groups:
        check = "\u2705 " if current_scope_type == "group" and str(g["id"]) == current_scope_id else ""
        count = g.get("member_count")
        suffix = f" ({count})" if count else ""
        label = f"{check}\U0001f4c1 {g['name']}{suffix}"
        items.append(InlineKeyboardButton(text=label, callback_data=f"dscope:g:{g['id']}"))

    for btn in _paginate(items, page):
        buttons.append([btn])
    buttons.extend(_page_nav(items, page, "pg:ds:"))

    # All sources
    check = "\u2705 " if current_scope_type == "all" else ""
    buttons.append([InlineKeyboardButton(
        text=f"{check}\U0001f30d \u0412\u0441\u0435 \u0438\u0441\u0442\u043e\u0447\u043d\u0438\u043a\u0438",
        callback_data="dscope:all",
    )])

    buttons.append([InlineKeyboardButton(text="\u2b05 \u041d\u0430\u0437\u0430\u0434", callback_data="digest:scope_back")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)


def digest_hour_kb() -> InlineKeyboardMarkup:
    """Select UTC hour for digest delivery."""
    rows = []
    for start in range(0, 24, 6):
        row = [
            InlineKeyboardButton(text=f"{h}:00", callback_data=f"digest:set_hour:{h}")
            for h in range(start, min(start + 6, 24))
        ]
        rows.append(row)
    return InlineKeyboardMarkup(inline_keyboard=rows)


_SEARCH_MODE_LABELS = {
    "fast": "\u26a1 \u0411\u044b\u0441\u0442\u0440\u044b\u0439",
    "balanced": "\U0001f3af \u0422\u043e\u0447\u043d\u044b\u0439",
    "deep": "\U0001f52c \u0413\u043b\u0443\u0431\u043e\u043a\u0438\u0439",
}


def search_mode_kb(current_mode: str) -> InlineKeyboardMarkup:
    buttons = []
    for key, label in _SEARCH_MODE_LABELS.items():
        text = f"\u2705 {label}" if key == current_mode else label
        buttons.append([InlineKeyboardButton(text=text, callback_data=f"mode:{key}")])
    buttons.append([InlineKeyboardButton(text="\u2b05\ufe0f \u041d\u0430\u0437\u0430\u0434", callback_data="settings:back")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)
