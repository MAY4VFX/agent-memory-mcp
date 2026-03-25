"""System prompt for the A-RAG agent loop."""

from __future__ import annotations


def _format_schema_block(schema: dict) -> str:
    """Format SGR schema into a compact knowledge-map for the agent."""
    lines: list[str] = []

    domain = schema.get("detected_domain", "")
    if domain:
        lines.append(f"Домен канала: {domain}")

    et = schema.get("entity_types") or []
    if et:
        lines.append("\nСущности в графе знаний:")
        for e in et[:15]:
            name = e.get("name", "")
            desc = e.get("description", "")
            examples = e.get("examples") or []
            ex_str = f" (напр. {', '.join(examples[:4])})" if examples else ""
            lines.append(f"  - {name}: {desc}{ex_str}")

    rt = schema.get("relation_types") or []
    if rt:
        lines.append("\nСвязи между сущностями:")
        for r in rt[:12]:
            src = r.get("source_type", "?")
            tgt = r.get("target_type", "?")
            rname = r.get("name", "")
            desc = r.get("description", "")
            lines.append(f"  - {src} →[{rname}]→ {tgt}: {desc}")

    return "\n".join(lines)


def build_agent_system_prompt(
    channel_username: str,
    schema: dict | None = None,
    sources: list[dict] | None = None,
) -> str:
    """Build the system prompt for the agent, incorporating domain context.

    Args:
        channel_username: Primary channel (for single-source compat).
        schema: Schema of primary domain (optional).
        sources: List of all connected sources [{channel_username, display_name, message_count}].
    """
    schema_block = _format_schema_block(schema) if schema else ""

    # Build sources description
    if sources and len(sources) > 1:
        src_lines = []
        for s in sources:
            name = f"@{s.get('channel_username', '')}" if s.get('channel_username') else s.get('display_name', '?')
            msgs = s.get('message_count', 0)
            if msgs > 0:
                src_lines.append(f"  - {name} ({msgs} сообщений)")
        sources_block = "Подключённые источники:\n" + "\n".join(src_lines) if src_lines else ""
        intro = f"Ты — ассистент памяти AI-агента. У тебя есть доступ к {len(sources)} Telegram-каналам."
    else:
        sources_block = ""
        intro = f"Ты — ассистент базы знаний Telegram-канала @{channel_username}."

    return f"""{intro}
{sources_block}
{schema_block}

## Инструменты
- keyword_search: BM25 поиск по точным терминам, хештегам, именам. Возвращает snippets (200 символов) + связи из графа знаний для найденных сущностей.
- semantic_search: векторный поиск по концепциям, темам, смысловому сходству. Возвращает snippets + связи из графа знаний.
- read_messages: полный контент сообщений по ID. Вызови ПОСЛЕ поиска для получения полного текста.
- graph_search: поиск по графу знаний — сущности, связи, тематические группы.
- graph_query: произвольный запрос к графу знаний на естественном языке. Преобразует вопрос в Cypher и выполняет.
- rerank_results: переранжировать результаты поиска кросс-энкодером. Используй при >10 результатах.
- get_domain_info: метаданные домена (тема, типы сущностей).
- analyze_large_set: map-reduce анализ для больших наборов (>30 постов).

## Стратегия
1. Приветствие или off-topic → ответь без инструментов.
2. "Какие каналы/источники подключены?" → перечисли из списка источников выше, не ищи.
3. Точные термины, имена, хештеги → keyword_search.
4. Концепции, темы, "расскажи про X" → semantic_search.
5. Связи между сущностями → graph_search или graph_query.
6. Snippets недостаточно → read_messages для полного контента.
7. Много результатов (>30) + обзорный вопрос → analyze_large_set.
8. >10 результатов и нужна точность → rerank_results.
9. Комбинируй инструменты для полноты ответа.

## Правила
- НЕ придумывай информацию, которой нет в результатах поиска.
- НЕ вызывай один и тот же инструмент с теми же параметрами дважды.
- Отвечай на языке запроса (русский/английский).
- Будь конкретным, цитируй факты из найденного контента.
- Формат ответа: обычный текст, без markdown заголовков.
- Если спрашивают про все каналы — ищи по всем, не ограничивайся одним.
- Если ничего не найдено — честно скажи об этом."""
