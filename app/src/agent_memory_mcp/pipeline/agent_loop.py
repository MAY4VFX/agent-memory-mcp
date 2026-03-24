"""A-RAG agent loop — async ReAct with OpenAI function calling."""

from __future__ import annotations

import asyncio

import httpx
import openai
import orjson
import structlog

from agent_memory_mcp.llm.client import get_llm_client
from agent_memory_mcp.models.query import AgentBudgetConfig
from agent_memory_mcp.pipeline.agent_context import AgentContext
from agent_memory_mcp.pipeline.agent_tools import TOOL_DEFINITIONS, execute_tool
from agent_memory_mcp.tracing.tracer import trace_observation

log = structlog.get_logger(__name__)

_TIMEOUT_ERRORS = (
    httpx.ReadTimeout,
    httpx.ConnectTimeout,
    openai.APITimeoutError,
    asyncio.TimeoutError,
)


async def run_agent_loop(
    query: str,
    system_prompt: str,
    context: AgentContext,
    budget: AgentBudgetConfig,
    history_messages: list[dict] | None = None,
) -> tuple[str, list[ToolCallRecord]]:
    """Run the ReAct agent loop.

    Returns (final_answer, tool_call_records).
    """
    client = get_llm_client()

    messages: list[dict] = [{"role": "system", "content": system_prompt}]
    if history_messages:
        messages.extend(history_messages)
    messages.append({"role": "user", "content": query})

    for step in range(budget.max_steps):
        with trace_observation(
            as_type="generation",
            name=f"agent_step_{step}",
            metadata={"step": step, "tokens_used": context.tokens_used},
        ) as obs:
            try:
                response = await client.chat.completions.create(
                    model=budget.llm_model,
                    messages=messages,
                    tools=TOOL_DEFINITIONS,
                    tool_choice="auto",
                    temperature=budget.temperature,
                    max_tokens=budget.max_answer_tokens,
                )
            except _TIMEOUT_ERRORS:
                log.warning("agent_step_timeout", step=step)
                if step > 0:
                    return await _force_final_answer(
                        messages, budget,
                        hint="Превышено время ожидания. Сформируй ответ из уже собранных данных.",
                    ), context.tool_calls
                return (
                    "Запрос занял слишком много времени. "
                    "Попробуйте переформулировать вопрос или использовать режим «Быстрый»."
                ), context.tool_calls

            msg = response.choices[0].message

            if obs:
                obs.update(output={
                    "finish_reason": response.choices[0].finish_reason,
                    "tool_calls": len(msg.tool_calls) if msg.tool_calls else 0,
                    "tokens": response.usage.total_tokens if response.usage else 0,
                })

        # Append assistant message (with or without tool_calls)
        assistant_msg: dict = {"role": "assistant"}
        if msg.content:
            assistant_msg["content"] = msg.content
        if msg.tool_calls:
            assistant_msg["tool_calls"] = [
                {
                    "id": tc.id,
                    "type": "function",
                    "function": {
                        "name": tc.function.name,
                        "arguments": tc.function.arguments,
                    },
                }
                for tc in msg.tool_calls
            ]
        messages.append(assistant_msg)

        # No tool calls → agent is done
        if not msg.tool_calls:
            log.info("agent_done", step=step, answer_len=len(msg.content or ""))
            return msg.content or "", context.tool_calls

        # Execute tool calls in parallel
        async def _exec_one(tc):
            try:
                args = orjson.loads(tc.function.arguments)
            except Exception:
                args = {}

            with trace_observation(
                as_type="tool",
                name=tc.function.name,
                input=args,
            ) as tool_obs:
                result = await execute_tool(tc.function.name, args, context)
                if tool_obs:
                    tool_obs.update(output=result[:500])
            return tc.id, result

        tool_results = await asyncio.gather(
            *[_exec_one(tc) for tc in msg.tool_calls],
            return_exceptions=True,
        )

        for i, tr in enumerate(tool_results):
            if isinstance(tr, Exception):
                log.exception("tool_parallel_failed", error=str(tr))
                # Must still add tool response — API requires one per tool_call
                messages.append({
                    "role": "tool",
                    "tool_call_id": msg.tool_calls[i].id,
                    "content": f"Error: {tr}",
                })
                continue
            tc_id, result = tr
            messages.append({
                "role": "tool",
                "tool_call_id": tc_id,
                "content": result,
            })

        # Finalize: map-reduce produced extracted data — format for user
        if context.passthrough_answer:
            log.info("agent_finalize_start", step=step, raw_len=len(context.passthrough_answer))
            answer = await _finalize_mapreduce(
                query, context.passthrough_answer, budget,
            )
            return answer, context.tool_calls

        # Check token budget
        if context.tokens_used > budget.token_budget:
            log.warning("token_budget_exceeded",
                        used=context.tokens_used, budget=budget.token_budget)
            break

    # Budget exhausted → force final answer
    return await _force_final_answer(messages, budget), context.tool_calls


async def _finalize_mapreduce(
    query: str,
    raw_result: str,
    budget: AgentBudgetConfig,
) -> str:
    """Format map-reduce extracted data into a user-facing answer.

    Uses a clean context (no conversation history bloat) with a dedicated
    formatting prompt so the LLM focuses on presentation, not re-analysis.
    """
    from agent_memory_mcp.llm.query_prompts import FINALIZE_SYSTEM

    client = get_llm_client()
    messages = [
        {"role": "system", "content": FINALIZE_SYSTEM},
        {"role": "user", "content": f"Вопрос: {query}\n\nИзвлечённые данные:\n{raw_result}"},
    ]

    max_tokens = 16384  # model native max — LLM stops naturally when done

    with trace_observation(
        as_type="generation",
        name="finalize_mapreduce",
        metadata={"raw_len": len(raw_result), "max_tokens": max_tokens},
    ):
        try:
            response = await client.chat.completions.create(
                model=budget.llm_model,
                messages=messages,
                temperature=budget.temperature,
                max_tokens=max_tokens,
            )
            answer = response.choices[0].message.content or ""
            log.info("agent_finalize_done", answer_len=len(answer))
            return answer
        except _TIMEOUT_ERRORS:
            log.warning("finalize_mapreduce_timeout")
            return raw_result  # fallback to raw data


async def _force_final_answer(
    messages: list[dict],
    budget: AgentBudgetConfig,
    hint: str = "Бюджет шагов исчерпан. Ответь на основе уже собранной информации.",
) -> str:
    """Force the agent to give a final answer without more tool calls."""
    client = get_llm_client()
    messages.append({"role": "user", "content": hint})

    try:
        response = await client.chat.completions.create(
            model=budget.llm_model,
            messages=messages,
            tools=TOOL_DEFINITIONS,
            tool_choice="none",
            temperature=budget.temperature,
            max_tokens=budget.max_answer_tokens,
        )
        return response.choices[0].message.content or ""
    except _TIMEOUT_ERRORS:
        log.warning("force_final_answer_timeout")
        return (
            "Запрос занял слишком много времени. "
            "Попробуйте переформулировать вопрос или использовать режим «Быстрый»."
        )
    except (openai.BadRequestError, openai.RateLimitError) as exc:
        log.error("force_final_answer_api_error", error=str(exc))
        return (
            "Не удалось сформировать ответ — запрос слишком большой. "
            "Попробуйте более конкретный вопрос."
        )
