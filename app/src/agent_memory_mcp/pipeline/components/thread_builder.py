"""Thread builder component -- groups messages into conversation threads."""

from __future__ import annotations

import uuid
from collections import defaultdict

from haystack import component

from agent_memory_mcp.models.messages import ProcessedMessage, ThreadGroup

_TEMPORAL_WINDOW_SEC = 300  # 5 minutes


@component
class ThreadBuilder:
    """Groups messages into threads by reply chains + temporal proximity."""

    @component.output_types(threads=list[ThreadGroup])
    def run(self, messages: list[ProcessedMessage], domain_id: str) -> dict:
        if not messages:
            return {"threads": []}

        domain_uuid = uuid.UUID(domain_id)

        # Sort chronologically
        sorted_msgs = sorted(messages, key=lambda m: m.date)

        # Index messages by message_id for reply-chain lookup
        by_msg_id: dict[int, ProcessedMessage] = {m.message_id: m for m in sorted_msgs}

        # Phase 1: Build reply chains (union-find style)
        parent: dict[int, int] = {}  # message_id -> root message_id

        def find_root(mid: int) -> int:
            while parent.get(mid, mid) != mid:
                parent[mid] = parent.get(parent[mid], parent[mid])
                mid = parent[mid]
            return mid

        for msg in sorted_msgs:
            mid = msg.message_id
            if mid not in parent:
                parent[mid] = mid
            if msg.reply_to_msg_id and msg.reply_to_msg_id in by_msg_id:
                reply_root = find_root(msg.reply_to_msg_id)
                parent[mid] = reply_root

        # Phase 2: Group messages by root
        chains: dict[int, list[ProcessedMessage]] = defaultdict(list)
        assigned: set[int] = set()
        for msg in sorted_msgs:
            root = find_root(msg.message_id)
            if root != msg.message_id or msg.reply_to_msg_id:
                chains[root].append(msg)
                assigned.add(msg.message_id)

        # Include root messages in their chains
        for root_id in list(chains.keys()):
            if root_id not in assigned and root_id in by_msg_id:
                chains[root_id].insert(0, by_msg_id[root_id])
                assigned.add(root_id)

        # Phase 3: Temporal proximity for unassigned messages
        unassigned = [m for m in sorted_msgs if m.message_id not in assigned]
        temporal_groups: list[list[ProcessedMessage]] = []
        current_group: list[ProcessedMessage] = []

        for msg in unassigned:
            if not current_group:
                current_group.append(msg)
            else:
                last_date = current_group[-1].date
                delta = abs((msg.date - last_date).total_seconds())
                if delta <= _TEMPORAL_WINDOW_SEC:
                    current_group.append(msg)
                else:
                    temporal_groups.append(current_group)
                    current_group = [msg]

        if current_group:
            temporal_groups.append(current_group)

        # Phase 4: Build ThreadGroup objects
        threads: list[ThreadGroup] = []

        for root_id, msgs in chains.items():
            tg = ThreadGroup(
                domain_id=domain_uuid,
                root_message_id=root_id,
                messages=msgs,
            )
            tg.make_deterministic_id()
            tg.build_combined_text()
            # Set thread_id on each message
            for m in msgs:
                m.thread_id = tg.id
            threads.append(tg)

        for group in temporal_groups:
            tg = ThreadGroup(
                domain_id=domain_uuid,
                root_message_id=group[0].message_id,
                messages=group,
            )
            tg.make_deterministic_id()
            tg.build_combined_text()
            for m in group:
                m.thread_id = tg.id
            threads.append(tg)

        return {"threads": threads}
