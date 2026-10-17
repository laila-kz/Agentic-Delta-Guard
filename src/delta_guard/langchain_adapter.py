"""
langchain_adapter.py -- LangChain Callback Adapter for Agentic Delta Guard.

Translates LangChain agent lifecycle events (tool calls, LLM completions,
errors) into canonical 9-field Agentic Delta Guard events and publishes them
asynchronously to the Kafka topic the PySpark gatekeeper consumes.

COST MAPPING:
    Model-specific token pricing is loaded from configs/model_pricing.yaml.
    If usage metadata is absent or the model is unknown, it falls back to
    the configurable token rate (default: $0.00001 per token).

WIRE FORMAT COMPATIBILITY:
    Events are emitted as JSON-serialized dicts with exactly the nine contract
    fields, matching the format producer.py uses. The gatekeeper's from_json
    parser silently drops any unknown fields, so only the nine canonical fields
    are ever emitted.

Usage::

    from src.delta_guard.langchain_adapter import create_langchain_producer
    from langchain_core.tools import tool

    callback = create_langchain_producer(
        agent_id="langchain_agent_001",
        tool_name_mapping={"search": "web_scraper"},
    )

    @tool
    def search(query: str) -> str:
        return "result"

    search.invoke({"query": "python"}, config={"callbacks": [callback]})
"""

from __future__ import annotations

import json
import logging
import os
import queue
import threading
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Union
from uuid import UUID

import yaml
from langchain_core.callbacks import BaseCallbackHandler

logger = logging.getLogger(__name__)

# Contract constants (mirrors agent_contract.yaml)
CONTRACT_ALLOWED_TOOLS: List[str] = [
    "sql_query_executor",
    "vector_search",
    "web_scraper",
    "db_writer",
]
COST_PER_TOKEN: float = 0.00001
MAX_TOOL_ARGS_LENGTH: int = 2000
KAFKA_TOPIC: str = "agent-events"
KAFKA_BOOTSTRAP_SERVERS: str = "localhost:9092"
_BACKGROUND_QUEUE_MAXSIZE: int = 1000

# Path to model pricing configuration
_PRICING_CONFIG_PATH = Path(__file__).resolve().parents[2] / "configs" / "model_pricing.yaml"


def _load_model_pricing() -> Dict[str, Any]:
    """Load token pricing table from configs/model_pricing.yaml with safe defaults."""
    defaults = {
        "models": {
            "default": {"fallback_per_token": COST_PER_TOKEN}
        }
    }
    if not _PRICING_CONFIG_PATH.exists():
        return defaults
    try:
        with open(_PRICING_CONFIG_PATH, "r", encoding="utf-8") as fh:
            data = yaml.safe_load(fh)
            return data if isinstance(data, dict) else defaults
    except Exception as exc:
        logger.warning("Failed to load model pricing config (%s), using defaults", exc)
        return defaults


_MODEL_PRICING_TABLE = _load_model_pricing()


def _calculate_model_cost(
    model_name: str,
    prompt_tokens: int,
    completion_tokens: int,
    total_tokens: int,
    fallback_rate: float = COST_PER_TOKEN,
) -> float:
    """Calculate USD cost based on per-model input/output pricing per 1M tokens."""
    models_dict = _MODEL_PRICING_TABLE.get("models", {})
    # Match model name or prefix (e.g. gpt-4o-2024-08-06 matches gpt-4o)
    matched_pricing = None
    clean_name = (model_name or "").lower().strip()
    for m_key, m_pricing in models_dict.items():
        if m_key != "default" and (m_key in clean_name or clean_name.startswith(m_key)):
            matched_pricing = m_pricing
            break

    if matched_pricing and ("input_per_million" in matched_pricing or "output_per_million" in matched_pricing):
        input_rate = float(matched_pricing.get("input_per_million", 0.0)) / 1_000_000.0
        output_rate = float(matched_pricing.get("output_per_million", 0.0)) / 1_000_000.0
        calculated = (prompt_tokens * input_rate) + (completion_tokens * output_rate)
        return round(min(calculated, 50.0), 6)

    # Fallback to per-token pricing
    per_token_rate = models_dict.get("default", {}).get("fallback_per_token", fallback_rate)
    count = total_tokens if total_tokens > 0 else (prompt_tokens + completion_tokens)
    return round(min(count * per_token_rate, 50.0), 6)


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _truncate(s: str, max_len: int = MAX_TOOL_ARGS_LENGTH) -> str:
    return s[:max_len] if len(s) > max_len else s


def _build_event(
    *,
    agent_id: str,
    session_id: str,
    action_id: str,
    tool_name: str,
    execution_time_ms: int,
    cost_usd: float,
    status: str,
    tool_args: str,
) -> Dict[str, Any]:
    return {
        "agent_id": agent_id,
        "session_id": session_id,
        "action_id": action_id,
        "timestamp": _now_iso(),
        "tool_name": tool_name,
        "execution_time_ms": max(0, int(execution_time_ms)),
        "cost_usd": round(max(0.0, float(cost_usd)), 6),
        "status": status,
        "tool_args": _truncate(tool_args),
    }


class LangChainKafkaProducer:
    """Non-blocking async Kafka publisher for LangChain adapter events with drop metrics."""

    def __init__(
        self,
        bootstrap_servers: str = KAFKA_BOOTSTRAP_SERVERS,
        topic: str = KAFKA_TOPIC,
        queue_maxsize: int = _BACKGROUND_QUEUE_MAXSIZE,
    ) -> None:
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self._queue: queue.Queue[Optional[Dict[str, Any]]] = queue.Queue(maxsize=queue_maxsize)
        self._producer: Any = None
        self._dropped_events_count: int = 0
        self._sent_events_count: int = 0
        self._lock = threading.Lock()
        self._thread = threading.Thread(target=self._drain_loop, daemon=True)
        self._thread.start()

    @property
    def dropped_count(self) -> int:
        with self._lock:
            return self._dropped_events_count

    @property
    def sent_count(self) -> int:
        with self._lock:
            return self._sent_events_count

    def send(self, event: Dict[str, Any]) -> None:
        try:
            self._queue.put_nowait(event)
            with self._lock:
                self._sent_events_count += 1
        except queue.Full:
            with self._lock:
                self._dropped_events_count += 1
            logger.warning(
                "LangChainKafkaProducer: queue full, event dropped. Total dropped=%d",
                self._dropped_events_count,
            )

    def flush(self, timeout: float = 5.0) -> None:
        try:
            self._queue.join()
        except Exception:
            pass

    def close(self) -> None:
        self.flush()
        try:
            self._queue.put_nowait(None)
        except queue.Full:
            pass
        self._thread.join(timeout=5.0)
        if self._producer is not None:
            try:
                self._producer.close()
            except Exception:
                pass
        with self._lock:
            logger.info(
                "LangChainKafkaProducer closed. Total sent: %d, Total dropped: %d",
                self._sent_events_count,
                self._dropped_events_count,
            )

    def _get_producer(self) -> Any:
        if self._producer is not None:
            return self._producer
        try:
            from kafka import KafkaProducer  # type: ignore[import]
            self._producer = KafkaProducer(
                bootstrap_servers=self.bootstrap_servers,
                value_serializer=lambda v: v.encode("utf-8") if isinstance(v, str) else json.dumps(v).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8") if k else None,
            )
            logger.info("LangChainKafkaProducer connected to %s", self.bootstrap_servers)
        except Exception as exc:
            logger.warning("LangChainKafkaProducer: Kafka unavailable -- %s", exc)
            self._producer = None
        return self._producer

    def _drain_loop(self) -> None:
        while True:
            try:
                item = self._queue.get(timeout=0.1)
                if item is None:
                    self._queue.task_done()
                    break
                producer = self._get_producer()
                if producer is not None:
                    try:
                        agent_id = item.get("agent_id", "unknown") if isinstance(item, dict) else "unknown"
                        producer.send(self.topic, key=agent_id, value=item)
                    except Exception as exc:
                        logger.warning("Kafka send failed: %s", exc)
                self._queue.task_done()
            except queue.Empty:
                continue
            except Exception as exc:
                logger.warning("LangChainKafkaProducer drain error: %s", exc)


class LangChainEventCallback(BaseCallbackHandler):
    """LangChain BaseCallbackHandler that emits canonical Agentic Delta Guard events."""

    def __init__(
        self,
        agent_id: str,
        kafka_producer: LangChainKafkaProducer,
        tool_name_mapping: Optional[Dict[str, str]] = None,
        strict: bool = True,
        cost_per_token: float = COST_PER_TOKEN,
        max_tool_args_length: int = MAX_TOOL_ARGS_LENGTH,
        session_id: Optional[str] = None,
    ) -> None:
        super().__init__()
        self.agent_id = agent_id
        self._producer = kafka_producer
        self.tool_name_mapping: Dict[str, str] = tool_name_mapping or {}
        self.strict = strict
        self.cost_per_token = cost_per_token
        self.max_tool_args_length = max_tool_args_length
        self._explicit_session_id = session_id
        self._session_id: Optional[str] = session_id
        self._tool_start_times: Dict[str, float] = {}
        self._tool_names: Dict[str, str] = {}
        self._tool_args: Dict[str, str] = {}

    @property
    def dropped_events_count(self) -> int:
        return getattr(self._producer, "dropped_count", 0)

    def on_chain_start(
        self,
        serialized: Dict[str, Any],
        inputs: Dict[str, Any],
        *,
        run_id: UUID,
        parent_run_id: Optional[UUID] = None,
        tags: Optional[List[str]] = None,
        metadata: Optional[Dict[str, Any]] = None,
        **kwargs: Any,
    ) -> None:
        if metadata and "session_id" in metadata:
            self._session_id = str(metadata["session_id"])
        elif isinstance(inputs, dict) and "session_id" in inputs:
            self._session_id = str(inputs["session_id"])
        elif self._session_id is None:
            self._session_id = str(run_id) if parent_run_id is None else str(uuid.uuid4())

    def on_chain_end(
        self,
        outputs: Dict[str, Any],
        *,
        run_id: UUID,
        parent_run_id: Optional[UUID] = None,
        **kwargs: Any,
    ) -> None:
        if parent_run_id is None:
            self._session_id = self._explicit_session_id

    def on_tool_start(
        self,
        serialized: Dict[str, Any],
        input_str: str,
        *,
        run_id: UUID,
        parent_run_id: Optional[UUID] = None,
        tags: Optional[List[str]] = None,
        metadata: Optional[Dict[str, Any]] = None,
        inputs: Optional[Dict[str, Any]] = None,
        **kwargs: Any,
    ) -> None:
        run_id_str = str(run_id)
        self._tool_start_times[run_id_str] = time.perf_counter()
        name = (
            kwargs.get("name")
            or (serialized.get("name") if isinstance(serialized, dict) else None)
            or "unknown_tool"
        )
        self._tool_names[run_id_str] = name
        raw_args = inputs if inputs is not None else input_str
        if isinstance(raw_args, (dict, list)):
            tool_args_str = json.dumps(raw_args)
        else:
            tool_args_str = str(raw_args)
        self._tool_args[run_id_str] = tool_args_str

    def on_tool_end(
        self,
        output: Any,
        *,
        run_id: UUID,
        parent_run_id: Optional[UUID] = None,
        **kwargs: Any,
    ) -> None:
        run_id_str = str(run_id)
        start_t = self._tool_start_times.pop(run_id_str, None)
        elapsed_ms = int((time.perf_counter() - start_t) * 1000) if start_t else 0
        raw_name = kwargs.get("name") or self._tool_names.pop(run_id_str, "unknown_tool")
        tool_name = self._resolve_tool_name(raw_name)
        session_id = self._ensure_session_id()
        
        input_str = self._tool_args.pop(run_id_str, "")
        output_str = str(output)
        tool_args_data = {"input": input_str, "output": output_str} if input_str else {"output": output_str}
        tool_args_str = _truncate(json.dumps(tool_args_data), self.max_tool_args_length)

        event = _build_event(
            agent_id=self.agent_id,
            session_id=session_id,
            action_id=run_id_str,
            tool_name=tool_name,
            execution_time_ms=elapsed_ms,
            cost_usd=0.0,
            status="SUCCESS",
            tool_args=tool_args_str,
        )
        self._emit(event)

    def on_tool_error(
        self,
        error: BaseException,
        *,
        run_id: UUID,
        parent_run_id: Optional[UUID] = None,
        **kwargs: Any,
    ) -> None:
        run_id_str = str(run_id)
        start_t = self._tool_start_times.pop(run_id_str, None)
        elapsed_ms = int((time.perf_counter() - start_t) * 1000) if start_t else 0
        raw_name = kwargs.get("name") or self._tool_names.pop(run_id_str, "unknown_tool")
        tool_name = self._resolve_tool_name(raw_name)
        session_id = self._ensure_session_id()

        input_str = self._tool_args.pop(run_id_str, "")
        tool_args_data = {"input": input_str, "error": str(error)} if input_str else {"error": str(error)}
        tool_args_str = _truncate(json.dumps(tool_args_data), self.max_tool_args_length)

        event = _build_event(
            agent_id=self.agent_id,
            session_id=session_id,
            action_id=run_id_str,
            tool_name=tool_name,
            execution_time_ms=elapsed_ms,
            cost_usd=0.0,
            status="ERROR",
            tool_args=tool_args_str,
        )
        self._emit(event)

    def on_llm_end(
        self,
        response: Any,
        *,
        run_id: UUID,
        parent_run_id: Optional[UUID] = None,
        **kwargs: Any,
    ) -> None:
        total_tokens = 0
        prompt_tokens = 0
        completion_tokens = 0
        model_name = ""

        try:
            # 1. Extract model name
            llm_output = getattr(response, "llm_output", {}) or {}
            if isinstance(llm_output, dict):
                model_name = (
                    llm_output.get("model_name")
                    or llm_output.get("model")
                    or kwargs.get("name", "")
                )
                token_usage = llm_output.get("token_usage") or llm_output.get("usage") or {}
                if isinstance(token_usage, dict):
                    prompt_tokens = int(token_usage.get("prompt_tokens", 0) or token_usage.get("input_tokens", 0))
                    completion_tokens = int(token_usage.get("completion_tokens", 0) or token_usage.get("output_tokens", 0))
                    total_tokens = int(token_usage.get("total_tokens", 0) or (prompt_tokens + completion_tokens))

            # 2. Extract standard usage_metadata on modern LangChain messages/generations
            if not total_tokens and hasattr(response, "generations") and response.generations:
                for gen_list in response.generations:
                    for gen in gen_list:
                        msg = getattr(gen, "message", None)
                        if msg and hasattr(msg, "usage_metadata") and msg.usage_metadata:
                            u = msg.usage_metadata
                            prompt_tokens = int(u.get("input_tokens", 0))
                            completion_tokens = int(u.get("output_tokens", 0))
                            total_tokens = int(u.get("total_tokens", 0) or (prompt_tokens + completion_tokens))
                        if not model_name and hasattr(gen, "generation_info") and gen.generation_info:
                            model_name = gen.generation_info.get("model_name", "")
        except Exception as exc:
            logger.debug("Failed parsing LLM token usage metadata: %s", exc)

        if total_tokens > 0 or prompt_tokens > 0 or completion_tokens > 0:
            cost_usd = _calculate_model_cost(
                model_name=model_name,
                prompt_tokens=prompt_tokens,
                completion_tokens=completion_tokens,
                total_tokens=total_tokens,
                fallback_rate=self.cost_per_token,
            )
            session_id = self._ensure_session_id()
            meta_args = {
                "model": model_name or "unknown",
                "prompt_tokens": prompt_tokens,
                "completion_tokens": completion_tokens,
                "total_tokens": total_tokens or (prompt_tokens + completion_tokens),
            }
            tool_args_str = _truncate(json.dumps(meta_args), self.max_tool_args_length)
            event = _build_event(
                agent_id=self.agent_id,
                session_id=session_id,
                action_id=str(run_id),
                tool_name="vector_search",
                execution_time_ms=0,
                cost_usd=cost_usd,
                status="SUCCESS",
                tool_args=tool_args_str,
            )
            self._emit(event)

    def on_agent_finish(
        self,
        finish: Any,
        *,
        run_id: UUID,
        parent_run_id: Optional[UUID] = None,
        **kwargs: Any,
    ) -> None:
        session_id = self._ensure_session_id()
        output_str = ""
        try:
            output_str = str(getattr(finish, "return_values", {}) or finish)
        except Exception:
            pass
        tool_args_str = _truncate(json.dumps({"agent_output": output_str[:200]}), self.max_tool_args_length)
        event = _build_event(
            agent_id=self.agent_id,
            session_id=session_id,
            action_id=str(run_id),
            tool_name="db_writer",
            execution_time_ms=0,
            cost_usd=0.0,
            status="SUCCESS",
            tool_args=tool_args_str,
        )
        self._emit(event)

    def _resolve_tool_name(self, lc_name: str) -> str:
        if lc_name in self.tool_name_mapping:
            return self.tool_name_mapping[lc_name]
        if lc_name in CONTRACT_ALLOWED_TOOLS:
            return lc_name
        return lc_name

    def _ensure_session_id(self) -> str:
        if self._session_id is None:
            self._session_id = str(uuid.uuid4())
        return self._session_id

    def _emit(self, event: Dict[str, Any]) -> None:
        logger.debug("LangChainEventCallback emit: %s", event)
        self._producer.send(event)


def create_langchain_producer(
    agent_id: str = "langchain_agent_001",
    bootstrap_servers: str = KAFKA_BOOTSTRAP_SERVERS,
    topic: str = KAFKA_TOPIC,
    tool_name_mapping: Optional[Dict[str, str]] = None,
    strict: bool = True,
    cost_per_token: float = COST_PER_TOKEN,
    max_tool_args_length: int = MAX_TOOL_ARGS_LENGTH,
    queue_maxsize: int = _BACKGROUND_QUEUE_MAXSIZE,
    session_id: Optional[str] = None,
) -> LangChainEventCallback:
    """Factory: returns a configured LangChainEventCallback with a running Kafka publisher."""
    kafka_producer = LangChainKafkaProducer(
        bootstrap_servers=bootstrap_servers,
        topic=topic,
        queue_maxsize=queue_maxsize,
    )
    return LangChainEventCallback(
        agent_id=agent_id,
        kafka_producer=kafka_producer,
        tool_name_mapping=tool_name_mapping,
        strict=strict,
        cost_per_token=cost_per_token,
        max_tool_args_length=max_tool_args_length,
        session_id=session_id,
    )
