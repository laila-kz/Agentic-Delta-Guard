"""
src/delta_guard/real_agent/contract_guard.py
Preventive MCP pre-flight decorator — zero heavy dependencies.

This module is intentionally kept free of langchain, pyspark, deltalake,
and any other optional package so it can be imported in any context
(scripts, tests, tools, verification utilities) without those packages
being installed.
"""

from __future__ import annotations

import functools
import json
import logging
from typing import Any, Callable

logger = logging.getLogger(__name__)


class ContractViolationError(RuntimeError):
    """Raised when a contract_guarded tool is blocked by the MCP pre-flight check."""


def contract_guarded(tool_name: str | None = None, *, fail_open: bool = True) -> Callable:
    """Decorator that performs a synchronous MCP pre-flight check before the wrapped
    tool body executes.

    This is the *preventive* control layer described in the architecture:
      - If the MCP server is reachable and returns ``{"allowed": false}``, the call
        is BLOCKED immediately and a ContractViolationError is raised.
      - If the MCP server is unreachable the decorator defaults to WARN-AND-ALLOW
        (``fail_open=True``) to avoid blocking the agent on infrastructure downtime.
      - Pass ``fail_open=False`` to hard-block on any MCP communication failure.

    The asynchronous Gatekeeper / Kafka pipeline acts as the independent detective layer.

    Args:
        tool_name:  The contract tool name to send to MCP. Defaults to the function name.
        fail_open:  If True (default), allow execution when MCP is unreachable.
                    If False, block execution on any MCP communication error.
    """
    def decorator(fn: Callable) -> Callable:
        resolved_name = tool_name or fn.__name__

        @functools.wraps(fn)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            try:
                import urllib.request as _urllib_request

                mcp_url = "http://localhost:8000/check_contract"
                payload = json.dumps({"tool_name": resolved_name}).encode()
                req = _urllib_request.Request(
                    mcp_url,
                    data=payload,
                    headers={"Content-Type": "application/json"},
                    method="POST",
                )
                with _urllib_request.urlopen(req, timeout=2) as resp:
                    result = json.loads(resp.read().decode())

                if not result.get("allowed", True):
                    reason = result.get("reason", "denied by contract")
                    logger.error(
                        "[contract_guarded] BLOCKED tool='%s' reason='%s'",
                        resolved_name,
                        reason,
                    )
                    raise ContractViolationError(
                        f"Tool '{resolved_name}' blocked by MCP pre-flight check: {reason}"
                    )

                logger.debug("[contract_guarded] ALLOWED tool='%s'", resolved_name)

            except ContractViolationError:
                raise  # propagate the hard block — never swallow it

            except Exception as exc:  # MCP unreachable or unexpected network error
                if fail_open:
                    logger.warning(
                        "[contract_guarded] MCP unreachable for tool='%s', failing open: %s",
                        resolved_name,
                        exc,
                    )
                else:
                    logger.error(
                        "[contract_guarded] MCP unreachable for tool='%s', failing CLOSED: %s",
                        resolved_name,
                        exc,
                    )
                    raise ContractViolationError(
                        f"Tool '{resolved_name}' blocked: MCP pre-flight check unavailable "
                        f"(fail_open=False)"
                    ) from exc

            return fn(*args, **kwargs)

        return wrapper
    return decorator
