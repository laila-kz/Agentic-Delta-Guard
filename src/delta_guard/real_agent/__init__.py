"""
src/delta_guard/real_agent/__init__.py
Autonomous Agent module for Agentic Delta Guard.
"""

from delta_guard.real_agent.contract_guard import (
    ContractViolationError,
    contract_guarded,
)

__all__ = ["ContractViolationError", "contract_guarded"]
