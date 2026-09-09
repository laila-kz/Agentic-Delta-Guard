"""
src/delta_guard/mcp/contract_validator.py
Re-exports InFlightContractValidator and ContractValidator for compatibility.
"""

from src.delta_guard.mcp.validators.contract_validator import (
    ContractValidator,
    InFlightContractValidator,
)

__all__ = ["ContractValidator", "InFlightContractValidator"]
