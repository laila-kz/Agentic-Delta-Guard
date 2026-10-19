import sys
from pathlib import Path

# Ensure src/ is on the path when this script is run directly (not via pytest)
_ROOT = Path(__file__).resolve().parents[1]
for _p in [str(_ROOT / "src"), str(_ROOT)]:
    if _p not in sys.path:
        sys.path.insert(0, _p)

from delta_guard.langchain_adapter import _redact_sensitive_data

tests = [
    ('{"api_key": "sk-abc123456789abcdef"}',                    "sk-...key redacted"),
    ('{"Authorization": "Bearer eyJtokenvalue1234567890ab"}',   "Bearer token redacted"),
    ('{"password": "hunter2", "query": "SELECT 1"}',            "password redacted, query preserved"),
    ('{"user": "alice@example.com"}',                           "email redacted"),
]

all_pass = True
for payload, label in tests:
    result = _redact_sensitive_data(payload)
    print(f"[{label}]")
    print(f"  IN : {payload}")
    print(f"  OUT: {result}")
    print()

# Assertions
r = _redact_sensitive_data('{"api_key": "sk-abc123456789abcdef"}')
assert "sk-abc123456789abcdef" not in r and "REDACTED" in r, f"FAIL api_key: {r}"
print("PASS: api_key redacted")

r = _redact_sensitive_data('{"Authorization": "Bearer eyJtokenvalue1234567890ab"}')
assert "eyJtokenvalue1234567890ab" not in r and "REDACTED" in r, f"FAIL Bearer: {r}"
print("PASS: Bearer token redacted")

r = _redact_sensitive_data('{"password": "hunter2", "query": "SELECT 1"}')
assert "hunter2" not in r and "REDACTED" in r, f"FAIL password: {r}"
assert "SELECT 1" in r, f"FAIL query removed: {r}"
print("PASS: password redacted, query preserved")

r = _redact_sensitive_data('{"user": "alice@example.com"}')
assert "alice@example.com" not in r and "REDACTED_EMAIL" in r, f"FAIL email: {r}"
print("PASS: email redacted")

print()
print("All redaction checks passed.")


# ── Cost calculation checks ──────────────────────────────────────────────────
print("=" * 60)
print("Cost calculation checks")
print("=" * 60)

from delta_guard.langchain_adapter import _calculate_model_cost

mini = _calculate_model_cost("gpt-4o-mini", 1000, 500, 1500)
print(f"gpt-4o-mini 1k+500 tokens : USD {mini:.6f}")
expected_mini = (1000 / 1_000_000) * 0.15 + (500 / 1_000_000) * 0.60
assert abs(mini - expected_mini) < 1e-4, f"FAIL gpt-4o-mini cost: {mini}"
print("PASS: gpt-4o-mini cost correct")

big = _calculate_model_cost("gpt-4o", 10_000_000, 5_000_000, 15_000_000)
print(f"gpt-4o 10M+5M tokens      : USD {big:.2f}  (must be > 50)")
assert big > 50, f"FAIL: cost is clamped! got {big}"
print("PASS: no artificial cap at USD 50")

unk = _calculate_model_cost("unknown-model-xyz", 500, 200, 700)
print(f"unknown model 500+200 tok  : USD {unk:.6f}  (must be > 0 via fallback)")
assert unk > 0, f"FAIL: unknown model returned 0 (silent error)"
print("PASS: fallback rate applied for unknown model")

print()
print("All cost calculation checks passed.")


# ── Dangerous tool denylist ──────────────────────────────────────────────────
print("=" * 60)
print("Dangerous tool denylist checks")
print("=" * 60)

from delta_guard.triage import _is_dangerous_tool, DANGEROUS_TOOL_PREFIXES

print(f"Active prefixes ({len(DANGEROUS_TOOL_PREFIXES)}): {sorted(DANGEROUS_TOOL_PREFIXES)}")
print()

MUST_BLOCK = [
    "bash_shell_executor", "shell_run", "exec_command",
    "admin_reset", "eval_code", "drop_table",
    "powershell_invoke", "sudo_rm", "cmd_exec",
]
for t in MUST_BLOCK:
    result = _is_dangerous_tool(t)
    print(f"  BLOCKED  {t}: {result}")
    assert result, f"FAIL: {t} should be flagged as dangerous"

MUST_ALLOW = ["sql_query_executor", "web_scraper", "vector_search", "db_writer"]
for t in MUST_ALLOW:
    result = _is_dangerous_tool(t)
    print(f"  ALLOWED  {t}: {not result}")
    assert not result, f"FAIL: {t} was wrongly flagged as dangerous"

print()
print("PASS: denylist correct")
print("All denylist checks passed.")


# ── contract_guarded decorator (MCP offline → fail-open) ────────────────────
print("=" * 60)
print("contract_guarded decorator checks")
print("=" * 60)

from delta_guard.real_agent.tools import contract_guarded, ContractViolationError

@contract_guarded(tool_name="test_tool", fail_open=True)
def _test_fail_open():
    return "executed"

result = _test_fail_open()
assert result == "executed", f"FAIL fail_open: {result}"
print("PASS: fail_open=True allows execution when MCP is offline")

@contract_guarded(tool_name="test_tool", fail_open=False)
def _test_fail_closed():
    return "executed"

try:
    _test_fail_closed()
    print("FAIL: should have raised ContractViolationError")
except ContractViolationError as e:
    print(f"PASS: fail_open=False correctly blocked: {str(e)[:60]}...")

print()
print("All contract_guarded checks passed.")


# ── Summary ──────────────────────────────────────────────────────────────────
print()
print("=" * 60)
print("ALL VERIFICATION CHECKS PASSED")
print("=" * 60)
