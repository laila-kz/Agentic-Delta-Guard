#!/usr/bin/env python
"""
record_mcp_session.py - Alias for running the MCP Client Demo.
"""
import sys
import subprocess
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
TARGET_SCRIPT = PROJECT_ROOT / "examples" / "mcp_client_demo.py"

if __name__ == "__main__":
    result = subprocess.run([sys.executable, str(TARGET_SCRIPT)] + sys.argv[1:])
    sys.exit(result.returncode)
