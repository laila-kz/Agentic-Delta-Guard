#!/usr/bin/env python
"""
run_mcp_server.py
Entrypoint script for launching the Agentic Delta Guard MCP Server via Gemini CLI / stdio.
"""

import os
import sys
from pathlib import Path

# Add project root to sys.path
PROJECT_ROOT = Path(__file__).resolve().parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.delta_guard.mcp.server import mcp

if __name__ == "__main__":
    try:
        mcp.run()
    except KeyboardInterrupt:
        print("\n[OK] MCP server stopped cleanly.", file=sys.stderr)
