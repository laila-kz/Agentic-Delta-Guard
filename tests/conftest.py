import os
import sys
from pathlib import Path

# Ensure src/ and project root are on the path for all test sessions,
# even when pytest is invoked from the system Python without editable install.
_ROOT = Path(__file__).resolve().parent.parent
for _p in [str(_ROOT / "src"), str(_ROOT)]:
    if _p not in sys.path:
        sys.path.insert(0, _p)


def pytest_configure(config):
    """
    Automatically load environment variables from workspace .env file if present,
    and configure PySpark Python executable for Windows environments.
    """
    if sys.platform == "win32":
        os.environ["PYSPARK_PYTHON"] = sys.executable
        os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

    env_file = _ROOT / ".env"
    if env_file.exists():
        with open(env_file, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#") and "=" in line:
                    key, val = line.split("=", 1)
                    key = key.strip()
                    val = val.strip().strip("'\"")
                    if key and key not in os.environ:
                        os.environ[key] = val


def pytest_collect_file(parent, file_path):
    """
    Do nothing here — collection is handled by pytest's normal machinery.
    We rely on per-file importorskip guards (see collect_ignore below).
    """
    return None


def pytest_ignore_collect(collection_path, config):
    """
    Skip collection of test files whose required optional dependency is absent,
    turning hard ImportErrors into clean SKIP notices.

    File → required top-level package
    """
    _optional_deps: dict[str, str] = {
        "test_langchain_adapter.py":  "langchain_core",
        "test_real_agent.py":         "langchain_core",
        "test_chaos_infra.py":        "deltalake",
        "test_contract_validation.py":"deltalake",
        "test_delta_clone_utils.py":  "deltalake",
        "test_mcp_server.py":         "mcp",
        "test_mcp_stdio.py":          "mcp",
    }
    name = collection_path.name
    if name in _optional_deps:
        dep = _optional_deps[name]
        try:
            __import__(dep)
        except ImportError:
            # Print a visible warning instead of a silent skip
            import warnings
            warnings.warn(
                f"Skipping {name}: optional dependency '{dep}' is not installed. "
                f"Run: pip install {dep}",
                stacklevel=1,
            )
            return True   # tells pytest to ignore this file
    return None
