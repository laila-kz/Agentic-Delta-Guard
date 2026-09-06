from pathlib import Path
import subprocess
import sys


PROJECT_DIR = Path(__file__).resolve().parents[2] / "dbt_delta_guard"


def main() -> int:
    result = subprocess.run(
        ["dbt", "test", "--profiles-dir", "."],
        cwd=PROJECT_DIR,
        check=False,
    )

    if result.returncode == 0:
        print("dbt test completed successfully.")
        return 0

    print(f"dbt test failed with exit code {result.returncode}.")
    return 1


if __name__ == "__main__":
    sys.exit(main())
