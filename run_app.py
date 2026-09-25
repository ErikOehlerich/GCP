#!/usr/bin/env python3
from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
VENV_DIR = ROOT / ".venv"
PYTHON_FILE = "python.exe" if os.name == "nt" else "python"
VENV_PYTHON = VENV_DIR / ("Scripts" if os.name == "nt" else "bin") / PYTHON_FILE


def ensure_venv() -> None:
    if VENV_PYTHON.exists():
        return

    subprocess.run([sys.executable, "-m", "venv", str(VENV_DIR)], check=True)


def ensure_requirements() -> None:
    ensure_venv()
    subprocess.run([str(VENV_PYTHON), "-m", "pip", "install", "--upgrade", "pip"], check=True)
    subprocess.run([str(VENV_PYTHON), "-m", "pip", "install", "-r", str(ROOT / "requirements.txt")], check=True)


def run_app() -> int:
    ensure_requirements()
    return subprocess.call([str(VENV_PYTHON), str(ROOT / "csv_searcher.py")])


def check_setup() -> int:
    ensure_requirements()
    print(f"Project root: {ROOT}")
    print(f"Virtualenv: {VENV_PYTHON}")
    print("Environment OK")
    return 0


def main() -> int:
    if len(sys.argv) > 1 and sys.argv[1] in {"--check", "-c"}:
        return check_setup()
    return run_app()


if __name__ == "__main__":
    sys.exit(main())
