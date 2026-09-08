"""Register the `pems-quarto` Jupyter kernel used by reports/pems_quality.

It's a copy of the default `python3` kernel that points IPYTHONDIR at a
private, empty profile (inside .venv, next to this kernelspec) instead of
the user's real `~/.ipython`. This avoids the Databricks VS Code extension's
global IPython startup script, which registers an input transformer that
breaks Quarto's plain (non-Databricks) code execution.

Run once after `uv sync` (and again any time .venv is recreated):

    uv run python scripts/setup_quarto_kernel.py
"""

import json
import sys
from pathlib import Path

venv_root = Path(sys.prefix)
kernel_dir = venv_root / "share" / "jupyter" / "kernels" / "pems-quarto"
profile_dir = venv_root / ".ipython_quarto"

kernel_dir.mkdir(parents=True, exist_ok=True)
profile_dir.mkdir(parents=True, exist_ok=True)

kernel_json = {
    "argv": ["python", "-m", "ipykernel_launcher", "-f", "{connection_file}"],
    "env": {"IPYTHONDIR": str(profile_dir)},
    "display_name": "Python 3 (PeMS Quarto)",
    "language": "python",
    "metadata": {"debugger": True, "supported_encryption": "curve"},
    "kernel_protocol_version": "5.5",
}

(kernel_dir / "kernel.json").write_text(json.dumps(kernel_json, indent=1))
print(f"Registered pems-quarto kernel at {kernel_dir}")
