"""Start Jupyter without placing its token in the process command line."""

import os
from pathlib import Path


if __name__ == "__main__":
    token = os.environ.get("JUPYTER_TOKEN", "").strip()
    if not token:
        raise SystemExit("JUPYTER_TOKEN must be set in .env.")
    config = Path.home() / ".jupyter" / "jupyter_server_config.py"
    config.parent.mkdir(parents=True, exist_ok=True)
    config.write_text(
        "import os\n"
        "c.IdentityProvider.token = os.environ['JUPYTER_TOKEN']\n"
        "c.ServerApp.ip = '0.0.0.0'\n"
        "c.ServerApp.port = 8888\n"
        "c.ServerApp.port_retries = 0\n"
        "c.ServerApp.open_browser = False\n"
        "c.ServerApp.root_dir = '/workspace'\n",
        encoding="utf-8",
    )
    os.execvp("jupyter", ["jupyter", "lab", "--config", str(config)])
