"""How the tools reach an API: through the gateway, over HTTPS, with its password.

Reads GATEWAY_USER and GATEWAY_PASSWORD from the environment, and trusts the
local CA saved at the repository root as local-ca.crt.
"""

import os
import ssl
from pathlib import Path

CA = Path(__file__).parents[2] / "local-ca.crt"


def client_options(host: str) -> dict:
    return {
        "base_url": f"https://{host}",
        "auth": (os.environ["GATEWAY_USER"], os.environ["GATEWAY_PASSWORD"]),
        "verify": ssl.create_default_context(cafile=str(CA)),
    }
