#!/usr/bin/env python3
"""Ansible inventory read from Terraform: the stores VM and its addresses.

The VM is created again every session with a new IP, so the inventory comes
from the stores-vm stack's outputs rather than from a file.
"""

import json
import subprocess
import sys
from pathlib import Path

TF = Path(__file__).resolve().parents[2] / "terraform" / "tf.sh"


def outputs() -> dict:
    raw = subprocess.run(
        [str(TF), "stores-vm", "output", "-json"], check=True, capture_output=True, text=True
    ).stdout
    return {name: item["value"] for name, item in json.loads(raw).items()}


def main() -> None:
    if len(sys.argv) > 1 and sys.argv[1] == "--host":
        print("{}")
        return
    out = outputs()
    host = out["name"]
    print(json.dumps({
        "stores": {"hosts": [host]},
        "_meta": {"hostvars": {host: {
            "ansible_host": out["external_ip"],
            "ansible_user": "fraudstream",
            "stores_internal_dns": out["internal_dns"],
            "models_bucket": out["models_bucket"],
        }}},
    }))


if __name__ == "__main__":
    main()
