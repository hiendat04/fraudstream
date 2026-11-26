#!/usr/bin/env python3
"""Ansible inventory read from Terraform: the platform VM, its addresses, the gateway and the registry.

The VM is created again every session with a new IP, so the inventory comes
from the platform-vm stack's outputs rather than from a file.

Ansible runs it; to see what it returns, from the repo root:

    ansible-inventory -i infra/ansible/inventory/terraform.py --list
"""

import json
import subprocess
import sys
from pathlib import Path

TF = Path(__file__).resolve().parents[2] / "terraform" / "tf.sh"


def outputs(stack: str) -> dict:
    raw = subprocess.run(
        [str(TF), stack, "output", "-json"], check=True, capture_output=True, text=True
    ).stdout
    return {name: item["value"] for name, item in json.loads(raw).items()}


def main() -> None:
    if len(sys.argv) > 1 and sys.argv[1] == "--host":
        print("{}")
        return
    out = outputs("platform-vm")
    host = out["name"]
    print(json.dumps({
        "platform": {"hosts": [host]},
        "_meta": {"hostvars": {host: {
            "ansible_host": out["external_ip"],
            "ansible_user": "fraudstream",
            "internal_ip": out["internal_ip"],
            "gateway_ip": outputs("network")["gateway_ip"],
            "repository_url": outputs("registry")["repository_url"],
        }}},
    }))


if __name__ == "__main__":
    main()
