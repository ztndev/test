import json
import socket
import subprocess
from datetime import UTC, datetime
from typing import Any, Dict
from urllib.error import URLError
from urllib.request import Request, urlopen

from prefect import flow, task


@task(name="gather_system_info")
def gather_system_info() -> Dict[str, Any]:
    """Gather Ubuntu system information using standard library only"""

    hostname = socket.gethostname()
    timestamp = datetime.now(UTC)

    audit_data = {"hostname": hostname, "timestamp": timestamp, "checks": {}}

    # Define Ubuntu-specific commands
    commands = {
        "os_version": ["lsb_release", "-a"],
        "kernel_version": ["uname", "-r"],
        "uptime": ["uptime", "-p"],
        "disk_usage": ["df", "-h"],
        "memory_info": ["free", "-h"],
        "cpu_info": ["lscpu"],
        "running_services": [
            "systemctl",
            "list-units",
            "--type=service",
            "--state=running",
        ],
        "failed_services": ["systemctl", "--failed"],
        "last_logins": ["last", "-n", "10"],
        "listening_ports": ["ss", "-tuln"],
        "installed_packages": ["dpkg", "-l"],
        "security_updates": ["apt", "list", "--upgradable"],
        "load_average": ["cat", "/proc/loadavg"],
        "network_interfaces": ["ip", "addr", "show"],
    }

    # Execute commands
    for check_name, cmd in commands.items():
        try:
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
            audit_data["checks"][check_name] = {
                "status": "success" if result.returncode == 0 else "failed",
                "output": result.stdout,
                "error": result.stderr if result.returncode != 0 else None,
                "return_code": result.returncode,
            }
        except subprocess.TimeoutExpired:
            audit_data["checks"][check_name] = {
                "status": "timeout",
                "error": "Command timed out after 30 seconds",
            }
        except FileNotFoundError:
            audit_data["checks"][check_name] = {
                "status": "not_found",
                "error": f"Command not found: {cmd[0]}",
            }
        except Exception as e:
            audit_data["checks"][check_name] = {"status": "error", "error": str(e)}

    return audit_data


@task(name="send_to_analytics")
def send_to_analytics(audit_data: Dict[str, Any], webhook_url: str) -> bool:
    """Send audit data to central analytics via webhook using standard library"""

    try:
        # Prepare the request
        data = json.dumps(audit_data).encode("utf-8")

        req = Request(
            webhook_url,
            data=data,
            headers={
                "Content-Type": "application/json",
                "User-Agent": "Prefect-Audit-System/1.0",
            },
            method="POST",
        )

        # Send the request
        with urlopen(req, timeout=30) as response:
            status_code = response.getcode()

            if 200 <= status_code < 300:
                return True
            else:
                raise Exception(f"Webhook returned status code: {status_code}")

    except URLError as e:
        raise Exception(f"Failed to send data to webhook: {e}")
    except Exception as e:
        raise Exception(f"Error sending audit data: {e}")


@flow(name="system_audit", log_prints=True)
def system_audit_flow():
    """Main audit flow that gathers system info and sends to analytics"""

    print(f"Starting system audit on {socket.gethostname()}")

    # Gather system information
    audit_data = gather_system_info()

    print(f"Collected {len(audit_data['checks'])} system checks")
    print(audit_data)
    # Send to analytics
    success = send_to_analytics(
        audit_data, "https://webhook.site/3517ded4-3143-4d33-897e-fa5f340a7cfd"
    )

    if success:
        print("Audit data successfully sent to analytics")
    else:
        print("Failed to send audit data")

    return audit_data
