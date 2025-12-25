import json
import os
import socket
import subprocess
import csv
import xml.etree.ElementTree as ET
import gzip
import base64
from datetime import UTC, datetime
from typing import Any, Dict, Optional, List
from urllib.error import URLError, HTTPError
from urllib.request import Request, urlopen
from enum import Enum
from pathlib import Path
import hashlib

from prefect import flow, task


class ExportFormat(Enum):
    """Supported export formats"""
    JSON = "json"
    CSV = "csv"
    XML = "xml"
    YAML = "yaml"
    HTML = "html"
    MARKDOWN = "md"


class TransmissionMethod(Enum):
    """Supported transmission methods"""
    WEBHOOK = "webhook"
    FILE = "file"
    S3 = "s3"
    FTP = "ftp"
    EMAIL = "email"
    SYSLOG = "syslog"


class CompressionMethod(Enum):
    """Supported compression methods"""
    NONE = "none"
    GZIP = "gzip"
    BASE64 = "base64"


def _json_serializer(value: Any) -> Any:
    """Convert unsupported objects to JSON-serializable data."""
    if isinstance(value, datetime):
        return value.isoformat()
    msg = f"Object of type {value.__class__.__name__} is not JSON serializable"
    raise TypeError(msg)


def calculate_size(data: str) -> int:
    """Calculate size of data in bytes"""
    return len(data.encode('utf-8'))


def format_size(size_bytes: int) -> str:
    """Format bytes to human-readable format"""
    for unit in ['B', 'KB', 'MB', 'GB']:
        if size_bytes < 1024.0:
            return f"{size_bytes:.2f} {unit}"
        size_bytes /= 1024.0
    return f"{size_bytes:.2f} TB"


@task(name="gather_system_info")
def gather_system_info() -> Dict[str, Any]:
    """Gather comprehensive Ubuntu system information"""
    hostname = socket.gethostname()
    timestamp = datetime.now(UTC)
    audit_data = {"hostname": hostname, "timestamp": timestamp, "checks": {}}

    # Comprehensive audit commands organized by category
    commands = {
        # ============ SYSTEM INFORMATION ============
        "os_version": ["lsb_release", "-a"],
        "kernel_version": ["uname", "-r"],
        "kernel_full_info": ["uname", "-a"],
        "hostname_info": ["hostnamectl"],
        "machine_id": ["cat", "/etc/machine-id"],
        "uptime": ["uptime", "-p"],
        "uptime_detailed": ["uptime"],
        "system_boot_time": ["who", "-b"],
        "last_reboot": ["last", "reboot", "-F"],
        "dmesg_kernel_messages": ["dmesg", "-T"],
        "system_load": ["cat", "/proc/loadavg"],
        
        # ============ HARDWARE INFORMATION ============
        "cpu_info": ["lscpu"],
        "cpu_details": ["cat", "/proc/cpuinfo"],
        "memory_info": ["free", "-h"],
        "memory_detailed": ["cat", "/proc/meminfo"],
        "swap_info": ["swapon", "--show"],
        "disk_usage": ["df", "-h"],
        "disk_usage_inodes": ["df", "-i"],
        "block_devices": ["lsblk", "-a"],
        "block_device_info": ["blkid"],
        "disk_io_stats": ["iostat"],
        "hardware_info": ["lshw", "-short"],
        "pci_devices": ["lspci", "-v"],
        "usb_devices": ["lsusb", "-v"],
        "dmidecode_hardware": ["dmidecode", "-t", "system"],
        "bios_info": ["dmidecode", "-t", "bios"],
        
        # ============ USER & AUTHENTICATION ============
        "all_users": ["cat", "/etc/passwd"],
        "all_groups": ["cat", "/etc/group"],
        "shadow_file_check": ["ls", "-la", "/etc/shadow"],
        "current_logged_users": ["w"],
        "who_logged_in": ["who", "-a"],
        "last_logins": ["last", "-n", "50"],
        "last_failed_logins": ["lastb", "-n", "50"],
        "sudo_users": ["getent", "group", "sudo"],
        "users_with_uid_0": ["awk", "-F:", "($3 == 0) {print}", "/etc/passwd"],
        "users_without_password": ["awk", "-F:", "($2 == \"\") {print $1}", "/etc/shadow"],
        "password_policies": ["cat", "/etc/login.defs"],
        "pam_config": ["cat", "/etc/pam.d/common-auth"],
        "sudoers_config": ["cat", "/etc/sudoers"],
        
        # ============ SERVICES & PROCESSES ============
        "running_services": ["systemctl", "list-units", "--type=service", "--state=running"],
        "all_services": ["systemctl", "list-units", "--type=service", "--all"],
        "failed_services": ["systemctl", "--failed"],
        "enabled_services": ["systemctl", "list-unit-files", "--state=enabled"],
        "disabled_services": ["systemctl", "list-unit-files", "--state=disabled"],
        "systemd_timers": ["systemctl", "list-timers", "--all"],
        "running_processes": ["ps", "aux"],
        "process_tree": ["pstree", "-p"],
        "top_processes_cpu": ["ps", "aux", "--sort=-pcpu"],
        "top_processes_memory": ["ps", "aux", "--sort=-pmem"],
        "zombie_processes": ["ps", "aux", "|", "grep", "Z"],
        
        # ============ NETWORK CONFIGURATION ============
        "network_interfaces": ["ip", "addr", "show"],
        "network_interfaces_detailed": ["ip", "-s", "link"],
        "routing_table": ["ip", "route", "show"],
        "routing_table_all": ["route", "-n"],
        "arp_table": ["ip", "neigh", "show"],
        "listening_ports": ["ss", "-tuln"],
        "all_sockets": ["ss", "-tunap"],
        "netstat_listening": ["netstat", "-tuln"],
        "netstat_all_connections": ["netstat", "-tunap"],
        "active_connections": ["ss", "-tn"],
        "network_statistics": ["netstat", "-s"],
        "dns_config": ["cat", "/etc/resolv.conf"],
        "hosts_file": ["cat", "/etc/hosts"],
        "network_manager_status": ["nmcli", "general", "status"],
        "network_connections": ["nmcli", "connection", "show"],
        
        # ============ FIREWALL & SECURITY ============
        "iptables_rules": ["iptables", "-L", "-n", "-v"],
        "ip6tables_rules": ["ip6tables", "-L", "-n", "-v"],
        "ufw_status": ["ufw", "status", "verbose"],
        "firewalld_status": ["firewall-cmd", "--state"],
        "firewalld_zones": ["firewall-cmd", "--list-all-zones"],
        "selinux_status": ["sestatus"],
        "apparmor_status": ["aa-status"],
        "fail2ban_status": ["fail2ban-client", "status"],
        
        # ============ PACKAGE MANAGEMENT ============
        "installed_packages": ["dpkg", "-l"],
        "apt_sources": ["cat", "/etc/apt/sources.list"],
        "apt_sources_d": ["ls", "-la", "/etc/apt/sources.list.d/"],
        "security_updates": ["apt", "list", "--upgradable"],
        "package_count": ["dpkg", "-l", "|", "wc", "-l"],
        "recently_installed": ["grep", "install", "/var/log/dpkg.log"],
        "recently_upgraded": ["grep", "upgrade", "/var/log/dpkg.log"],
        "held_packages": ["apt-mark", "showholds"],
        "auto_installed": ["apt-mark", "showauto"],
        
        # ============ FILE SYSTEM & PERMISSIONS ============
        "mounted_filesystems": ["mount"],
        "fstab_config": ["cat", "/etc/fstab"],
        "disk_partitions": ["fdisk", "-l"],
        "lvm_volumes": ["lvdisplay"],
        "volume_groups": ["vgdisplay"],
        "physical_volumes": ["pvdisplay"],
        "suid_files": ["find", "/", "-perm", "-4000", "-type", "f", "2>/dev/null"],
        "sgid_files": ["find", "/", "-perm", "-2000", "-type", "f", "2>/dev/null"],
        "world_writable_files": ["find", "/", "-perm", "-002", "-type", "f", "2>/dev/null"],
        "world_writable_dirs": ["find", "/", "-perm", "-002", "-type", "d", "2>/dev/null"],
        "noowner_files": ["find", "/", "-nouser", "-o", "-nogroup", "2>/dev/null"],
        
        # ============ SSH CONFIGURATION ============
        "ssh_config": ["cat", "/etc/ssh/sshd_config"],
        "ssh_authorized_keys": ["find", "/home", "-name", "authorized_keys", "-exec", "cat", "{}", ";"],
        "ssh_host_keys": ["ls", "-la", "/etc/ssh/"],
        "ssh_active_sessions": ["who"],
        
        # ============ LOGS & AUDIT ============
        "auth_log": ["tail", "-n", "200", "/var/log/auth.log"],
        "syslog": ["tail", "-n", "200", "/var/log/syslog"],
        "kern_log": ["tail", "-n", "100", "/var/log/kern.log"],
        "failed_login_attempts": ["grep", "Failed password", "/var/log/auth.log"],
        "successful_sudo": ["grep", "sudo.*COMMAND", "/var/log/auth.log"],
        "journal_errors": ["journalctl", "-p", "err", "-n", "100"],
        "journal_warnings": ["journalctl", "-p", "warning", "-n", "100"],
        "auditd_status": ["systemctl", "status", "auditd"],
        "audit_rules": ["auditctl", "-l"],
        
        # ============ CRON JOBS ============
        "cron_jobs_root": ["crontab", "-l", "-u", "root"],
        "cron_daily": ["ls", "-la", "/etc/cron.daily/"],
        "cron_weekly": ["ls", "-la", "/etc/cron.weekly/"],
        "cron_monthly": ["ls", "-la", "/etc/cron.monthly/"],
        "system_crontab": ["cat", "/etc/crontab"],
        "cron_d": ["ls", "-la", "/etc/cron.d/"],
        
        # ============ KERNEL & SYSTEM PARAMETERS ============
        "sysctl_all": ["sysctl", "-a"],
        "kernel_modules": ["lsmod"],
        "loaded_modules_info": ["cat", "/proc/modules"],
        "kernel_parameters": ["cat", "/proc/cmdline"],
        "limits_config": ["cat", "/etc/security/limits.conf"],
        
        # ============ DOCKER & CONTAINERS (if installed) ============
        "docker_version": ["docker", "--version"],
        "docker_containers": ["docker", "ps", "-a"],
        "docker_images": ["docker", "images"],
        "docker_networks": ["docker", "network", "ls"],
        "docker_volumes": ["docker", "volume", "ls"],
        
        # ============ SECURITY SCANNING ============
        "rootkit_check": ["chkrootkit"],
        "rkhunter_check": ["rkhunter", "--check", "--skip-keypress"],
        "lynis_audit": ["lynis", "audit", "system", "--quick"],
        "open_files": ["lsof"],
        "file_capabilities": ["getcap", "-r", "/"],
        
        # ============ ENVIRONMENT & VARIABLES ============
        "environment_vars": ["env"],
        "shell_config": ["cat", "/etc/bash.bashrc"],
        "profile_config": ["cat", "/etc/profile"],
        
        # ============ TIME & NTP ============
        "current_time": ["date"],
        "timezone": ["timedatectl"],
        "ntp_status": ["timedatectl", "show-timesync", "--all"],
        "chrony_status": ["chronyc", "tracking"],
        
        # ============ MAIL & SMTP ============
        "mail_queue": ["mailq"],
        "postfix_config": ["postconf", "-n"],
        
        # ============ DATABASE (if installed) ============
        "mysql_status": ["systemctl", "status", "mysql"],
        "postgresql_status": ["systemctl", "status", "postgresql"],
        
        # ============ WEB SERVERS (if installed) ============
        "apache_status": ["systemctl", "status", "apache2"],
        "nginx_status": ["systemctl", "status", "nginx"],
        "apache_config_test": ["apache2ctl", "-t"],
        "nginx_config_test": ["nginx", "-t"],
    }

    # Execute commands
    for check_name, cmd in commands.items():
        try:
            result = subprocess.run(
                cmd, 
                capture_output=True, 
                text=True, 
                timeout=30,
                shell=False
            )
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
            audit_data["checks"][check_name] = {
                "status": "error", 
                "error": str(e)
            }

    return audit_data


@task(name="compress_data")
def compress_data(data: str, method: CompressionMethod = CompressionMethod.GZIP) -> bytes:
    """Compress data using specified method"""
    data_bytes = data.encode('utf-8')
    
    if method == CompressionMethod.GZIP:
        return gzip.compress(data_bytes)
    elif method == CompressionMethod.BASE64:
        return base64.b64encode(data_bytes)
    else:
        return data_bytes


@task(name="create_summary")
def create_summary(audit_data: Dict[str, Any]) -> Dict[str, Any]:
    """Create a lightweight summary of audit data"""
    checks = audit_data.get("checks", {})
    
    summary = {
        "hostname": audit_data.get("hostname"),
        "timestamp": audit_data.get("timestamp"),
        "total_checks": len(checks),
        "successful_checks": sum(1 for c in checks.values() if c.get("status") == "success"),
        "failed_checks": sum(1 for c in checks.values() if c.get("status") == "failed"),
        "error_checks": sum(1 for c in checks.values() if c.get("status") == "error"),
        "check_summary": {}
    }
    
    # Add condensed check info (status only, no output)
    for check_name, check_data in checks.items():
        summary["check_summary"][check_name] = {
            "status": check_data.get("status"),
            "return_code": check_data.get("return_code"),
            "has_error": bool(check_data.get("error")),
            "output_size": len(str(check_data.get("output", "")))
        }
    
    return summary


@task(name="chunk_data")
def chunk_data(data: Dict[str, Any], max_chunk_size: int = 1024 * 1024) -> List[Dict[str, Any]]:
    """Split data into smaller chunks"""
    chunks = []
    checks = data.get("checks", {})
    
    current_chunk = {
        "hostname": data.get("hostname"),
        "timestamp": data.get("timestamp"),
        "chunk_index": 0,
        "total_chunks": 0,  # Will be updated later
        "checks": {}
    }
    
    current_size = calculate_size(json.dumps(current_chunk, default=_json_serializer))
    
    for check_name, check_data in checks.items():
        check_size = calculate_size(json.dumps({check_name: check_data}, default=_json_serializer))
        
        # If adding this check exceeds max size, start new chunk
        if current_size + check_size > max_chunk_size and current_chunk["checks"]:
            chunks.append(current_chunk)
            current_chunk = {
                "hostname": data.get("hostname"),
                "timestamp": data.get("timestamp"),
                "chunk_index": len(chunks),
                "total_chunks": 0,
                "checks": {}
            }
            current_size = calculate_size(json.dumps(current_chunk, default=_json_serializer))
        
        current_chunk["checks"][check_name] = check_data
        current_size += check_size
    
    # Add last chunk
    if current_chunk["checks"]:
        chunks.append(current_chunk)
    
    # Update total_chunks in all chunks
    total = len(chunks)
    for chunk in chunks:
        chunk["total_chunks"] = total
    
    return chunks


@task(name="truncate_output")
def truncate_output(audit_data: Dict[str, Any], max_output_length: int = 500) -> Dict[str, Any]:
    """Truncate long outputs to reduce size"""
    truncated_data = audit_data.copy()
    truncated_data["checks"] = {}
    
    for check_name, check_data in audit_data.get("checks", {}).items():
        truncated_check = check_data.copy()
        
        # Truncate output
        output = str(check_data.get("output", ""))
        if len(output) > max_output_length:
            truncated_check["output"] = output[:max_output_length] + f"\n... [truncated {len(output) - max_output_length} characters]"
            truncated_check["output_truncated"] = True
            truncated_check["original_output_size"] = len(output)
        
        truncated_data["checks"][check_name] = truncated_check
    
    return truncated_data


@task(name="send_via_webhook")
def send_via_webhook(
    data: str, 
    webhook_url: str, 
    content_type: str = "application/json",
    compressed: bool = False,
    max_retries: int = 3
) -> bool:
    """Send data via HTTP webhook with retry logic"""
    
    for attempt in range(max_retries):
        try:
            if compressed:
                encoded_data = gzip.compress(data.encode("utf-8"))
                headers = {
                    "Content-Type": content_type,
                    "Content-Encoding": "gzip",
                    "User-Agent": "Prefect-Audit-System/2.0",
                }
            else:
                encoded_data = data.encode("utf-8")
                headers = {
                    "Content-Type": content_type,
                    "User-Agent": "Prefect-Audit-System/2.0",
                }
            
            print(f"Sending {format_size(len(encoded_data))} to webhook (attempt {attempt + 1}/{max_retries})")
            
            req = Request(webhook_url, data=encoded_data, headers=headers, method="POST")

            with urlopen(req, timeout=60) as response:
                status_code = response.getcode()
                if 200 <= status_code < 300:
                    print(f"✓ Successfully sent data to webhook (status: {status_code})")
                    return True
                else:
                    print(f"✗ Webhook returned status code: {status_code}")

        except HTTPError as e:
            if e.code == 413:
                print(f"✗ Payload too large (413). Data size: {format_size(len(encoded_data))}")
                raise Exception(f"Payload too large for webhook. Size: {format_size(len(encoded_data))}. Consider using chunking or summary mode.")
            elif e.code >= 500 and attempt < max_retries - 1:
                print(f"✗ Server error ({e.code}). Retrying...")
                continue
            else:
                raise Exception(f"HTTP Error {e.code}: {e.reason}")
        
        except URLError as e:
            if attempt < max_retries - 1:
                print(f"✗ Connection error. Retrying...")
                continue
            raise Exception(f"Failed to send data to webhook: {e}")
        
        except Exception as e:
            if attempt < max_retries - 1:
                print(f"✗ Error: {e}. Retrying...")
                continue
            raise Exception(f"Error sending audit data: {e}")
    
    return False


@task(name="send_chunked_webhook")
def send_chunked_webhook(chunks: List[Dict[str, Any]], webhook_url: str) -> bool:
    """Send data in multiple chunks to webhook"""
    print(f"Sending {len(chunks)} chunks to webhook...")
    
    for i, chunk in enumerate(chunks):
        chunk_json = json.dumps(chunk, default=_json_serializer)
        chunk_size = calculate_size(chunk_json)
        
        print(f"Chunk {i+1}/{len(chunks)}: {format_size(chunk_size)}")
        
        success = send_via_webhook(
            chunk_json, 
            webhook_url, 
            content_type="application/json",
            compressed=True
        )
        
        if not success:
            print(f"✗ Failed to send chunk {i+1}")
            return False
    
    print(f"✓ All {len(chunks)} chunks sent successfully")
    return True


@task(name="export_to_json")
def export_to_json(data: Dict[str, Any], output_path: Optional[str] = None, 
                   pretty: bool = True) -> str:
    """Export data to JSON format"""
    json_str = json.dumps(
        data, 
        default=_json_serializer, 
        indent=2 if pretty else None,
        sort_keys=True
    )
    
    if output_path:
        with open(output_path, 'w') as f:
            f.write(json_str)
    
    return json_str


@flow(name="system_audit_optimized", log_prints=True)
def system_audit_flow(
    export_format: ExportFormat = ExportFormat.JSON,
    transmission_method: TransmissionMethod = TransmissionMethod.WEBHOOK,
    output_dir: str = "./audit_reports",
    webhook_url: Optional[str] = "https://webhook.site/3517ded4-3143-4d33-897e-fa5f340a7cfd",
    
    # Size management options
    send_mode: str = "auto",  # "full", "summary", "chunked", "truncated", "auto"
    max_payload_size: int = 5 * 1024 * 1024,  # 5MB default
    max_chunk_size: int = 1 * 1024 * 1024,  # 1MB per chunk
    max_output_length: int = 500,  # Truncate outputs longer than this
    compress: bool = True,
    
    # Other configs
    s3_bucket: Optional[str] = None,
    s3_key: Optional[str] = None,
    pretty_print: bool = True
):
    """
    Optimized audit flow with size management
    
    Args:
        send_mode: How to handle large data
            - "full": Send complete data (may fail if too large)
            - "summary": Send only summary statistics
            - "chunked": Split into multiple requests
            - "truncated": Truncate long outputs
            - "auto": Automatically choose best method
        max_payload_size: Maximum size for single payload
        max_chunk_size: Size of each chunk when using chunked mode
        max_output_length: Maximum length for command outputs
        compress: Use gzip compression
    """
    
    print(f"🔍 Starting system audit on {socket.gethostname()}")
    print(f"📊 Export Format: {export_format.value}")
    print(f"📤 Transmission: {transmission_method.value}")
    print(f"📦 Send Mode: {send_mode}")
    
    # Gather system information
    audit_data = gather_system_info()
    print(f"✓ Collected {len(audit_data['checks'])} system checks")
    
    # Create output directory
    Path(output_dir).mkdir(parents=True, exist_ok=True)
    
    # Generate identifiers
    timestamp = datetime.now(UTC).strftime("%Y%m%d_%H%M%S")
    hostname = audit_data.get("hostname", "unknown")
    
    # Always save full data to file
    file_path = f"{output_dir}/audit_{hostname}_{timestamp}_full.json"
    full_json = export_to_json(audit_data, file_path, pretty_print)
    full_size = calculate_size(full_json)
    print(f"💾 Full data saved to: {file_path} ({format_size(full_size)})")
    
    # Determine send strategy
    if send_mode == "auto":
        if full_size <= max_payload_size:
            send_mode = "full"
            print(f"📊 Auto mode: Using FULL (size OK: {format_size(full_size)})")
        elif full_size <= max_payload_size * 5:
            send_mode = "chunked"
            print(f"📊 Auto mode: Using CHUNKED (size: {format_size(full_size)})")
        else:
            send_mode = "summary"
            print(f"📊 Auto mode: Using SUMMARY (size too large: {format_size(full_size)})")
    
    # Prepare data based on mode
    success = False
    
    if transmission_method == TransmissionMethod.WEBHOOK:
        if not webhook_url:
            webhook_url = os.environ.get("WEBHOOK_URL")
        
        if not webhook_url:
            raise ValueError("webhook_url is required for WEBHOOK transmission")
        
        if send_mode == "full":
            print(f"📤 Sending full data ({format_size(full_size)})...")
            success = send_via_webhook(full_json, webhook_url, compressed=compress)
        
        elif send_mode == "summary":
            print(f"📤 Sending summary only...")
            summary = create_summary(audit_data)
            summary_json = json.dumps(summary, default=_json_serializer)
            summary_size = calculate_size(summary_json)
            print(f"📊 Summary size: {format_size(summary_size)}")
            
            # Add reference to full data file
            summary["full_data_file"] = file_path
            summary["full_data_size"] = full_size
            
            summary_json = json.dumps(summary, default=_json_serializer)
            success = send_via_webhook(summary_json, webhook_url, compressed=compress)
        
        elif send_mode == "truncated":
            print(f"📤 Sending truncated data...")
            truncated_data = truncate_output(audit_data, max_output_length)
            truncated_json = json.dumps(truncated_data, default=_json_serializer)
            truncated_size = calculate_size(truncated_json)
            print(f"📊 Truncated size: {format_size(truncated_size)} (reduced from {format_size(full_size)})")
            success = send_via_webhook(truncated_json, webhook_url, compressed=compress)
        
        elif send_mode == "chunked":
            print(f"📤 Chunking data...")
            chunks = chunk_data(audit_data, max_chunk_size)
            print(f"📦 Created {len(chunks)} chunks")
            success = send_chunked_webhook(chunks, webhook_url)
    
    elif transmission_method == TransmissionMethod.FILE:
        print(f"💾 Data saved to file: {file_path}")
        success = True
    
    # Add other transmission methods as needed...
    
    if success:
        print(f"✓ Audit data successfully transmitted via {transmission_method.value}")
    else:
        print(f"✗ Failed to transmit audit data via {transmission_method.value}")
    
    return {
        "audit_data": audit_data,
        "file_path": file_path,
        "transmission_success": success,
        "data_size": full_size,
        "send_mode": send_mode
    }


if __name__ == "__main__":
    # Example 1: Auto mode (recommended)
    system_audit_flow(
        export_format=ExportFormat.JSON,
        transmission_method=TransmissionMethod.WEBHOOK,
        webhook_url="https://webhook.site/3517ded4-3143-4d33-897e-fa5f340a7cfd",
        send_mode="auto",  # Automatically choose best method
        compress=True
    )
    
    # Example 2: Force summary mode
    # system_audit_flow(
    #     send_mode="summary",
    #     webhook_url=os.environ.get("WEBHOOK_URL")
    # )
    
    # Example 3: Force chunked mode
    # system_audit_flow(
    #     send_mode="chunked",
    #     max_chunk_size=512 * 1024,  # 512KB chunks
    #     webhook_url=os.environ.get("WEBHOOK_URL")
    # )
