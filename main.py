"""
Enterprise System Audit Tool
Collects comprehensive Ubuntu system information with intelligent transmission strategies.
"""

from __future__ import annotations

import asyncio
import base64
import gzip
import json
import logging
import os
import socket
import subprocess
from collections.abc import Sequence
from dataclasses import dataclass, field, asdict
from datetime import UTC, datetime
from enum import Enum
from pathlib import Path
from typing import Any, Final, TypedDict, NotRequired
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from prefect import flow, task

# ============================================================================
# CONFIGURATION & CONSTANTS
# ============================================================================

# Size constants
SIZE_1KB: Final[int] = 1024
SIZE_1MB: Final[int] = SIZE_1KB * 1024
SIZE_5MB: Final[int] = 5 * SIZE_1MB

# Defaults
DEFAULT_MAX_PAYLOAD_SIZE: Final[int] = SIZE_5MB
DEFAULT_MAX_CHUNK_SIZE: Final[int] = SIZE_1MB
DEFAULT_MAX_OUTPUT_LENGTH: Final[int] = 500
DEFAULT_COMMAND_TIMEOUT: Final[int] = 30
DEFAULT_HTTP_TIMEOUT: Final[int] = 60
DEFAULT_MAX_RETRIES: Final[int] = 3

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


# ============================================================================
# ENUMERATIONS
# ============================================================================


class ExportFormat(str, Enum):
    """Supported export formats."""
    JSON = "json"
    CSV = "csv"
    XML = "xml"
    YAML = "yaml"
    HTML = "html"
    MARKDOWN = "md"


class TransmissionMethod(str, Enum):
    """Supported transmission methods."""
    WEBHOOK = "webhook"
    FILE = "file"
    S3 = "s3"
    FTP = "ftp"
    EMAIL = "email"
    SYSLOG = "syslog"


class CompressionMethod(str, Enum):
    """Supported compression methods."""
    NONE = "none"
    GZIP = "gzip"
    BASE64 = "base64"


class SendMode(str, Enum):
    """Data transmission strategies."""
    AUTO = "auto"
    FULL = "full"
    SUMMARY = "summary"
    CHUNKED = "chunked"
    TRUNCATED = "truncated"


class CheckStatus(str, Enum):
    """Status of individual system checks."""
    SUCCESS = "success"
    FAILED = "failed"
    ERROR = "error"
    TIMEOUT = "timeout"
    NOT_FOUND = "not_found"


# ============================================================================
# TYPE DEFINITIONS
# ============================================================================


class CheckResult(TypedDict, total=False):
    """Result of a single system check."""
    status: str
    output: str | None
    error: str | None
    return_code: int | None
    output_truncated: NotRequired[bool]
    original_output_size: NotRequired[int]


class AuditData(TypedDict):
    """Complete audit data structure."""
    hostname: str
    timestamp: datetime
    checks: dict[str, CheckResult]


class ChunkData(TypedDict):
    """Data structure for chunked transmission."""
    hostname: str
    timestamp: datetime
    chunk_index: int
    total_chunks: int
    checks: dict[str, CheckResult]


class SummaryCheckInfo(TypedDict):
    """Condensed information about a single check."""
    status: str
    return_code: int | None
    has_error: bool
    output_size: int


# ============================================================================
# DATACLASSES (Replacing Pydantic)
# ============================================================================


@dataclass(frozen=True)
class AuditConfiguration:
    """Configuration for system audit flow with validation."""
    
    export_format: ExportFormat = ExportFormat.JSON
    transmission_method: TransmissionMethod = TransmissionMethod.WEBHOOK
    output_dir: Path = field(default_factory=lambda: Path("./audit_reports"))
    webhook_url: str | None = "https://webhook.site/3517ded4-3143-4d33-897e-fa5f340a7cfd"
    
    # Size management
    send_mode: SendMode = SendMode.AUTO
    max_payload_size: int = DEFAULT_MAX_PAYLOAD_SIZE
    max_chunk_size: int = DEFAULT_MAX_CHUNK_SIZE
    max_output_length: int = DEFAULT_MAX_OUTPUT_LENGTH
    compress: bool = True
    
    # Optional configurations
    s3_bucket: str | None = None
    s3_key: str | None = None
    pretty_print: bool = True
    
    # Retry configuration
    max_retries: int = DEFAULT_MAX_RETRIES
    command_timeout: int = DEFAULT_COMMAND_TIMEOUT
    http_timeout: int = DEFAULT_HTTP_TIMEOUT
    
    def __post_init__(self) -> None:
        """Validate configuration after initialization."""
        # Validate positive integers
        if self.max_payload_size <= 0:
            raise ValueError("max_payload_size must be positive")
        if self.max_chunk_size <= 0:
            raise ValueError("max_chunk_size must be positive")
        if self.max_output_length <= 0:
            raise ValueError("max_output_length must be positive")
        if not 1 <= self.max_retries <= 10:
            raise ValueError("max_retries must be between 1 and 10")
        if self.command_timeout <= 0:
            raise ValueError("command_timeout must be positive")
        if self.http_timeout <= 0:
            raise ValueError("http_timeout must be positive")
        
        # Get webhook URL from environment if not provided
        if self.webhook_url is None:
            object.__setattr__(self, 'webhook_url', os.getenv("WEBHOOK_URL"))
        
        # Ensure output_dir is a Path object
        if not isinstance(self.output_dir, Path):
            object.__setattr__(self, 'output_dir', Path(self.output_dir))
    
    def to_dict(self) -> dict[str, Any]:
        """Convert configuration to dictionary."""
        data = asdict(self)
        # Convert enums to their values
        for key, value in data.items():
            if isinstance(value, Enum):
                data[key] = value.value
            elif isinstance(value, Path):
                data[key] = str(value)
        return data


@dataclass
class SummaryData:
    """Summary statistics of audit data."""
    
    hostname: str
    timestamp: datetime
    total_checks: int = 0
    successful_checks: int = 0
    failed_checks: int = 0
    error_checks: int = 0
    timeout_checks: int = 0
    not_found_checks: int = 0
    check_summary: dict[str, SummaryCheckInfo] = field(default_factory=dict)
    full_data_file: Path | None = None
    full_data_size: int | None = None
    
    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary with proper serialization."""
        data = asdict(self)
        if self.full_data_file:
            data['full_data_file'] = str(self.full_data_file)
        data['timestamp'] = self.timestamp.isoformat()
        return data


@dataclass(frozen=True)
class SystemCommand:
    """Represents a system command to execute."""
    
    name: str
    command: Sequence[str]
    timeout: int = DEFAULT_COMMAND_TIMEOUT
    required: bool = False


# ============================================================================
# SYSTEM COMMANDS REGISTRY
# ============================================================================


class CommandRegistry:
    """Registry of all system audit commands organized by category."""
    
    @staticmethod
    def get_all_commands() -> dict[str, Sequence[str]]:
        """Get all system audit commands."""
        return {
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
            "users_without_password": ["awk", "-F:", '($2 == "") {print $1}', "/etc/shadow"],
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
            "suid_files": ["find", "/", "-perm", "-4000", "-type", "f"],
            "sgid_files": ["find", "/", "-perm", "-2000", "-type", "f"],
            "world_writable_files": ["find", "/", "-perm", "-002", "-type", "f"],
            "world_writable_dirs": ["find", "/", "-perm", "-002", "-type", "d"],
            "noowner_files": ["find", "/", "-nouser", "-o", "-nogroup"],
            
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
            
            # ============ DOCKER & CONTAINERS ============
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
            
            # ============ DATABASE ============
            "mysql_status": ["systemctl", "status", "mysql"],
            "postgresql_status": ["systemctl", "status", "postgresql"],
            
            # ============ WEB SERVERS ============
            "apache_status": ["systemctl", "status", "apache2"],
            "nginx_status": ["systemctl", "status", "nginx"],
            "apache_config_test": ["apache2ctl", "-t"],
            "nginx_config_test": ["nginx", "-t"],
        }


# ============================================================================
# UTILITY FUNCTIONS
# ============================================================================


def json_serializer(value: Any) -> Any:
    """Convert unsupported objects to JSON-serializable data."""
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, Enum):
        return value.value
    msg = f"Object of type {value.__class__.__name__} is not JSON serializable"
    raise TypeError(msg)


def calculate_size(data: str | bytes) -> int:
    """Calculate size of data in bytes."""
    if isinstance(data, str):
        return len(data.encode("utf-8"))
    return len(data)


def format_size(size_bytes: int) -> str:
    """Format bytes to human-readable format."""
    units = ["B", "KB", "MB", "GB", "TB"]
    size = float(size_bytes)
    
    for unit in units[:-1]:
        if size < 1024.0:
            return f"{size:.2f} {unit}"
        size /= 1024.0
    
    return f"{size:.2f} {units[-1]}"


def compress_data(data: str | bytes, method: CompressionMethod) -> bytes:
    """Compress data using specified method."""
    data_bytes = data if isinstance(data, bytes) else data.encode("utf-8")
    
    match method:
        case CompressionMethod.GZIP:
            return gzip.compress(data_bytes)
        case CompressionMethod.BASE64:
            return base64.b64encode(data_bytes)
        case CompressionMethod.NONE:
            return data_bytes
        case _:
            return data_bytes


# ============================================================================
# CORE TASKS
# ============================================================================


@task(name="execute_command")
def execute_command(
    command: Sequence[str],
    timeout: int = DEFAULT_COMMAND_TIMEOUT,
) -> CheckResult:
    """
    Execute a single system command safely.
    
    Args:
        command: Command and arguments as a sequence
        timeout: Timeout in seconds
        
    Returns:
        CheckResult with command output and status
    """
    try:
        result = subprocess.run(
            command,
            capture_output=True,
            text=True,
            timeout=timeout,
            shell=False,  # Security: prevent shell injection
            check=False,
        )
        
        return CheckResult(
            status=CheckStatus.SUCCESS if result.returncode == 0 else CheckStatus.FAILED,
            output=result.stdout,
            error=result.stderr if result.returncode != 0 else None,
            return_code=result.returncode,
        )
        
    except subprocess.TimeoutExpired:
        logger.warning(f"Command timed out: {' '.join(command)}")
        return CheckResult(
            status=CheckStatus.TIMEOUT,
            error=f"Command timed out after {timeout} seconds",
            output=None,
            return_code=None,
        )
        
    except FileNotFoundError:
        logger.debug(f"Command not found: {command[0]}")
        return CheckResult(
            status=CheckStatus.NOT_FOUND,
            error=f"Command not found: {command[0]}",
            output=None,
            return_code=None,
        )
        
    except PermissionError as e:
        logger.warning(f"Permission denied for command: {' '.join(command)}")
        return CheckResult(
            status=CheckStatus.ERROR,
            error=f"Permission denied: {e}",
            output=None,
            return_code=None,
        )
        
    except Exception as e:
        logger.error(f"Unexpected error executing command {' '.join(command)}: {e}")
        return CheckResult(
            status=CheckStatus.ERROR,
            error=f"Unexpected error: {type(e).__name__}: {e}",
            output=None,
            return_code=None,
        )


@task(name="gather_system_info")
def gather_system_info(timeout: int = DEFAULT_COMMAND_TIMEOUT) -> AuditData:
    """
    Gather comprehensive Ubuntu system information.
    
    Args:
        timeout: Timeout for each command in seconds
        
    Returns:
        Complete audit data with all system checks
    """
    hostname = socket.gethostname()
    timestamp = datetime.now(UTC)
    
    logger.info(f"Starting system audit on {hostname}")
    
    audit_data: AuditData = {
        "hostname": hostname,
        "timestamp": timestamp,
        "checks": {},
    }
    
    commands = CommandRegistry.get_all_commands()
    total_commands = len(commands)
    
    for idx, (check_name, cmd) in enumerate(commands.items(), 1):
        if idx % 20 == 0:
            logger.info(f"Progress: {idx}/{total_commands} checks completed")
        
        result = execute_command(cmd, timeout=timeout)
        audit_data["checks"][check_name] = result
    
    successful = sum(
        1 for check in audit_data["checks"].values()
        if check.get("status") == CheckStatus.SUCCESS
    )
    
    logger.info(
        f"Audit complete: {successful}/{total_commands} checks successful"
    )
    
    return audit_data


@task(name="create_summary")
def create_summary(audit_data: AuditData) -> SummaryData:
    """
    Create a lightweight summary of audit data.
    
    Args:
        audit_data: Complete audit data
        
    Returns:
        Summary statistics and condensed check information
    """
    checks = audit_data.get("checks", {})
    
    # Count checks by status
    status_counts: dict[str, int] = {status.value: 0 for status in CheckStatus}
    for check in checks.values():
        status = check.get("status", CheckStatus.ERROR)
        status_counts[status] = status_counts.get(status, 0) + 1
    
    # Create condensed check summaries
    check_summary: dict[str, SummaryCheckInfo] = {}
    for check_name, check_data in checks.items():
        check_summary[check_name] = SummaryCheckInfo(
            status=check_data.get("status", CheckStatus.ERROR),
            return_code=check_data.get("return_code"),
            has_error=bool(check_data.get("error")),
            output_size=len(str(check_data.get("output", ""))),
        )
    
    return SummaryData(
        hostname=audit_data.get("hostname", "unknown"),
        timestamp=audit_data.get("timestamp", datetime.now(UTC)),
        total_checks=len(checks),
        successful_checks=status_counts.get(CheckStatus.SUCCESS, 0),
        failed_checks=status_counts.get(CheckStatus.FAILED, 0),
        error_checks=status_counts.get(CheckStatus.ERROR, 0),
        timeout_checks=status_counts.get(CheckStatus.TIMEOUT, 0),
        not_found_checks=status_counts.get(CheckStatus.NOT_FOUND, 0),
        check_summary=check_summary,
    )


@task(name="truncate_output")
def truncate_output(
    audit_data: AuditData,
    max_output_length: int = DEFAULT_MAX_OUTPUT_LENGTH,
) -> AuditData:
    """
    Truncate long outputs to reduce data size.
    
    Args:
        audit_data: Original audit data
        max_output_length: Maximum characters per output
        
    Returns:
        Audit data with truncated outputs
    """
    truncated_data: AuditData = {
        "hostname": audit_data["hostname"],
        "timestamp": audit_data["timestamp"],
        "checks": {},
    }
    
    for check_name, check_data in audit_data.get("checks", {}).items():
        truncated_check = check_data.copy()
        
        output = str(check_data.get("output", ""))
        if len(output) > max_output_length:
            truncated_length = len(output) - max_output_length
            truncated_check["output"] = (
                f"{output[:max_output_length]}\n"
                f"... [truncated {truncated_length} characters]"
            )
            truncated_check["output_truncated"] = True
            truncated_check["original_output_size"] = len(output)
        
        truncated_data["checks"][check_name] = truncated_check
    
    return truncated_data


@task(name="chunk_data")
def chunk_data(
    data: AuditData,
    max_chunk_size: int = DEFAULT_MAX_CHUNK_SIZE,
) -> list[ChunkData]:
    """
    Split data into smaller chunks for transmission.
    
    Args:
        data: Complete audit data
        max_chunk_size: Maximum size per chunk in bytes
        
    Returns:
        List of data chunks
    """
    chunks: list[ChunkData] = []
    checks = data.get("checks", {})
    
    current_chunk: ChunkData = {
        "hostname": data["hostname"],
        "timestamp": data["timestamp"],
        "chunk_index": 0,
        "total_chunks": 0,
        "checks": {},
    }
    
    # Calculate base size
    base_json = json.dumps(
        {k: v for k, v in current_chunk.items() if k != "checks"},
        default=json_serializer,
    )
    current_size = calculate_size(base_json)
    
    for check_name, check_data in checks.items():
        check_json = json.dumps({check_name: check_data}, default=json_serializer)
        check_size = calculate_size(check_json)
        
        # Start new chunk if adding this check exceeds max size
        if (
            current_size + check_size > max_chunk_size
            and current_chunk["checks"]
        ):
            chunks.append(current_chunk)
            current_chunk = {
                "hostname": data["hostname"],
                "timestamp": data["timestamp"],
                "chunk_index": len(chunks),
                "total_chunks": 0,
                "checks": {},
            }
            current_size = calculate_size(base_json)
        
        current_chunk["checks"][check_name] = check_data
        current_size += check_size
    
    # Add final chunk
    if current_chunk["checks"]:
        chunks.append(current_chunk)
    
    # Update total_chunks in all chunks
    total = len(chunks)
    for chunk in chunks:
        chunk["total_chunks"] = total
    
    logger.info(f"Data split into {total} chunks")
    return chunks


@task(name="export_to_json")
def export_to_json(
    data: AuditData | dict[str, Any],
    output_path: Path | None = None,
    pretty: bool = True,
) -> str:
    """
    Export data to JSON format.
    
    Args:
        data: Data to export
        output_path: Optional file path to write
        pretty: Whether to format with indentation
        
    Returns:
        JSON string representation
    """
    # Handle SummaryData objects
    if isinstance(data, SummaryData):
        data = data.to_dict()
    
    json_str = json.dumps(
        data,
        default=json_serializer,
        indent=2 if pretty else None,
        sort_keys=True,
    )
    
    if output_path:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(json_str, encoding="utf-8")
        logger.info(f"Data exported to {output_path}")
    
    return json_str


# ============================================================================
# ASYNC TRANSMISSION FUNCTIONS (Using standard library)
# ============================================================================


async def send_http_async(
    url: str,
    data: bytes,
    headers: dict[str, str],
    timeout: int,
) -> tuple[int, str]:
    """
    Send HTTP request asynchronously using standard library.
    
    Args:
        url: Target URL
        data: Request body
        headers: HTTP headers
        timeout: Timeout in seconds
        
    Returns:
        Tuple of (status_code, response_body)
        
    Raises:
        HTTPError, URLError, TimeoutError
    """
    # Run urllib request in thread pool to make it async
    loop = asyncio.get_event_loop()
    
    def _sync_request() -> tuple[int, str]:
        req = Request(url, data=data, headers=headers, method="POST")
        try:
            with urlopen(req, timeout=timeout) as response:
                return response.getcode(), response.read().decode('utf-8')
        except HTTPError as e:
            # Re-raise to be caught by caller
            raise
        except URLError as e:
            raise
    
    return await loop.run_in_executor(None, _sync_request)


@task(name="send_via_webhook")
async def send_via_webhook(
    data: str,
    webhook_url: str,
    content_type: str = "application/json",
    compressed: bool = False,
    max_retries: int = DEFAULT_MAX_RETRIES,
    timeout: int = DEFAULT_HTTP_TIMEOUT,
) -> bool:
    """
    Send data via HTTP webhook with retry logic (async).
    
    Args:
        data: Data to send
        webhook_url: Destination webhook URL
        content_type: HTTP Content-Type header
        compressed: Whether to gzip compress the payload
        max_retries: Maximum number of retry attempts
        timeout: HTTP request timeout in seconds
        
    Returns:
        True if successful, False otherwise
        
    Raises:
        Exception: On unrecoverable errors
    """
    # Prepare payload
    if compressed:
        encoded_data = gzip.compress(data.encode("utf-8"))
        headers = {
            "Content-Type": content_type,
            "Content-Encoding": "gzip",
            "User-Agent": "Prefect-Audit-System/3.0",
        }
    else:
        encoded_data = data.encode("utf-8")
        headers = {
            "Content-Type": content_type,
            "User-Agent": "Prefect-Audit-System/3.0",
        }
    
    payload_size = len(encoded_data)
    
    for attempt in range(1, max_retries + 1):
        try:
            logger.info(
                f"Sending {format_size(payload_size)} to webhook "
                f"(attempt {attempt}/{max_retries})"
            )
            
            status_code, response_body = await send_http_async(
                webhook_url,
                encoded_data,
                headers,
                timeout,
            )
            
            if 200 <= status_code < 300:
                logger.info(
                    f"✓ Successfully sent data (status: {status_code})"
                )
                return True
            
            logger.warning(f"Unexpected status code: {status_code}")
            
        except HTTPError as e:
            if e.code == 413:
                error_msg = (
                    f"Payload too large (413). Size: {format_size(payload_size)}. "
                    "Consider using chunking or summary mode."
                )
                logger.error(error_msg)
                raise ValueError(error_msg) from e
            
            elif e.code >= 500 and attempt < max_retries:
                logger.warning(f"Server error ({e.code}). Retrying...")
                await asyncio.sleep(2 ** attempt)  # Exponential backoff
                continue
            
            else:
                error_msg = f"HTTP Error {e.code}: {e.reason}"
                logger.error(error_msg)
                raise RuntimeError(error_msg) from e
        
        except URLError as e:
            if attempt < max_retries:
                logger.warning("Network error. Retrying...")
                await asyncio.sleep(2 ** attempt)
                continue
            logger.error(f"Network error after {max_retries} attempts")
            raise ConnectionError("Failed to send data: network error") from e
        
        except TimeoutError as e:
            if attempt < max_retries:
                logger.warning("Request timeout. Retrying...")
                await asyncio.sleep(2 ** attempt)
                continue
            logger.error(f"Request timed out after {max_retries} attempts")
            raise TimeoutError(f"Failed to send data: timeout after {timeout}s") from e
        
        except Exception as e:
            if attempt < max_retries:
                logger.warning(f"Error: {e}. Retrying...")
                await asyncio.sleep(2 ** attempt)
                continue
            logger.error(f"Failed to send data after {max_retries} attempts")
            raise RuntimeError(f"Error sending audit data: {e}") from e
    
    return False


@task(name="send_chunked_webhook")
async def send_chunked_webhook(
    chunks: list[ChunkData],
    webhook_url: str,
    config: AuditConfiguration,
) -> bool:
    """
    Send data in multiple chunks to webhook (async).
    
    Args:
        chunks: List of data chunks
        webhook_url: Destination webhook URL
        config: Audit configuration
        
    Returns:
        True if all chunks sent successfully
    """
    logger.info(f"Sending {len(chunks)} chunks to webhook...")
    
    for i, chunk in enumerate(chunks, 1):
        chunk_json = json.dumps(chunk, default=json_serializer)
        chunk_size = calculate_size(chunk_json)
        
        logger.info(f"Chunk {i}/{len(chunks)}: {format_size(chunk_size)}")
        
        success = await send_via_webhook(
            chunk_json,
            webhook_url,
            content_type="application/json",
            compressed=config.compress,
            max_retries=config.max_retries,
            timeout=config.http_timeout,
        )
        
        if not success:
            logger.error(f"✗ Failed to send chunk {i}")
            return False
    
    logger.info(f"✓ All {len(chunks)} chunks sent successfully")
    return True


# ============================================================================
# MAIN FLOW
# ============================================================================


@flow(name="system_audit_optimized", log_prints=True)
async def system_audit_flow(config: AuditConfiguration | None = None) -> dict[str, Any]:
    """
    Optimized system audit flow with intelligent size management.
    
    Args:
        config: Audit configuration (uses defaults if not provided)
        
    Returns:
        Dictionary with audit results and transmission status
    """
    # Use default config if not provided
    if config is None:
        config = AuditConfiguration()
    
    logger.info(f"🔍 Starting system audit on {socket.gethostname()}")
    logger.info(f"📊 Export Format: {config.export_format.value}")
    logger.info(f"📤 Transmission: {config.transmission_method.value}")
    logger.info(f"📦 Send Mode: {config.send_mode.value}")
    
    # Gather system information
    audit_data = gather_system_info(timeout=config.command_timeout)
    
    timestamp = datetime.now(UTC).strftime("%Y%m%d_%H%M%S")
    hostname = audit_data.get("hostname", "unknown")
    
    logger.info(f"✓ Collected {len(audit_data['checks'])} system checks")
    
    # Always save full data to file first (as backup and reference)
    config.output_dir.mkdir(parents=True, exist_ok=True)
    file_path = config.output_dir / f"audit_{hostname}_{timestamp}_full.json"
    
    full_json = export_to_json(audit_data, file_path, config.pretty_print)
    full_size = calculate_size(full_json)
    
    logger.info(f"💾 Full data saved to: {file_path} ({format_size(full_size)})")
    
    # Determine send strategy
    send_mode = config.send_mode
    
    if send_mode == SendMode.AUTO:
        if full_size <= config.max_payload_size:
            send_mode = SendMode.FULL
            logger.info(f"📊 Auto mode: Using FULL (size OK: {format_size(full_size)})")
        elif full_size <= config.max_payload_size * 5:
            send_mode = SendMode.CHUNKED
            logger.info(f"📊 Auto mode: Using CHUNKED (size: {format_size(full_size)})")
        else:
            send_mode = SendMode.SUMMARY
            logger.info(
                f"📊 Auto mode: Using SUMMARY (size too large: {format_size(full_size)})"
            )
    
    # Transmit data based on method
    success = False
    
    match config.transmission_method:
        case TransmissionMethod.WEBHOOK:
            if not config.webhook_url:
                raise ValueError("webhook_url is required for WEBHOOK transmission")
            
            webhook_url_str = config.webhook_url
            
            match send_mode:
                case SendMode.FULL:
                    logger.info(f"📤 Sending full data ({format_size(full_size)})...")
                    success = await send_via_webhook(
                        full_json,
                        webhook_url_str,
                        compressed=config.compress,
                        max_retries=config.max_retries,
                        timeout=config.http_timeout,
                    )
                
                case SendMode.SUMMARY:
                    logger.info("📤 Sending summary only...")
                    summary = create_summary(audit_data)
                    summary.full_data_file = file_path
                    summary.full_data_size = full_size
                    
                    summary_json = json.dumps(summary.to_dict(), default=json_serializer)
                    summary_size = calculate_size(summary_json)
                    logger.info(f"📊 Summary size: {format_size(summary_size)}")
                    
                    success = await send_via_webhook(
                        summary_json,
                        webhook_url_str,
                        compressed=config.compress,
                        max_retries=config.max_retries,
                        timeout=config.http_timeout,
                    )
                
                case SendMode.TRUNCATED:
                    logger.info("📤 Sending truncated data...")
                    truncated_data = truncate_output(audit_data, config.max_output_length)
                    truncated_json = json.dumps(truncated_data, default=json_serializer)
                    truncated_size = calculate_size(truncated_json)
                    logger.info(
                        f"📊 Truncated size: {format_size(truncated_size)} "
                        f"(reduced from {format_size(full_size)})"
                    )
                    success = await send_via_webhook(
                        truncated_json,
                        webhook_url_str,
                        compressed=config.compress,
                        max_retries=config.max_retries,
                        timeout=config.http_timeout,
                    )
                
                case SendMode.CHUNKED:
                    logger.info("📤 Chunking data...")
                    chunks = chunk_data(audit_data, config.max_chunk_size)
                    logger.info(f"📦 Created {len(chunks)} chunks")
                    success = await send_chunked_webhook(chunks, webhook_url_str, config)
                
                case _:
                    raise ValueError(f"Unsupported send mode: {send_mode}")
        
        case TransmissionMethod.FILE:
            # File transmission already completed above
            success = True
        
        case _:
            logger.warning(
                f"Transmission method {config.transmission_method.value} not implemented"
            )
            success = False
    
    if success:
        logger.info(
            f"✓ Audit data successfully transmitted via {config.transmission_method.value}"
        )
    else:
        logger.error(
            f"✗ Failed to transmit audit data via {config.transmission_method.value}"
        )
    
    return {
        "audit_data": audit_data,
        "transmission_success": success,
        "send_mode": send_mode.value,
        "full_data_path": str(file_path),
        "full_data_size": full_size,
    }


# ============================================================================
# ENTRY POINT
# ============================================================================


async def main() -> None:
    """Main entry point."""
    # Example 1: Auto mode with defaults (recommended)
    config = AuditConfiguration(
        export_format=ExportFormat.JSON,
        transmission_method=TransmissionMethod.WEBHOOK,
        send_mode=SendMode.AUTO,
        compress=True,
    )
    
    await system_audit_flow(config)
    
    # Example 2: Force summary mode
    # config = AuditConfiguration(
    #     send_mode=SendMode.SUMMARY,
    #     webhook_url="https://example.com/webhook",
    # )
    # await system_audit_flow(config)
    
    # Example 3: Force chunked mode with custom chunk size
    # config = AuditConfiguration(
    #     send_mode=SendMode.CHUNKED,
    #     max_chunk_size=512 * 1024,  # 512KB chunks
    #     webhook_url="https://example.com/webhook",
    # )
    # await system_audit_flow(config)
    
    # Example 4: File-only mode (no webhook)
    # config = AuditConfiguration(
    #     transmission_method=TransmissionMethod.FILE,
    #     output_dir=Path("/var/log/system-audit"),
    # )
    # await system_audit_flow(config)


if __name__ == "__main__":
    asyncio.run(main())
