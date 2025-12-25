"""
Enterprise System Audit Tool
Collects comprehensive Ubuntu system information with intelligent transmission strategies.

This module provides a Prefect 3.6-based orchestration system for comprehensive
system auditing on Ubuntu systems. It supports multiple transmission methods,
intelligent size management, and graceful degradation strategies.

Architecture:
    - Configuration: Validated dataclass-based configuration
    - Execution: Async task coordination with Prefect 3.6
    - Transmission: Multiple strategies (webhook, file, chunked, summary)
    - Error Handling: Specific exceptions with retry logic

Requirements:
    - Python ≥3.10
    - Prefect ≥3.6
    - Ubuntu/Debian-based system (for command compatibility)

Execution Model:
    - Main flow is async for I/O-bound operations (HTTP, file I/O)
    - Command execution tasks are sync (CPU-bound, subprocess calls)
    - Prefect 3.6 automatically handles sync tasks in async flows via thread pool
    - Concurrent command execution uses asyncio.gather with thread pool

Type Safety:
    - Fully type-annotated with strict mypy compliance
    - Literal types for enum-like string values
    - Protocol definitions for duck typing
    - TypedDict for structured dictionaries

Security:
    - All subprocess calls use shell=False to prevent injection
    - Supports environment-based secrets for webhook URLs
    - Optional redaction of sensitive data (future enhancement)
"""

from __future__ import annotations

import asyncio
import base64
import functools
import gzip
import json
import logging
import os
import re
import socket
import subprocess
from collections.abc import Awaitable, Callable, Sequence
from contextlib import asynccontextmanager, contextmanager
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from enum import Enum
from pathlib import Path
from typing import (
    Any,
    Final,
    Literal,
    NoReturn,
    Protocol,
    TypeAlias,
    TypedDict,
    cast,
)
from urllib.error import HTTPError, URLError
from urllib.parse import urlparse
from urllib.request import Request, urlopen

from prefect import flow, get_run_logger, task
from prefect.futures import PrefectFuture
from prefect.tasks import task_input_hash

# ============================================================================
# CONFIGURATION & CONSTANTS
# ============================================================================

# Type aliases for clarity
CheckStatusType: TypeAlias = Literal["success", "failed", "error", "timeout", "not_found"]
SendModeType: TypeAlias = Literal["auto", "full", "summary", "chunked", "truncated"]

# Size constants (bytes)
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
DEFAULT_RETRY_BACKOFF_BASE: Final[int] = 2
DEFAULT_MAX_BACKOFF_SECONDS: Final[int] = 60
DEFAULT_PROGRESS_LOG_INTERVAL: Final[int] = 20
DEFAULT_CONCURRENT_COMMANDS: Final[int] = 10  # Max concurrent command executions

# Validation constants
MIN_RETRIES: Final[int] = 1
MAX_RETRIES: Final[int] = 10
MIN_TIMEOUT: Final[int] = 1
MAX_TIMEOUT: Final[int] = 3600
MIN_OUTPUT_LENGTH: Final[int] = 10

# URL validation pattern
URL_PATTERN: Final[re.Pattern[str]] = re.compile(
    r"^https?://"  # http:// or https://
    r"(?:(?:[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?\.)+[A-Z]{2,6}\.?|"  # domain
    r"localhost|"  # localhost
    r"\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})"  # IP
    r"(?::\d+)?"  # optional port
    r"(?:/?|[/?]\S+)$",
    re.IGNORECASE,
)

# Configure root logger - will be overridden by Prefect in tasks/flows
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger: logging.Logger = logging.getLogger(__name__)


# ============================================================================
# CUSTOM EXCEPTIONS
# ============================================================================


class AuditError(Exception):
    """
    Base exception for audit-related errors.
    
    All custom exceptions in this module inherit from this base class
    to allow for centralized error handling.
    """


class ConfigurationError(AuditError):
    """
    Configuration validation error.
    
    Raised when configuration parameters are invalid, missing, or
    contradictory. Should be raised during initialization, not runtime.
    """


class TransmissionError(AuditError):
    """
    Data transmission error.
    
    Base class for all transmission-related failures (network, protocol, etc.).
    """


class PayloadTooLargeError(TransmissionError):
    """
    Payload exceeds maximum size.
    
    Raised when server responds with HTTP 413 or when payload exceeds
    pre-configured limits. Should trigger chunking or summary mode.
    """


class NetworkError(TransmissionError):
    """
    Network-related transmission error.
    
    Includes connection failures, timeouts, DNS resolution failures, etc.
    Typically retriable with exponential backoff.
    """


class CommandExecutionError(AuditError):
    """
    System command execution error.
    
    Raised for unexpected command execution failures. Note that non-zero
    exit codes are NOT exceptions - they're captured in CheckResult.
    """


# ============================================================================
# ENUMERATIONS
# ============================================================================


class ExportFormat(str, Enum):
    """
    Supported export formats for audit data.
    
    Currently only JSON is fully implemented. Others are placeholders
    for future expansion.
    """

    JSON = "json"
    CSV = "csv"
    XML = "xml"
    YAML = "yaml"
    HTML = "html"
    MARKDOWN = "md"


class TransmissionMethod(str, Enum):
    """
    Supported transmission methods for audit data.
    
    Defines how audit data is delivered to its destination.
    """

    WEBHOOK = "webhook"
    FILE = "file"
    S3 = "s3"
    FTP = "ftp"
    EMAIL = "email"
    SYSLOG = "syslog"


class CompressionMethod(str, Enum):
    """
    Supported compression methods.
    
    Applied to payloads before transmission to reduce bandwidth.
    """

    NONE = "none"
    GZIP = "gzip"
    BASE64 = "base64"


class SendMode(str, Enum):
    """
    Data transmission strategies for size management.
    
    Determines how data is prepared and sent based on size constraints:
    - AUTO: Automatically select best mode based on data size
    - FULL: Send complete audit data
    - SUMMARY: Send only aggregated statistics
    - CHUNKED: Split data into multiple transmissions
    - TRUNCATED: Trim command outputs to fit size limit
    """

    AUTO = "auto"
    FULL = "full"
    SUMMARY = "summary"
    CHUNKED = "chunked"
    TRUNCATED = "truncated"


class CheckStatus(str, Enum):
    """
    Status codes for individual system checks.
    
    Represents the outcome of executing a single system command:
    - SUCCESS: Command executed with return code 0
    - FAILED: Command executed with non-zero return code
    - ERROR: Execution error (permission denied, OS error)
    - TIMEOUT: Command exceeded time limit
    - NOT_FOUND: Command binary not found in PATH
    """

    SUCCESS = "success"
    FAILED = "failed"
    ERROR = "error"
    TIMEOUT = "timeout"
    NOT_FOUND = "not_found"


# ============================================================================
# TYPE DEFINITIONS
# ============================================================================


class CheckResult(TypedDict):
    """
    Result of a single system check.

    Attributes:
        status: Check outcome (success/failed/error/timeout/not_found)
        output: Standard output from command (None if no output)
        error: Standard error from command (None if no error)
        return_code: Exit code from command (None if not executed)
        output_truncated: Whether output was truncated
        original_output_size: Original output size in characters before truncation
    """

    status: CheckStatusType
    output: str | None
    error: str | None
    return_code: int | None
    output_truncated: bool
    original_output_size: int


class AuditData(TypedDict):
    """
    Complete audit data structure.

    Contains hostname, timestamp, and all check results.
    
    Attributes:
        hostname: System hostname
        timestamp: Audit execution timestamp (UTC)
        checks: Dictionary mapping check names to results
    """

    hostname: str
    timestamp: datetime
    checks: dict[str, CheckResult]


class ChunkData(TypedDict):
    """
    Data structure for chunked transmission.

    Includes chunk metadata for reassembly on the receiving end.
    
    Attributes:
        hostname: System hostname
        timestamp: Audit timestamp
        chunk_index: Zero-based index of this chunk
        total_chunks: Total number of chunks in the set
        checks: Subset of checks in this chunk
    """

    hostname: str
    timestamp: datetime
    chunk_index: int
    total_chunks: int
    checks: dict[str, CheckResult]


class SummaryCheckInfo(TypedDict):
    """
    Condensed information about a single check for summary mode.

    Excludes full output to reduce size, keeping only metadata.
    
    Attributes:
        status: Check outcome status
        return_code: Command exit code (None if not executed)
        has_error: Whether stderr output was captured
        output_size: Size of stdout in characters
    """

    status: CheckStatusType
    return_code: int | None
    has_error: bool
    output_size: int


class Serializable(Protocol):
    """
    Protocol for objects that can be converted to JSON-serializable dict.
    
    Any object implementing this protocol can be serialized by json_serializer().
    """

    def to_dict(self) -> dict[str, Any]:
        """
        Convert object to dictionary.
        
        Returns:
            Dictionary with JSON-serializable values
        """
        ...


# ============================================================================
# DATACLASSES
# ============================================================================


@dataclass(frozen=True, slots=True)
class AuditConfiguration:
    """
    Immutable configuration for system audit flow with comprehensive validation.

    Attributes:
        export_format: Output format (JSON, CSV, etc.)
        transmission_method: How to send data (webhook, file, etc.)
        output_dir: Directory for file output
        webhook_url: URL for webhook transmission (can be None)
        send_mode: Transmission strategy (auto, full, summary, chunked, truncated)
        max_payload_size: Maximum size in bytes for a single payload
        max_chunk_size: Maximum size in bytes for each chunk
        max_output_length: Maximum characters per command output
        compress: Whether to gzip compress payloads
        s3_bucket: S3 bucket name (optional)
        s3_key: S3 key prefix (optional)
        pretty_print: Whether to format JSON with indentation
        max_retries: Maximum retry attempts for transmission
        command_timeout: Timeout in seconds for each command
        http_timeout: Timeout in seconds for HTTP requests
        max_concurrent_commands: Maximum concurrent command executions

    Raises:
        ConfigurationError: If validation fails

    Example:
        >>> config = AuditConfiguration(
        ...     transmission_method=TransmissionMethod.WEBHOOK,
        ...     webhook_url="https://example.com/webhook"
        ... )
    """

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

    # Retry and timeout configuration
    max_retries: int = DEFAULT_MAX_RETRIES
    command_timeout: int = DEFAULT_COMMAND_TIMEOUT
    http_timeout: int = DEFAULT_HTTP_TIMEOUT
    max_concurrent_commands: int = DEFAULT_CONCURRENT_COMMANDS

    def __post_init__(self) -> None:
        """
        Validate configuration after initialization.

        Performs comprehensive validation of all fields and applies
        environment variable overrides where appropriate.

        Raises:
            ConfigurationError: If any validation constraint is violated
        """
        # Validate positive integers with minimums
        if self.max_payload_size <= 0:
            raise ConfigurationError(
                f"max_payload_size must be positive, got {self.max_payload_size}"
            )

        if self.max_chunk_size <= 0:
            raise ConfigurationError(
                f"max_chunk_size must be positive, got {self.max_chunk_size}"
            )

        if self.max_chunk_size > self.max_payload_size:
            raise ConfigurationError(
                f"max_chunk_size ({self.max_chunk_size}) cannot exceed "
                f"max_payload_size ({self.max_payload_size})"
            )

        if self.max_output_length < MIN_OUTPUT_LENGTH:
            raise ConfigurationError(
                f"max_output_length must be at least {MIN_OUTPUT_LENGTH}, "
                f"got {self.max_output_length}"
            )

        if not MIN_RETRIES <= self.max_retries <= MAX_RETRIES:
            raise ConfigurationError(
                f"max_retries must be between {MIN_RETRIES} and {MAX_RETRIES}, "
                f"got {self.max_retries}"
            )

        if not MIN_TIMEOUT <= self.command_timeout <= MAX_TIMEOUT:
            raise ConfigurationError(
                f"command_timeout must be between {MIN_TIMEOUT} and {MAX_TIMEOUT}, "
                f"got {self.command_timeout}"
            )

        if not MIN_TIMEOUT <= self.http_timeout <= MAX_TIMEOUT:
            raise ConfigurationError(
                f"http_timeout must be between {MIN_TIMEOUT} and {MAX_TIMEOUT}, "
                f"got {self.http_timeout}"
            )

        if self.max_concurrent_commands < 1:
            raise ConfigurationError(
                f"max_concurrent_commands must be at least 1, got {self.max_concurrent_commands}"
            )

        # Get webhook URL from environment if not provided
        if self.webhook_url is None:
            webhook_env: str | None = os.getenv("WEBHOOK_URL")
            if webhook_env:
                object.__setattr__(self, "webhook_url", webhook_env)

        # Validate webhook URL format if provided
        if self.webhook_url:
            webhook_str: str = self.webhook_url
            if not self._is_valid_url(webhook_str):
                raise ConfigurationError(
                    f"webhook_url is not a valid HTTP/HTTPS URL: {webhook_str}"
                )

        # Ensure output_dir is Path
        if not isinstance(self.output_dir, Path):
            object.__setattr__(self, "output_dir", Path(self.output_dir))

        # Validate transmission-specific requirements
        if self.transmission_method == TransmissionMethod.WEBHOOK:
            if not self.webhook_url:
                raise ConfigurationError(
                    "webhook_url is required for WEBHOOK transmission method"
                )

        if self.transmission_method == TransmissionMethod.S3:
            if not self.s3_bucket:
                raise ConfigurationError(
                    "s3_bucket is required for S3 transmission method"
                )

    @staticmethod
    def _is_valid_url(url: str) -> bool:
        """
        Validate URL format comprehensively.
        
        Args:
            url: URL string to validate
            
        Returns:
            True if URL is well-formed HTTP/HTTPS URL
        """
        if not url:
            return False
            
        # Check basic format with regex
        if not URL_PATTERN.match(url):
            return False
            
        # Validate with urllib.parse for additional checks
        try:
            parsed = urlparse(url)
            return all([
                parsed.scheme in ("http", "https"),
                parsed.netloc,  # Must have network location
            ])
        except Exception:
            return False

    def to_dict(self) -> dict[str, Any]:
        """
        Convert configuration to a JSON-serializable dictionary.

        Returns:
            Dictionary with all configuration values, enums converted to strings,
            and Path objects converted to strings.
        """
        data: dict[str, Any] = asdict(self)

        # Convert enums and paths to serializable types
        for key, value in data.items():
            if isinstance(value, Enum):
                data[key] = value.value
            elif isinstance(value, Path):
                data[key] = str(value)

        return data


@dataclass(slots=True)
class SummaryData:
    """
    Summary statistics and metadata for audit data.

    Provides aggregated information without full command outputs,
    useful for reducing transmission size.

    Attributes:
        hostname: System hostname
        timestamp: Audit timestamp
        total_checks: Total number of checks performed
        successful_checks: Count of successful checks
        failed_checks: Count of failed checks
        error_checks: Count of checks with errors
        timeout_checks: Count of timed-out checks
        not_found_checks: Count of checks with command not found
        check_summary: Per-check condensed information
        full_data_file: Path to full data file (if saved)
        full_data_size: Size of full data in bytes (if available)
    """

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
        """
        Convert summary to a JSON-serializable dictionary.

        Returns:
            Dictionary with all summary data, properly formatted for JSON.
        """
        data: dict[str, Any] = asdict(self)
        data["timestamp"] = self.timestamp.isoformat()

        if self.full_data_file:
            data["full_data_file"] = str(self.full_data_file)

        return data


@dataclass(frozen=True, slots=True)
class SystemCommand:
    """
    Immutable representation of a system command to execute.

    Attributes:
        name: Human-readable identifier for the command
        command: Command and arguments as a sequence
        timeout: Maximum execution time in seconds
        required: Whether failure should halt the audit
    """

    name: str
    command: Sequence[str]
    timeout: int = DEFAULT_COMMAND_TIMEOUT
    required: bool = False

    def __post_init__(self) -> None:
        """Validate command structure."""
        if not self.command:
            raise ValueError(f"Command for '{self.name}' cannot be empty")

        if self.timeout < MIN_TIMEOUT:
            raise ValueError(
                f"Timeout for '{self.name}' must be at least {MIN_TIMEOUT}s"
            )


# ============================================================================
# SYSTEM COMMANDS REGISTRY
# ============================================================================


class CommandRegistry:
    """
    Centralized registry of all system audit commands.

    Organizes commands by category for comprehensive system analysis.
    All commands are designed for Ubuntu/Debian systems.
    
    Note:
        Uses @lru_cache for thread-safe, automatic caching of command dictionary.
    """

    @staticmethod
    @functools.lru_cache(maxsize=1)
    def get_all_commands() -> dict[str, Sequence[str]]:
        """
        Get all system audit commands organized by category.

        Returns:
            Dictionary mapping check names to command sequences.
            Commands are returned as tuples for immutability.

        Note:
            Result is cached using @lru_cache for performance.
            Thread-safe and automatically handles cache invalidation.
        """
        commands: dict[str, Sequence[str]] = {
            # ============ SYSTEM INFORMATION ============
            "os_version": ("lsb_release", "-a"),
            "kernel_version": ("uname", "-r"),
            "kernel_full_info": ("uname", "-a"),
            "hostname_info": ("hostnamectl",),
            "machine_id": ("cat", "/etc/machine-id"),
            "uptime": ("uptime", "-p"),
            "uptime_detailed": ("uptime",),
            "system_boot_time": ("who", "-b"),
            "last_reboot": ("last", "reboot", "-F"),
            "dmesg_kernel_messages": ("dmesg", "-T"),
            "system_load": ("cat", "/proc/loadavg"),
            # ============ HARDWARE INFORMATION ============
            "cpu_info": ("lscpu",),
            "cpu_details": ("cat", "/proc/cpuinfo"),
            "memory_info": ("free", "-h"),
            "memory_detailed": ("cat", "/proc/meminfo"),
            "swap_info": ("swapon", "--show"),
            "disk_usage": ("df", "-h"),
            "disk_usage_inodes": ("df", "-i"),
            "block_devices": ("lsblk", "-a"),
            "block_device_info": ("blkid",),
            "disk_io_stats": ("iostat",),
            "hardware_info": ("lshw", "-short"),
            "pci_devices": ("lspci", "-v"),
            "usb_devices": ("lsusb", "-v"),
            "dmidecode_hardware": ("dmidecode", "-t", "system"),
            "bios_info": ("dmidecode", "-t", "bios"),
            # ============ USER & AUTHENTICATION ============
            "all_users": ("cat", "/etc/passwd"),
            "all_groups": ("cat", "/etc/group"),
            "shadow_file_check": ("ls", "-la", "/etc/shadow"),
            "current_logged_users": ("w",),
            "who_logged_in": ("who", "-a"),
            "last_logins": ("last", "-n", "50"),
            "last_failed_logins": ("lastb", "-n", "50"),
            "sudo_users": ("getent", "group", "sudo"),
            "users_with_uid_0": ("awk", "-F:", "($3 == 0) {print}", "/etc/passwd"),
            "users_without_password": (
                "awk",
                "-F:",
                '($2 == "") {print $1}',
                "/etc/shadow",
            ),
            "password_policies": ("cat", "/etc/login.defs"),
            "pam_config": ("cat", "/etc/pam.d/common-auth"),
            "sudoers_config": ("cat", "/etc/sudoers"),
            # ============ SERVICES & PROCESSES ============
            "running_services": (
                "systemctl",
                "list-units",
                "--type=service",
                "--state=running",
            ),
            "all_services": ("systemctl", "list-units", "--type=service", "--all"),
            "failed_services": ("systemctl", "--failed"),
            "enabled_services": ("systemctl", "list-unit-files", "--state=enabled"),
            "disabled_services": ("systemctl", "list-unit-files", "--state=disabled"),
            "systemd_timers": ("systemctl", "list-timers", "--all"),
            "running_processes": ("ps", "aux"),
            "process_tree": ("pstree", "-p"),
            "top_processes_cpu": ("ps", "aux", "--sort=-pcpu"),
            "top_processes_memory": ("ps", "aux", "--sort=-pmem"),
            # ============ NETWORK CONFIGURATION ============
            "network_interfaces": ("ip", "addr", "show"),
            "network_interfaces_detailed": ("ip", "-s", "link"),
            "routing_table": ("ip", "route", "show"),
            "routing_table_all": ("route", "-n"),
            "arp_table": ("ip", "neigh", "show"),
            "listening_ports": ("ss", "-tuln"),
            "all_sockets": ("ss", "-tunap"),
            "netstat_listening": ("netstat", "-tuln"),
            "netstat_all_connections": ("netstat", "-tunap"),
            "active_connections": ("ss", "-tn"),
            "network_statistics": ("netstat", "-s"),
            "dns_config": ("cat", "/etc/resolv.conf"),
            "hosts_file": ("cat", "/etc/hosts"),
            "network_manager_status": ("nmcli", "general", "status"),
            "network_connections": ("nmcli", "connection", "show"),
            # ============ FIREWALL & SECURITY ============
            "iptables_rules": ("iptables", "-L", "-n", "-v"),
            "ip6tables_rules": ("ip6tables", "-L", "-n", "-v"),
            "ufw_status": ("ufw", "status", "verbose"),
            "firewalld_status": ("firewall-cmd", "--state"),
            "firewalld_zones": ("firewall-cmd", "--list-all-zones"),
            "selinux_status": ("sestatus",),
            "apparmor_status": ("aa-status",),
            "fail2ban_status": ("fail2ban-client", "status"),
            # ============ PACKAGE MANAGEMENT ============
            "installed_packages": ("dpkg", "-l"),
            "apt_sources": ("cat", "/etc/apt/sources.list"),
            "apt_sources_d": ("ls", "-la", "/etc/apt/sources.list.d/"),
            "security_updates": ("apt", "list", "--upgradable"),
            "recently_installed": ("grep", "install", "/var/log/dpkg.log"),
            "recently_upgraded": ("grep", "upgrade", "/var/log/dpkg.log"),
            "held_packages": ("apt-mark", "showholds"),
            "auto_installed": ("apt-mark", "showauto"),
            # ============ FILE SYSTEM & PERMISSIONS ============
            "mounted_filesystems": ("mount",),
            "fstab_config": ("cat", "/etc/fstab"),
            "disk_partitions": ("fdisk", "-l"),
            "lvm_volumes": ("lvdisplay",),
            "volume_groups": ("vgdisplay",),
            "physical_volumes": ("pvdisplay",),
            "suid_files": ("find", "/", "-perm", "-4000", "-type", "f"),
            "sgid_files": ("find", "/", "-perm", "-2000", "-type", "f"),
            "world_writable_files": ("find", "/", "-perm", "-002", "-type", "f"),
            "world_writable_dirs": ("find", "/", "-perm", "-002", "-type", "d"),
            "noowner_files": ("find", "/", "-nouser", "-o", "-nogroup"),
            # ============ SSH CONFIGURATION ============
            "ssh_config": ("cat", "/etc/ssh/sshd_config"),
            "ssh_authorized_keys": (
                "find",
                "/home",
                "-name",
                "authorized_keys",
                "-exec",
                "cat",
                "{}",
                ";",
            ),
            "ssh_host_keys": ("ls", "-la", "/etc/ssh/"),
            "ssh_active_sessions": ("who",),
            # ============ LOGS & AUDIT ============
            "auth_log": ("tail", "-n", "200", "/var/log/auth.log"),
            "syslog": ("tail", "-n", "200", "/var/log/syslog"),
            "kern_log": ("tail", "-n", "100", "/var/log/kern.log"),
            "failed_login_attempts": ("grep", "Failed password", "/var/log/auth.log"),
            "successful_sudo": ("grep", "sudo.*COMMAND", "/var/log/auth.log"),
            "journal_errors": ("journalctl", "-p", "err", "-n", "100"),
            "journal_warnings": ("journalctl", "-p", "warning", "-n", "100"),
            "auditd_status": ("systemctl", "status", "auditd"),
            "audit_rules": ("auditctl", "-l"),
            # ============ CRON JOBS ============
            "cron_jobs_root": ("crontab", "-l", "-u", "root"),
            "cron_daily": ("ls", "-la", "/etc/cron.daily/"),
            "cron_weekly": ("ls", "-la", "/etc/cron.weekly/"),
            "cron_monthly": ("ls", "-la", "/etc/cron.monthly/"),
            "system_crontab": ("cat", "/etc/crontab"),
            "cron_d": ("ls", "-la", "/etc/cron.d/"),
            # ============ KERNEL & SYSTEM PARAMETERS ============
            "sysctl_all": ("sysctl", "-a"),
            "kernel_modules": ("lsmod",),
            "loaded_modules_info": ("cat", "/proc/modules"),
            "kernel_parameters": ("cat", "/proc/cmdline"),
            "limits_config": ("cat", "/etc/security/limits.conf"),
            # ============ DOCKER & CONTAINERS ============
            "docker_version": ("docker", "--version"),
            "docker_containers": ("docker", "ps", "-a"),
            "docker_images": ("docker", "images"),
            "docker_networks": ("docker", "network", "ls"),
            "docker_volumes": ("docker", "volume", "ls"),
            # ============ SECURITY SCANNING ============
            "rootkit_check": ("chkrootkit",),
            "rkhunter_check": ("rkhunter", "--check", "--skip-keypress"),
            "lynis_audit": ("lynis", "audit", "system", "--quick"),
            "open_files": ("lsof",),
            "file_capabilities": ("getcap", "-r", "/"),
            # ============ ENVIRONMENT & VARIABLES ============
            "environment_vars": ("env",),
            "shell_config": ("cat", "/etc/bash.bashrc"),
            "profile_config": ("cat", "/etc/profile"),
            # ============ TIME & NTP ============
            "current_time": ("date",),
            "timezone": ("timedatectl",),
            "ntp_status": ("timedatectl", "show-timesync", "--all"),
            "chrony_status": ("chronyc", "tracking"),
            # ============ MAIL & SMTP ============
            "mail_queue": ("mailq",),
            "postfix_config": ("postconf", "-n"),
            # ============ DATABASE ============
            "mysql_status": ("systemctl", "status", "mysql"),
            "postgresql_status": ("systemctl", "status", "postgresql"),
            # ============ WEB SERVERS ============
            "apache_status": ("systemctl", "status", "apache2"),
            "nginx_status": ("systemctl", "status", "nginx"),
            "apache_config_test": ("apache2ctl", "-t"),
            "nginx_config_test": ("nginx", "-t"),
        }

        return commands


# ============================================================================
# UTILITY FUNCTIONS
# ============================================================================


def json_serializer(value: Any) -> Any:
    """
    Convert unsupported objects to JSON-serializable types.

    Handles common non-serializable types including datetime, Path,
    Enum, bytes, and objects implementing the Serializable protocol.

    Args:
        value: Object to serialize

    Returns:
        JSON-serializable representation of the value

    Raises:
        TypeError: If the object cannot be serialized

    Example:
        >>> from datetime import datetime
        >>> json_serializer(datetime(2024, 1, 1, 12, 0))
        '2024-01-01T12:00:00'
    """
    if isinstance(value, datetime):
        return value.isoformat()

    if isinstance(value, Path):
        return str(value)

    if isinstance(value, Enum):
        return value.value

    if isinstance(value, bytes):
        return base64.b64encode(value).decode("ascii")

    if hasattr(value, "to_dict") and callable(value.to_dict):
        return value.to_dict()

    raise TypeError(
        f"Object of type {type(value).__name__} (value: {value!r}) "
        f"is not JSON serializable"
    )


def calculate_size(data: str | bytes) -> int:
    """
    Calculate size of data in bytes.

    Args:
        data: String or bytes to measure

    Returns:
        Size in bytes (for strings, UTF-8 encoding is assumed)
        
    Example:
        >>> calculate_size("hello")
        5
        >>> calculate_size("hello".encode())
        5
    """
    if isinstance(data, str):
        return len(data.encode("utf-8"))
    return len(data)


def format_size(size_bytes: int) -> str:
    """
    Format bytes to human-readable format using binary units.

    Args:
        size_bytes: Size in bytes

    Returns:
        Formatted string (e.g., "1.23 MB")

    Example:
        >>> format_size(1536)
        '1.50 KB'
        >>> format_size(5242880)
        '5.00 MB'
    """
    units: tuple[str, ...] = ("B", "KB", "MB", "GB", "TB", "PB")
    size: float = float(size_bytes)

    for unit in units[:-1]:
        if size < 1024.0:
            return f"{size:.2f} {unit}"
        size /= 1024.0

    return f"{size:.2f} {units[-1]}"


def compress_data(data: str | bytes, method: CompressionMethod) -> bytes:
    """
    Compress data using the specified compression method.

    Args:
        data: String or bytes to compress
        method: Compression method to use

    Returns:
        Compressed data as bytes

    Example:
        >>> compressed = compress_data("test data", CompressionMethod.GZIP)
        >>> isinstance(compressed, bytes)
        True
        >>> len(compressed) < len("test data")
        True
    """
    data_bytes: bytes = data if isinstance(data, bytes) else data.encode("utf-8")

    match method:
        case CompressionMethod.GZIP:
            return gzip.compress(data_bytes, compresslevel=6)
        case CompressionMethod.BASE64:
            return base64.b64encode(data_bytes)
        case CompressionMethod.NONE:
            return data_bytes
        case _:
            # Fallback for unknown methods
            logger.warning(f"Unknown compression method: {method}, using NONE")
            return data_bytes


@contextmanager
def safe_file_write(path: Path, mode: str = "w", encoding: str = "utf-8"):
    """
    Context manager for safe file writing with automatic parent directory creation.

    Ensures parent directories exist before opening file. The file is automatically
    closed when exiting the context.

    Args:
        path: Destination file path
        mode: File open mode
        encoding: Text encoding (for text modes)

    Yields:
        File handle

    Raises:
        OSError: If directory creation or file opening fails

    Example:
        >>> from pathlib import Path
        >>> with safe_file_write(Path("test.json")) as f:
        ...     f.write('{"key": "value"}')
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    
    with path.open(mode=mode, encoding=encoding if "b" not in mode else None) as f:
        yield f


# ============================================================================
# CORE TASKS
# ============================================================================


@task(
    name="execute_command",
    retries=0,  # Command execution should not be retried - results may vary
    log_prints=False,  # Using structured logging instead
)
def execute_command(
    command: Sequence[str],
    timeout: int = DEFAULT_COMMAND_TIMEOUT,
) -> CheckResult:
    """
    Execute a single system command safely with comprehensive error handling.

    This task executes system commands in a secure manner (shell=False) and
    captures all output and error information. Individual command failures
    do not raise exceptions - they're captured in the CheckResult status.

    Args:
        command: Command and arguments as a sequence (prevents shell injection)
        timeout: Maximum execution time in seconds

    Returns:
        CheckResult dictionary with command output, error, status, and metadata

    Note:
        - Uses shell=False to prevent command injection attacks
        - Captures both stdout and stderr
        - Handles various error conditions gracefully (timeout, not found, permission)
        - Logs warnings for failures but doesn't raise exceptions
        - Non-zero exit codes are captured, not treated as exceptions

    Security:
        Command is passed as a sequence, not a string, preventing shell injection.
        Even with malicious input, subprocess.run with shell=False is safe.

    Example:
        >>> result = execute_command(("uname", "-r"))
        >>> result["status"] == "success"
        True
        >>> result["output"] is not None
        True
    """
    run_logger: logging.Logger = get_run_logger()

    if not command:
        run_logger.error("Empty command provided")
        return CheckResult(
            status=CheckStatus.ERROR.value,
            output=None,
            error="Empty command provided",
            return_code=None,
            output_truncated=False,
            original_output_size=0,
        )

    command_str: str = " ".join(str(c) for c in command)

    try:
        result: subprocess.CompletedProcess[str] = subprocess.run(
            command,
            capture_output=True,
            text=True,
            timeout=timeout,
            shell=False,  # Critical: prevent shell injection
            check=False,  # Don't raise on non-zero exit
        )

        # Determine status based on return code
        status: CheckStatus = (
            CheckStatus.SUCCESS if result.returncode == 0 else CheckStatus.FAILED
        )

        return CheckResult(
            status=status.value,
            output=result.stdout if result.stdout else None,
            error=result.stderr if result.stderr and result.returncode != 0 else None,
            return_code=result.returncode,
            output_truncated=False,
            original_output_size=len(result.stdout) if result.stdout else 0,
        )

    except subprocess.TimeoutExpired:
        run_logger.warning(
            f"Command timed out after {timeout}s",
            extra={"command": command_str, "timeout": timeout},
        )
        return CheckResult(
            status=CheckStatus.TIMEOUT.value,
            error=f"Command timed out after {timeout} seconds",
            output=None,
            return_code=None,
            output_truncated=False,
            original_output_size=0,
        )

    except FileNotFoundError:
        run_logger.debug(
            f"Command not found: {command[0]}",
            extra={"command": command[0]},
        )
        return CheckResult(
            status=CheckStatus.NOT_FOUND.value,
            error=f"Command not found: {command[0]}",
            output=None,
            return_code=None,
            output_truncated=False,
            original_output_size=0,
        )

    except PermissionError as e:
        run_logger.warning(
            f"Permission denied for command",
            extra={"command": command_str, "error": str(e)},
        )
        return CheckResult(
            status=CheckStatus.ERROR.value,
            error=f"Permission denied: {e}",
            output=None,
            return_code=None,
            output_truncated=False,
            original_output_size=0,
        )

    except OSError as e:
        run_logger.error(
            f"OS error executing command",
            extra={"command": command_str, "error": str(e)},
        )
        return CheckResult(
            status=CheckStatus.ERROR.value,
            error=f"OS error: {e}",
            output=None,
            return_code=None,
            output_truncated=False,
            original_output_size=0,
        )

    except Exception as e:
        run_logger.error(
            f"Unexpected error executing command",
            extra={
                "command": command_str,
                "error_type": type(e).__name__,
                "error": str(e),
            },
            exc_info=True,
        )
        return CheckResult(
            status=CheckStatus.ERROR.value,
            error=f"Unexpected error: {type(e).__name__}: {e}",
            output=None,
            return_code=None,
            output_truncated=False,
            original_output_size=0,
        )


@task(
    name="gather_system_info",
    retries=1,
    log_prints=False,
)
async def gather_system_info(
    timeout: int = DEFAULT_COMMAND_TIMEOUT,
    max_concurrent: int = DEFAULT_CONCURRENT_COMMANDS,
) -> AuditData:
    """
    Gather comprehensive Ubuntu system information with concurrent command execution.

    Executes all commands in the CommandRegistry with controlled concurrency,
    collecting results and metadata. Individual command failures do not halt
    the overall audit process.

    Args:
        timeout: Timeout for each individual command in seconds
        max_concurrent: Maximum number of commands to run concurrently

    Returns:
        AuditData dictionary with hostname, timestamp, and all check results

    Note:
        - Commands are executed concurrently using asyncio + thread pool
        - Progress is logged every N commands (DEFAULT_PROGRESS_LOG_INTERVAL)
        - Individual command failures are logged but don't stop execution
        - Uses Prefect's task system for observability
        - Prefect 3.6 handles sync tasks in async flows automatically

    Performance:
        Concurrent execution significantly reduces total audit time. With 10
        concurrent commands and 100 total commands, this can reduce execution
        time from ~5 minutes to ~30 seconds on typical systems.

    Example:
        >>> data = await gather_system_info(timeout=30, max_concurrent=10)
        >>> len(data["checks"]) > 0
        True
        >>> "hostname" in data
        True
    """
    run_logger: logging.Logger = get_run_logger()

    hostname: str = socket.gethostname()
    timestamp: datetime = datetime.now(UTC)

    run_logger.info(
        f"Starting comprehensive system audit on {hostname}",
        extra={"hostname": hostname, "timestamp": timestamp.isoformat()},
    )

    audit_data: AuditData = {
        "hostname": hostname,
        "timestamp": timestamp,
        "checks": {},
    }

    commands: dict[str, Sequence[str]] = CommandRegistry.get_all_commands()
    total_commands: int = len(commands)

    run_logger.info(
        f"Executing {total_commands} system checks with {max_concurrent} concurrent tasks...",
        extra={"total_commands": total_commands, "max_concurrent": max_concurrent},
    )

    # Create semaphore to limit concurrency
    semaphore = asyncio.Semaphore(max_concurrent)
    
    async def execute_with_limit(
        check_name: str, cmd: Sequence[str], idx: int
    ) -> tuple[str, CheckResult]:
        """Execute single command with semaphore limiting."""
        async with semaphore:
            # Log progress periodically
            if idx % DEFAULT_PROGRESS_LOG_INTERVAL == 0:
                run_logger.info(
                    f"Progress: {idx}/{total_commands} checks completed "
                    f"({idx * 100 // total_commands}%)",
                    extra={
                        "progress": idx,
                        "total": total_commands,
                        "percent": idx * 100 // total_commands,
                    },
                )
            
            # Execute command in thread pool (Prefect handles this automatically)
            # We use asyncio.to_thread to explicitly run sync function in executor
            result = await asyncio.to_thread(execute_command, cmd, timeout)
            return check_name, result

    # Execute all commands concurrently with limit
    tasks = [
        execute_with_limit(check_name, cmd, idx)
        for idx, (check_name, cmd) in enumerate(commands.items(), start=1)
    ]
    
    results = await asyncio.gather(*tasks, return_exceptions=True)
    
    # Process results
    for result in results:
        if isinstance(result, Exception):
            run_logger.error(
                f"Unexpected error in command execution: {result}",
                extra={"error_type": type(result).__name__, "error": str(result)},
                exc_info=result,
            )
            continue
            
        check_name, check_result = result
        audit_data["checks"][check_name] = check_result

    # Calculate statistics
    successful_count: int = sum(
        1
        for check in audit_data["checks"].values()
        if check.get("status") == CheckStatus.SUCCESS.value
    )

    failed_count: int = sum(
        1
        for check in audit_data["checks"].values()
        if check.get("status") == CheckStatus.FAILED.value
    )

    error_count: int = sum(
        1
        for check in audit_data["checks"].values()
        if check.get("status")
        in (CheckStatus.ERROR.value, CheckStatus.TIMEOUT.value, CheckStatus.NOT_FOUND.value)
    )

    run_logger.info(
        f"Audit complete: {successful_count} successful, "
        f"{failed_count} failed, {error_count} errors "
        f"(total: {total_commands})",
        extra={
            "successful": successful_count,
            "failed": failed_count,
            "errors": error_count,
            "total": total_commands,
        },
    )

    return audit_data


@task(
    name="create_summary",
    retries=0,
    log_prints=False,
)
def create_summary(audit_data: AuditData) -> SummaryData:
    """
    Create a lightweight summary of audit data without full outputs.

    Aggregates statistics and creates condensed per-check information,
    significantly reducing data size for transmission. Useful for sending
    overview information before full data is available.

    Args:
        audit_data: Complete audit data with all check results

    Returns:
        SummaryData object with aggregated statistics and condensed check info

    Note:
        - Excludes full command outputs (can reduce size by 90%+)
        - Maintains per-check metadata (status, return code, output size)
        - Useful for preliminary analysis before fetching full data
        - All status counts are pre-calculated for quick access

    Example:
        >>> summary = create_summary(audit_data)
        >>> summary.total_checks == len(audit_data["checks"])
        True
        >>> summary.successful_checks + summary.failed_checks <= summary.total_checks
        True
    """
    run_logger: logging.Logger = get_run_logger()

    checks: dict[str, CheckResult] = audit_data.get("checks", {})

    # Initialize status counters
    status_counts: dict[CheckStatusType, int] = {
        CheckStatus.SUCCESS.value: 0,
        CheckStatus.FAILED.value: 0,
        CheckStatus.ERROR.value: 0,
        CheckStatus.TIMEOUT.value: 0,
        CheckStatus.NOT_FOUND.value: 0,
    }

    # Count checks by status
    for check in checks.values():
        status = cast(CheckStatusType, check.get("status", CheckStatus.ERROR.value))
        if status in status_counts:
            status_counts[status] += 1

    # Create condensed check summaries (exclude full output)
    check_summary: dict[str, SummaryCheckInfo] = {}
    for check_name, check_data in checks.items():
        output_str: str = str(check_data.get("output") or "")

        check_summary[check_name] = SummaryCheckInfo(
            status=cast(CheckStatusType, check_data.get("status", CheckStatus.ERROR.value)),
            return_code=check_data.get("return_code"),
            has_error=bool(check_data.get("error")),
            output_size=len(output_str),
        )

    summary: SummaryData = SummaryData(
        hostname=audit_data.get("hostname", "unknown"),
        timestamp=audit_data.get("timestamp", datetime.now(UTC)),
        total_checks=len(checks),
        successful_checks=status_counts[CheckStatus.SUCCESS.value],
        failed_checks=status_counts[CheckStatus.FAILED.value],
        error_checks=status_counts[CheckStatus.ERROR.value],
        timeout_checks=status_counts[CheckStatus.TIMEOUT.value],
        not_found_checks=status_counts[CheckStatus.NOT_FOUND.value],
        check_summary=check_summary,
    )

    run_logger.info(
        f"Summary created: {summary.successful_checks}/{summary.total_checks} successful",
        extra={
            "total": summary.total_checks,
            "successful": summary.successful_checks,
            "failed": summary.failed_checks,
            "errors": summary.error_checks,
        },
    )

    return summary


@task(
    name="truncate_output",
    retries=0,
    log_prints=False,
)
def truncate_output(
    audit_data: AuditData,
    max_output_length: int = DEFAULT_MAX_OUTPUT_LENGTH,
) -> AuditData:
    """
    Truncate long command outputs to reduce overall data size.

    Creates a new AuditData structure with truncated outputs while
    preserving metadata about the original sizes. Original data is
    not modified.

    Args:
        audit_data: Original audit data with full outputs
        max_output_length: Maximum characters to keep per output

    Returns:
        New AuditData with truncated outputs and truncation metadata

    Note:
        - Original data is not modified (returns new structure)
        - Adds truncation indicators and original size metadata
        - Preserves all other fields (errors, return codes, etc.)
        - Truncation message includes character count for reference

    Example:
        >>> truncated = truncate_output(audit_data, max_output_length=100)
        >>> all(
        ...     len(str(check.get("output", ""))) <= 120  # 100 + truncation message
        ...     for check in truncated["checks"].values()
        ... )
        True
    """
    run_logger: logging.Logger = get_run_logger()

    if max_output_length < MIN_OUTPUT_LENGTH:
        run_logger.warning(
            f"max_output_length ({max_output_length}) is very small, "
            f"minimum recommended: {MIN_OUTPUT_LENGTH}",
            extra={
                "max_output_length": max_output_length,
                "min_recommended": MIN_OUTPUT_LENGTH,
            },
        )

    truncated_data: AuditData = {
        "hostname": audit_data["hostname"],
        "timestamp": audit_data["timestamp"],
        "checks": {},
    }

    truncated_count: int = 0
    total_original_size: int = 0
    total_truncated_size: int = 0

    for check_name, check_data in audit_data.get("checks", {}).items():
        # Create a copy of check data
        truncated_check: CheckResult = CheckResult(
            status=check_data["status"],
            output=check_data.get("output"),
            error=check_data.get("error"),
            return_code=check_data.get("return_code"),
            output_truncated=False,
            original_output_size=0,
        )

        output: str | None = check_data.get("output")
        if output and len(output) > max_output_length:
            original_size: int = len(output)
            truncated_length: int = original_size - max_output_length

            truncated_check["output"] = (
                f"{output[:max_output_length]}\n"
                f"... [truncated {truncated_length:,} characters]"
            )
            truncated_check["output_truncated"] = True
            truncated_check["original_output_size"] = original_size

            truncated_count += 1
            total_original_size += original_size
            total_truncated_size += max_output_length

        truncated_data["checks"][check_name] = truncated_check

    if truncated_count > 0:
        reduction_percent: float = (
            (total_original_size - total_truncated_size) * 100 / total_original_size
        )
        run_logger.info(
            f"Truncated {truncated_count} outputs, "
            f"reduced by {reduction_percent:.1f}% "
            f"({format_size(total_original_size)} → {format_size(total_truncated_size)})",
            extra={
                "truncated_count": truncated_count,
                "reduction_percent": reduction_percent,
                "original_size": total_original_size,
                "truncated_size": total_truncated_size,
            },
        )
    else:
        run_logger.info("No outputs required truncation")

    return truncated_data


@task(
    name="chunk_data",
    retries=0,
    log_prints=False,
)
def chunk_data(
    data: AuditData,
    max_chunk_size: int = DEFAULT_MAX_CHUNK_SIZE,
) -> list[ChunkData]:
    """
    Split audit data into smaller chunks for transmission.

    Divides the checks into multiple chunks that each fit within the
    specified size limit. Preserves all data while adding chunk metadata
    for reassembly.

    Args:
        data: Complete audit data to chunk
        max_chunk_size: Maximum size per chunk in bytes (JSON representation)

    Returns:
        List of ChunkData dictionaries, each with subset of checks and metadata

    Note:
        - Each chunk includes hostname, timestamp, and chunk indices
        - Chunks are sized based on JSON representation
        - Individual checks are not split (chunk may slightly exceed max_chunk_size)
        - All chunks include total_chunks for reassembly
        - Chunks should be transmitted in order for proper reassembly

    Algorithm:
        1. Calculate base chunk overhead (metadata without checks)
        2. Iterate through checks, adding to current chunk
        3. When adding a check would exceed limit, start new chunk
        4. Update all chunks with total_chunks count

    Example:
        >>> chunks = chunk_data(audit_data, max_chunk_size=1024*1024)
        >>> all(chunk["total_chunks"] == len(chunks) for chunk in chunks)
        True
        >>> sum(len(chunk["checks"]) for chunk in chunks) == len(audit_data["checks"])
        True
    """
    run_logger: logging.Logger = get_run_logger()

    chunks: list[ChunkData] = []
    checks: dict[str, CheckResult] = data.get("checks", {})

    if not checks:
        run_logger.warning("No checks to chunk")
        return chunks

    # Prepare base chunk structure
    base_chunk: ChunkData = {
        "hostname": data["hostname"],
        "timestamp": data["timestamp"],
        "chunk_index": 0,
        "total_chunks": 0,
        "checks": {},
    }

    # Calculate base size (without checks)
    base_json: str = json.dumps(
        {k: v for k, v in base_chunk.items() if k != "checks"},
        default=json_serializer,
    )
    base_size: int = calculate_size(base_json)

    run_logger.debug(
        f"Base chunk overhead: {format_size(base_size)}",
        extra={"base_size": base_size},
    )

    # Initialize first chunk
    current_chunk: ChunkData = base_chunk.copy()
    current_chunk["checks"] = {}
    current_size: int = base_size

    # Distribute checks across chunks
    for check_name, check_data in checks.items():
        # Calculate size of this check
        check_json: str = json.dumps({check_name: check_data}, default=json_serializer)
        check_size: int = calculate_size(check_json)

        # If adding this check would exceed max size and we have checks, start new chunk
        if current_size + check_size > max_chunk_size and current_chunk["checks"]:
            run_logger.debug(
                f"Chunk {len(chunks)} filled: {format_size(current_size)}, "
                f"{len(current_chunk['checks'])} checks",
                extra={
                    "chunk_index": len(chunks),
                    "chunk_size": current_size,
                    "num_checks": len(current_chunk["checks"]),
                },
            )
            chunks.append(current_chunk)

            # Start new chunk
            current_chunk = base_chunk.copy()
            current_chunk["chunk_index"] = len(chunks)
            current_chunk["checks"] = {}
            current_size = base_size

        # Add check to current chunk
        current_chunk["checks"][check_name] = check_data
        current_size += check_size

    # Add final chunk if it has checks
    if current_chunk["checks"]:
        run_logger.debug(
            f"Chunk {len(chunks)} (final): {format_size(current_size)}, "
            f"{len(current_chunk['checks'])} checks",
            extra={
                "chunk_index": len(chunks),
                "chunk_size": current_size,
                "num_checks": len(current_chunk["checks"]),
            },
        )
        chunks.append(current_chunk)

    # Update total_chunks in all chunks
    total: int = len(chunks)
    for chunk in chunks:
        chunk["total_chunks"] = total

    avg_checks_per_chunk: int = len(checks) // total if total > 0 else 0
    run_logger.info(
        f"Data split into {total} chunks, "
        f"average {avg_checks_per_chunk} checks per chunk",
        extra={
            "total_chunks": total,
            "total_checks": len(checks),
            "avg_checks_per_chunk": avg_checks_per_chunk,
        },
    )

    return chunks


@task(
    name="export_to_json",
    retries=1,
    log_prints=False,
)
def export_to_json(
    data: AuditData | dict[str, Any] | SummaryData,
    output_path: Path | None = None,
    pretty: bool = True,
) -> str:
    """
    Export data to JSON format with optional file writing.

    Serializes data to JSON string with proper handling of custom types
    (datetime, Path, Enum, etc.). Optionally writes to file with directory
    creation.

    Args:
        data: Data to export (AuditData, SummaryData, or dict)
        output_path: Optional file path to write JSON
        pretty: Whether to format with indentation (2 spaces)

    Returns:
        JSON string representation of the data

    Raises:
        TypeError: If data contains unserializable objects
        OSError: If file writing fails

    Note:
        - Automatically handles SummaryData.to_dict() conversion
        - Creates parent directories if needed
        - Uses UTF-8 encoding for file writing
        - Sorts keys for consistent output
        - Sets ensure_ascii=False to preserve Unicode characters

    Example:
        >>> json_str = export_to_json(audit_data, pretty=True)
        >>> isinstance(json_str, str)
        True
        >>> "hostname" in json_str
        True
    """
    run_logger: logging.Logger = get_run_logger()

    # Convert SummaryData to dict if needed
    export_data: dict[str, Any] | AuditData
    if isinstance(data, SummaryData):
        export_data = data.to_dict()
    else:
        export_data = data

    # Serialize to JSON
    try:
        json_str: str = json.dumps(
            export_data,
            default=json_serializer,
            indent=2 if pretty else None,
            sort_keys=True,
            ensure_ascii=False,
        )
    except TypeError as e:
        run_logger.error(
            f"Failed to serialize data to JSON: {e}",
            extra={"error": str(e)},
            exc_info=True,
        )
        raise

    # Write to file if path provided
    if output_path:
        try:
            with safe_file_write(output_path, mode="w", encoding="utf-8") as f:
                f.write(json_str)

            file_size: int = output_path.stat().st_size
            run_logger.info(
                f"Data exported to {output_path} ({format_size(file_size)})",
                extra={"output_path": str(output_path), "file_size": file_size},
            )
        except OSError as e:
            run_logger.error(
                f"Failed to write to {output_path}: {e}",
                extra={"output_path": str(output_path), "error": str(e)},
                exc_info=True,
            )
            raise

    return json_str


# ============================================================================
# ASYNC TRANSMISSION FUNCTIONS
# ============================================================================


async def send_http_async(
    url: str,
    data: bytes,
    headers: dict[str, str],
    timeout: int,
) -> tuple[int, str]:
    """
    Send HTTP request asynchronously using standard library.

    Wraps urllib's synchronous request in an async executor to avoid
    blocking the event loop. Uses asyncio.to_thread for proper async integration.

    Args:
        url: Target URL
        data: Request body (bytes)
        headers: HTTP headers dictionary
        timeout: Timeout in seconds

    Returns:
        Tuple of (status_code, response_body)

    Raises:
        HTTPError: For HTTP error responses (4xx, 5xx)
        URLError: For network errors (DNS, connection refused, etc.)
        TimeoutError: For timeout errors (converted from socket.timeout)

    Note:
        - Runs in thread pool executor to maintain async compatibility
        - Uses POST method
        - Reads full response body
        - Thread-safe and properly integrated with asyncio

    Example:
        >>> status, body = await send_http_async(
        ...     "https://httpbin.org/post",
        ...     b'{"test": "data"}',
        ...     {"Content-Type": "application/json"},
        ...     30
        ... )
        >>> 200 <= status < 300
        True
    """

    def _sync_request() -> tuple[int, str]:
        """Synchronous request wrapper for executor."""
        req: Request = Request(url, data=data, headers=headers, method="POST")

        try:
            with urlopen(req, timeout=timeout) as response:
                status_code: int = response.getcode()
                response_body: str = response.read().decode("utf-8")
                return status_code, response_body

        except HTTPError:
            # Let HTTPError propagate with full context
            raise

        except URLError as e:
            # URLError includes timeout errors in Python's urllib
            if isinstance(e.reason, TimeoutError) or "timed out" in str(e.reason).lower():
                raise TimeoutError(f"Request timed out after {timeout}s") from e
            raise

        except Exception as e:
            # Wrap unexpected errors
            raise RuntimeError(f"Unexpected error in HTTP request: {e}") from e

    # Run in thread pool to avoid blocking event loop
    return await asyncio.to_thread(_sync_request)


@task(
    name="send_via_webhook",
    retries=0,  # Manual retry logic implemented below
    log_prints=False,
)
async def send_via_webhook(
    data: str,
    webhook_url: str,
    content_type: str = "application/json",
    compressed: bool = False,
    max_retries: int = DEFAULT_MAX_RETRIES,
    timeout: int = DEFAULT_HTTP_TIMEOUT,
) -> bool:
    """
    Send data via HTTP webhook with exponential backoff retry logic.

    Implements robust error handling and retry strategies for reliable
    data transmission. Retries on transient errors (5xx, network issues)
    but fails immediately on permanent errors (413 payload too large).

    Args:
        data: Data to send (string)
        webhook_url: Destination webhook URL
        content_type: HTTP Content-Type header value
        compressed: Whether to gzip compress the payload
        max_retries: Maximum number of retry attempts
        timeout: HTTP request timeout in seconds

    Returns:
        True if successful, False if all retries exhausted

    Raises:
        PayloadTooLargeError: If server returns 413 (Payload Too Large)
        NetworkError: If network errors persist after all retries
        RuntimeError: For unrecoverable HTTP errors (4xx except 413)

    Note:
        - Uses exponential backoff: 2^attempt seconds (capped at 60s)
        - Retries on 5xx errors and network issues (transient)
        - Fails immediately on 413 (payload size issue)
        - Fails immediately on other 4xx (client errors)
        - Logs detailed information for each attempt
        - Uses manual retry logic instead of Prefect's to allow
          for status-specific handling (e.g., don't retry 413)

    Retry Strategy:
        - Attempt 1: Immediate
        - Attempt 2: Wait 2s
        - Attempt 3: Wait 4s
        - Attempt 4: Wait 8s (if max_retries=4)
        - Maximum backoff: 60s

    Example:
        >>> success = await send_via_webhook(
        ...     '{"test": "data"}',
        ...     "https://httpbin.org/post",
        ...     compressed=True,
        ...     max_retries=3
        ... )
        >>> isinstance(success, bool)
        True
    """
    run_logger: logging.Logger = get_run_logger()

    # Prepare payload
    encoded_data: bytes
    headers: dict[str, str]

    if compressed:
        encoded_data = gzip.compress(data.encode("utf-8"), compresslevel=6)
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

    payload_size: int = len(encoded_data)

    # Retry loop
    for attempt in range(1, max_retries + 1):
        try:
            run_logger.info(
                f"Sending {format_size(payload_size)} to webhook (attempt {attempt}/{max_retries})",
                extra={
                    "payload_size": payload_size,
                    "attempt": attempt,
                    "max_retries": max_retries,
                    "webhook_url": webhook_url,
                    "compressed": compressed,
                },
            )

            status_code, response_body = await send_http_async(
                webhook_url,
                encoded_data,
                headers,
                timeout,
            )

            if 200 <= status_code < 300:
                run_logger.info(
                    f"✓ Successfully sent data (status: {status_code})",
                    extra={"status_code": status_code, "attempt": attempt},
                )
                return True

            run_logger.warning(
                f"Unexpected status code: {status_code}, response: {response_body[:200]}",
                extra={
                    "status_code": status_code,
                    "response_preview": response_body[:200],
                },
            )

        except HTTPError as e:
            if e.code == 413:
                error_msg: str = (
                    f"Payload too large (413). Size: {format_size(payload_size)}. "
                    "Consider using chunking or summary mode."
                )
                run_logger.error(
                    error_msg,
                    extra={"status_code": 413, "payload_size": payload_size},
                )
                raise PayloadTooLargeError(error_msg) from e

            elif e.code >= 500:
                if attempt < max_retries:
                    backoff: int = min(
                        DEFAULT_RETRY_BACKOFF_BASE**attempt, DEFAULT_MAX_BACKOFF_SECONDS
                    )
                    run_logger.warning(
                        f"Server error ({e.code}). Retrying in {backoff}s...",
                        extra={
                            "status_code": e.code,
                            "attempt": attempt,
                            "backoff_seconds": backoff,
                        },
                    )
                    await asyncio.sleep(backoff)
                    continue
                else:
                    error_msg = f"Server error {e.code} after {max_retries} attempts"
                    run_logger.error(
                        error_msg,
                        extra={"status_code": e.code, "total_attempts": max_retries},
                    )
                    raise RuntimeError(error_msg) from e

            else:
                error_msg = f"HTTP Error {e.code}: {e.reason}"
                run_logger.error(
                    error_msg,
                    extra={"status_code": e.code, "reason": str(e.reason)},
                )
                raise RuntimeError(error_msg) from e

        except URLError as e:
            if attempt < max_retries:
                backoff = min(
                    DEFAULT_RETRY_BACKOFF_BASE**attempt, DEFAULT_MAX_BACKOFF_SECONDS
                )
                run_logger.warning(
                    f"Network error: {e.reason}. Retrying in {backoff}s...",
                    extra={"error": str(e.reason), "attempt": attempt, "backoff_seconds": backoff},
                )
                await asyncio.sleep(backoff)
                continue

            error_msg = f"Network error after {max_retries} attempts: {e.reason}"
            run_logger.error(
                error_msg,
                extra={"error": str(e.reason), "total_attempts": max_retries},
            )
            raise NetworkError(error_msg) from e

        except TimeoutError as e:
            if attempt < max_retries:
                backoff = min(
                    DEFAULT_RETRY_BACKOFF_BASE**attempt, DEFAULT_MAX_BACKOFF_SECONDS
                )
                run_logger.warning(
                    f"Request timeout. Retrying in {backoff}s...",
                    extra={"timeout": timeout, "attempt": attempt, "backoff_seconds": backoff},
                )
                await asyncio.sleep(backoff)
                continue

            error_msg = f"Request timed out after {max_retries} attempts ({timeout}s each)"
            run_logger.error(
                error_msg,
                extra={"timeout": timeout, "total_attempts": max_retries},
            )
            raise NetworkError(error_msg) from e

        except Exception as e:
            if attempt < max_retries:
                backoff = min(
                    DEFAULT_RETRY_BACKOFF_BASE**attempt, DEFAULT_MAX_BACKOFF_SECONDS
                )
                run_logger.warning(
                    f"Unexpected error: {type(e).__name__}: {e}. Retrying in {backoff}s...",
                    extra={
                        "error_type": type(e).__name__,
                        "error": str(e),
                        "attempt": attempt,
                        "backoff_seconds": backoff,
                    },
                )
                await asyncio.sleep(backoff)
                continue

            error_msg = f"Failed to send data after {max_retries} attempts: {e}"
            run_logger.error(
                error_msg,
                extra={
                    "error_type": type(e).__name__,
                    "error": str(e),
                    "total_attempts": max_retries,
                },
            )
            raise RuntimeError(error_msg) from e

    run_logger.error(
        f"All {max_retries} attempts exhausted",
        extra={"total_attempts": max_retries},
    )
    return False


@task(
    name="send_chunked_webhook",
    retries=1,
    log_prints=False,
)
async def send_chunked_webhook(
    chunks: list[ChunkData],
    webhook_url: str,
    config: AuditConfiguration,
) -> bool:
    """
    Send data in multiple chunks to webhook sequentially.

    Transmits each chunk in order, stopping on first failure. Chunks
    should be sent sequentially to maintain order for proper reassembly
    on the receiving end.

    Args:
        chunks: List of data chunks to send
        webhook_url: Destination webhook URL
        config: Audit configuration (for compression, retry settings)

    Returns:
        True if all chunks sent successfully, False otherwise

    Note:
        - Sends chunks sequentially (not in parallel) to maintain order
        - Stops on first failure
        - Logs progress for each chunk
        - Each chunk includes metadata for reassembly (chunk_index, total_chunks)
        - Receiver should buffer chunks and reassemble when total_chunks received

    Example:
        >>> success = await send_chunked_webhook(chunks, url, config)
        >>> isinstance(success, bool)
        True
    """
    run_logger: logging.Logger = get_run_logger()

    if not chunks:
        run_logger.warning("No chunks to send")
        return True

    total_chunks: int = len(chunks)
    run_logger.info(
        f"Sending {total_chunks} chunks to webhook...",
        extra={"total_chunks": total_chunks, "webhook_url": webhook_url},
    )

    for i, chunk in enumerate(chunks, start=1):
        chunk_json: str = json.dumps(chunk, default=json_serializer)
        chunk_size: int = calculate_size(chunk_json)

        run_logger.info(
            f"Sending chunk {i}/{total_chunks} "
            f"({format_size(chunk_size)}, "
            f"{len(chunk.get('checks', {}))} checks)",
            extra={
                "chunk_index": i,
                "total_chunks": total_chunks,
                "chunk_size": chunk_size,
                "num_checks": len(chunk.get("checks", {})),
            },
        )

        try:
            success: bool = await send_via_webhook(
                chunk_json,
                webhook_url,
                content_type="application/json",
                compressed=config.compress,
                max_retries=config.max_retries,
                timeout=config.http_timeout,
            )

            if not success:
                run_logger.error(
                    f"✗ Failed to send chunk {i}/{total_chunks}",
                    extra={"chunk_index": i, "total_chunks": total_chunks},
                )
                return False

        except Exception as e:
            run_logger.error(
                f"✗ Error sending chunk {i}/{total_chunks}: {type(e).__name__}: {e}",
                extra={
                    "chunk_index": i,
                    "total_chunks": total_chunks,
                    "error_type": type(e).__name__,
                    "error": str(e),
                },
                exc_info=True,
            )
            return False

    run_logger.info(
        f"✓ All {total_chunks} chunks sent successfully",
        extra={"total_chunks": total_chunks},
    )
    return True


# ============================================================================
# MAIN FLOW
# ============================================================================


@flow(
    name="system_audit_optimized",
    log_prints=False,  # Using structured logging
    retries=1,
    retry_delay_seconds=30,
)
async def system_audit_flow(config: AuditConfiguration | None = None) -> dict[str, Any]:
    """
    Optimized system audit flow with intelligent size management and transmission.

    This is the main orchestration flow that coordinates all audit tasks:
    1. Gather comprehensive system information
    2. Save full data to file (backup)
    3. Determine optimal transmission strategy (auto mode)
    4. Transmit data using selected method
    5. Report results

    Args:
        config: Audit configuration. If None, uses default configuration.

    Returns:
        Dictionary with audit results:
            - audit_data: Complete audit data
            - transmission_success: Whether data was successfully transmitted
            - send_mode: Transmission mode used (full/summary/chunked/truncated)
            - full_data_path: Path to saved full data file
            - full_data_size: Size of full data in bytes
            - hostname: System hostname
            - timestamp: Audit timestamp

    Raises:
        ConfigurationError: If configuration is invalid
        ValueError: If unsupported send mode or transmission method specified
        TransmissionError: If transmission fails after all retries

    Note:
        - Async flow for efficient I/O handling
        - Prefect 3.6 automatically handles sync tasks in async flows
        - Data is always saved to file, even if transmission fails
        - Graceful degradation: partial success if file saved but transmission fails
        - Uses structured logging with extra context for observability

    Execution Model:
        - Main flow is async for I/O-bound operations (HTTP, file I/O)
        - Command execution tasks are sync (CPU-bound, subprocess calls)
        - Prefect automatically runs sync tasks in thread pool from async flow
        - Concurrent command execution uses asyncio.gather with semaphore limiting

    Send Mode Selection (AUTO):
        - FULL: Data ≤ max_payload_size
        - CHUNKED: Data ≤ max_payload_size * 5
        - SUMMARY: Data > max_payload_size * 5

    Example:
        >>> config = AuditConfiguration(
        ...     transmission_method=TransmissionMethod.WEBHOOK,
        ...     webhook_url="https://example.com/webhook",
        ...     send_mode=SendMode.AUTO,
        ... )
        >>> result = await system_audit_flow(config)
        >>> result["transmission_success"]
        True
    """
    flow_logger: logging.Logger = get_run_logger()

    # Use default config if not provided
    if config is None:
        config = AuditConfiguration()
        flow_logger.info("Using default configuration")

    hostname: str = socket.gethostname()

    flow_logger.info(
        f"🔍 Starting system audit on {hostname}",
        extra={
            "hostname": hostname,
            "export_format": config.export_format.value,
            "transmission_method": config.transmission_method.value,
            "send_mode": config.send_mode.value,
        },
    )
    flow_logger.info(f"📊 Export Format: {config.export_format.value}")
    flow_logger.info(f"📤 Transmission: {config.transmission_method.value}")
    flow_logger.info(f"📦 Send Mode: {config.send_mode.value}")

    # ========== PHASE 1: Gather System Information ==========
    flow_logger.info("Phase 1: Gathering system information...")

    # Async task - uses await
    audit_data: AuditData = await gather_system_info(
        timeout=config.command_timeout,
        max_concurrent=config.max_concurrent_commands,
    )

    timestamp: str = datetime.now(UTC).strftime("%Y%m%d_%H%M%S")
    audit_hostname: str = audit_data.get("hostname", "unknown")

    flow_logger.info(
        f"✓ Collected {len(audit_data['checks'])} system checks",
        extra={"total_checks": len(audit_data["checks"])},
    )

    # ========== PHASE 2: Save Full Data to File ==========
    flow_logger.info("Phase 2: Saving full data to file...")

    config.output_dir.mkdir(parents=True, exist_ok=True)
    file_path: Path = config.output_dir / f"audit_{audit_hostname}_{timestamp}_full.json"

    # Sync task - Prefect handles automatically in thread pool from async flow
    full_json: str = await asyncio.to_thread(
        export_to_json, audit_data, file_path, config.pretty_print
    )
    full_size: int = calculate_size(full_json)

    flow_logger.info(
        f"💾 Full data saved to: {file_path} ({format_size(full_size)})",
        extra={"file_path": str(file_path), "file_size": full_size},
    )

    # ========== PHASE 3: Determine Send Strategy ==========
    flow_logger.info("Phase 3: Determining transmission strategy...")

    send_mode: SendMode = config.send_mode

    if send_mode == SendMode.AUTO:
        if full_size <= config.max_payload_size:
            send_mode = SendMode.FULL
            flow_logger.info(
                f"📊 Auto mode: Using FULL (size acceptable: {format_size(full_size)})",
                extra={"send_mode": "full", "size": full_size},
            )
        elif full_size <= config.max_payload_size * 5:
            send_mode = SendMode.CHUNKED
            flow_logger.info(
                f"📊 Auto mode: Using CHUNKED (size moderate: {format_size(full_size)})",
                extra={"send_mode": "chunked", "size": full_size},
            )
        else:
            send_mode = SendMode.SUMMARY
            flow_logger.info(
                f"📊 Auto mode: Using SUMMARY (size too large: {format_size(full_size)})",
                extra={"send_mode": "summary", "size": full_size},
            )
    else:
        flow_logger.info(
            f"📊 Using configured send mode: {send_mode.value}",
            extra={"send_mode": send_mode.value},
        )

    # ========== PHASE 4: Transmit Data ==========
    flow_logger.info(
        f"Phase 4: Transmitting data via {config.transmission_method.value}...",
        extra={"transmission_method": config.transmission_method.value},
    )

    success: bool = False

    match config.transmission_method:
        case TransmissionMethod.WEBHOOK:
            if not config.webhook_url:
                error_msg = "webhook_url is required for WEBHOOK transmission"
                flow_logger.error(error_msg)
                raise ConfigurationError(error_msg)

            webhook_url_str: str = config.webhook_url

            match send_mode:
                case SendMode.FULL:
                    flow_logger.info(f"📤 Sending full data ({format_size(full_size)})...")
                    success = await send_via_webhook(
                        full_json,
                        webhook_url_str,
                        compressed=config.compress,
                        max_retries=config.max_retries,
                        timeout=config.http_timeout,
                    )

                case SendMode.SUMMARY:
                    flow_logger.info("📤 Creating and sending summary...")
                    
                    # Sync task - run in thread pool
                    summary: SummaryData = await asyncio.to_thread(
                        create_summary, audit_data
                    )
                    summary.full_data_file = file_path
                    summary.full_data_size = full_size

                    summary_json: str = json.dumps(
                        summary.to_dict(), default=json_serializer
                    )
                    summary_size: int = calculate_size(summary_json)

                    reduction_percent: float = (
                        (full_size - summary_size) * 100 / full_size if full_size > 0 else 0
                    )
                    flow_logger.info(
                        f"📊 Summary size: {format_size(summary_size)} "
                        f"({reduction_percent:.1f}% reduction)",
                        extra={
                            "summary_size": summary_size,
                            "full_size": full_size,
                            "reduction_percent": reduction_percent,
                        },
                    )

                    success = await send_via_webhook(
                        summary_json,
                        webhook_url_str,
                        compressed=config.compress,
                        max_retries=config.max_retries,
                        timeout=config.http_timeout,
                    )

                case SendMode.TRUNCATED:
                    flow_logger.info("📤 Truncating and sending data...")
                    
                    # Sync task - run in thread pool
                    truncated_data: AuditData = await asyncio.to_thread(
                        truncate_output, audit_data, config.max_output_length
                    )

                    truncated_json: str = json.dumps(
                        truncated_data, default=json_serializer
                    )
                    truncated_size: int = calculate_size(truncated_json)

                    reduction_percent = (
                        (full_size - truncated_size) * 100 / full_size
                        if full_size > 0
                        else 0
                    )
                    flow_logger.info(
                        f"📊 Truncated size: {format_size(truncated_size)} "
                        f"({reduction_percent:.1f}% reduction)",
                        extra={
                            "truncated_size": truncated_size,
                            "full_size": full_size,
                            "reduction_percent": reduction_percent,
                        },
                    )

                    success = await send_via_webhook(
                        truncated_json,
                        webhook_url_str,
                        compressed=config.compress,
                        max_retries=config.max_retries,
                        timeout=config.http_timeout,
                    )

                case SendMode.CHUNKED:
                    flow_logger.info("📤 Chunking and sending data...")
                    
                    # Sync task - run in thread pool
                    chunks: list[ChunkData] = await asyncio.to_thread(
                        chunk_data, audit_data, config.max_chunk_size
                    )

                    flow_logger.info(
                        f"📦 Created {len(chunks)} chunks",
                        extra={"num_chunks": len(chunks)},
                    )

                    success = await send_chunked_webhook(chunks, webhook_url_str, config)

                case _:
                    error_msg = f"Unsupported send mode: {send_mode}"
                    flow_logger.error(error_msg, extra={"send_mode": send_mode.value})
                    raise ValueError(error_msg)

        case TransmissionMethod.FILE:
            # File transmission already completed in Phase 2
            flow_logger.info("📤 File transmission complete (saved in Phase 2)")
            success = True

        case TransmissionMethod.S3 | TransmissionMethod.FTP | TransmissionMethod.EMAIL | TransmissionMethod.SYSLOG:
            flow_logger.warning(
                f"Transmission method {config.transmission_method.value} not implemented",
                extra={"transmission_method": config.transmission_method.value},
            )
            flow_logger.info("💡 Data is available in file for manual transmission")
            success = False

        case _:
            error_msg = f"Unknown transmission method: {config.transmission_method}"
            flow_logger.error(
                error_msg,
                extra={"transmission_method": str(config.transmission_method)},
            )
            raise ValueError(error_msg)

    # ========== PHASE 5: Report Results ==========
    if success:
        flow_logger.info(
            f"✅ Audit data successfully transmitted via {config.transmission_method.value}",
            extra={
                "transmission_method": config.transmission_method.value,
                "send_mode": send_mode.value,
                "success": True,
            },
        )
    else:
        flow_logger.error(
            f"❌ Failed to transmit audit data via {config.transmission_method.value}",
            extra={
                "transmission_method": config.transmission_method.value,
                "send_mode": send_mode.value,
                "success": False,
            },
        )
        flow_logger.info(
            f"💡 Full data is available at: {file_path}",
            extra={"file_path": str(file_path)},
        )

    return {
        "audit_data": audit_data,
        "transmission_success": success,
        "send_mode": send_mode.value,
        "full_data_path": str(file_path),
        "full_data_size": full_size,
        "hostname": audit_hostname,
        "timestamp": timestamp,
    }


# ============================================================================
# ENTRY POINT
# ============================================================================


async def main() -> None:
    """
    Main entry point for system audit execution.

    Demonstrates usage of the system audit flow with various configurations.
    Uncomment examples to test different modes.

    Note:
        - AUTO mode is recommended for most use cases
        - FILE mode useful for testing without webhook
        - CHUNKED mode for very large datasets
        - SUMMARY mode for quick overview transmission
        - Supports environment variable WEBHOOK_URL for secure configuration

    Environment Variables:
        - WEBHOOK_URL: Webhook URL for transmission (overrides config)

    Example:
        $ export WEBHOOK_URL="https://example.com/webhook"
        $ python audit_tool.py
    """
    # Example 1: Auto mode with defaults (recommended)
    config: AuditConfiguration = AuditConfiguration(
        export_format=ExportFormat.JSON,
        transmission_method=TransmissionMethod.WEBHOOK,
        webhook_url=os.getenv("WEBHOOK_URL", "https://webhook.site/your-unique-id"),
        send_mode=SendMode.AUTO,
        compress=True,
        max_concurrent_commands=10,  # Adjust based on system resources
    )

    result: dict[str, Any] = await system_audit_flow(config)

    print("\n" + "=" * 70)
    print("AUDIT COMPLETE")
    print("=" * 70)
    print(f"Hostname: {result['hostname']}")
    print(f"Timestamp: {result['timestamp']}")
    print(f"Send Mode: {result['send_mode']}")
    print(f"Data Size: {format_size(result['full_data_size'])}")
    print(f"Data File: {result['full_data_path']}")
    print(f"Transmission: {'✓ Success' if result['transmission_success'] else '✗ Failed'}")
    print("=" * 70)

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
