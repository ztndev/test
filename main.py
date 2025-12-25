@flow(
    name="system_audit_optimized",
    log_prints=True,
    retries=1,
    retry_delay_seconds=30,
)
async def system_audit_flow(config: AuditConfiguration | None = None) -> dict[str, Any]:
    """
    Optimized system audit flow with intelligent size management and transmission.
    
    [Full docstring remains the same...]
    """
    flow_logger: logging.Logger = get_run_logger()

    # Use default config if not provided
    if config is None:
        config = AuditConfiguration()
        flow_logger.info("Using default configuration")

    hostname: str = socket.gethostname()

    flow_logger.info(f"🔍 Starting system audit on {hostname}")
    flow_logger.info(f"📊 Export Format: {config.export_format.value}")
    flow_logger.info(f"📤 Transmission: {config.transmission_method.value}")
    flow_logger.info(f"📦 Send Mode: {config.send_mode.value}")

    # ========== PHASE 1: Gather System Information ==========
    flow_logger.info("Phase 1: Gathering system information...")

    # FIX: Call synchronous task directly from async flow
    audit_data: AuditData = gather_system_info(timeout=config.command_timeout)

    timestamp: str = datetime.now(UTC).strftime("%Y%m%d_%H%M%S")
    audit_hostname: str = audit_data.get("hostname", "unknown")

    flow_logger.info(f"✓ Collected {len(audit_data['checks'])} system checks")

    # ========== PHASE 2: Save Full Data to File ==========
    flow_logger.info("Phase 2: Saving full data to file...")

    config.output_dir.mkdir(parents=True, exist_ok=True)
    file_path: Path = config.output_dir / f"audit_{audit_hostname}_{timestamp}_full.json"

    # FIX: Call synchronous task directly
    full_json: str = export_to_json(audit_data, file_path, config.pretty_print)
    full_size: int = calculate_size(full_json)

    flow_logger.info(f"💾 Full data saved to: {file_path} ({format_size(full_size)})")

    # ========== PHASE 3: Determine Send Strategy ==========
    flow_logger.info("Phase 3: Determining transmission strategy...")

    send_mode: SendMode = config.send_mode

    if send_mode == SendMode.AUTO:
        if full_size <= config.max_payload_size:
            send_mode = SendMode.FULL
            flow_logger.info(
                f"📊 Auto mode: Using FULL (size acceptable: {format_size(full_size)})"
            )
        elif full_size <= config.max_payload_size * 5:
            send_mode = SendMode.CHUNKED
            flow_logger.info(
                f"📊 Auto mode: Using CHUNKED (size moderate: {format_size(full_size)})"
            )
        else:
            send_mode = SendMode.SUMMARY
            flow_logger.info(
                f"📊 Auto mode: Using SUMMARY (size too large: {format_size(full_size)})"
            )
    else:
        flow_logger.info(f"📊 Using configured send mode: {send_mode.value}")

    # ========== PHASE 4: Transmit Data ==========
    flow_logger.info(f"Phase 4: Transmitting data via {config.transmission_method.value}...")

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
                    # FIX: Call synchronous task directly
                    summary: SummaryData = create_summary(audit_data)
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
                        f"({reduction_percent:.1f}% reduction)"
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
                    # FIX: Call synchronous task directly
                    truncated_data: AuditData = truncate_output(
                        audit_data, config.max_output_length
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
                        f"({reduction_percent:.1f}% reduction)"
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
                    # FIX: Call synchronous task directly
                    chunks: list[ChunkData] = chunk_data(
                        audit_data, config.max_chunk_size
                    )

                    flow_logger.info(f"📦 Created {len(chunks)} chunks")

                    success = await send_chunked_webhook(chunks, webhook_url_str, config)

                case _:
                    error_msg = f"Unsupported send mode: {send_mode}"
                    flow_logger.error(error_msg)
                    raise ValueError(error_msg)

        case TransmissionMethod.FILE:
            # File transmission already completed in Phase 2
            flow_logger.info("📤 File transmission complete (saved in Phase 2)")
            success = True

        case TransmissionMethod.S3 | TransmissionMethod.FTP | TransmissionMethod.EMAIL | TransmissionMethod.SYSLOG:
            flow_logger.warning(
                f"Transmission method {config.transmission_method.value} not implemented"
            )
            flow_logger.info("💡 Data is available in file for manual transmission")
            success = False

        case _:
            error_msg = f"Unknown transmission method: {config.transmission_method}"
            flow_logger.error(error_msg)
            raise ValueError(error_msg)

    # ========== PHASE 5: Report Results ==========
    if success:
        flow_logger.info(
            f"✅ Audit data successfully transmitted via {config.transmission_method.value}"
        )
    else:
        flow_logger.error(
            f"❌ Failed to transmit audit data via {config.transmission_method.value}"
        )
        flow_logger.info(f"💡 Full data is available at: {file_path}")

    return {
        "audit_data": audit_data,
        "transmission_success": success,
        "send_mode": send_mode.value,
        "full_data_path": str(file_path),
        "full_data_size": full_size,
        "hostname": audit_hostname,
        "timestamp": timestamp,
    }
