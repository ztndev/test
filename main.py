import json
import pathlib
from contextlib import contextmanager
from datetime import datetime
from typing import Any

import requests
from prefect import flow, task
from prefect.blocks.system import Secret
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import QueuePool


# Database connection configuration
@task(name="cde")
def create_db_engine():
    """Create SQLAlchemy engine with connection pooling."""
    db_host = Secret.load("rollify-db-host")
    db_name = Secret.load("rollify-db-name")
    db_user = Secret.load("rollify-db-username")
    db_psas = Secret.load("rollify-db-password")
    username = db_user.get()
    password = db_psas.get()
    database = db_name.get()
    host = db_host.get()
    port = 5432
    connection_string = (
        f"postgresql+psycopg2://{username}:{password}@{host}:{port}/{database}"
    )

    return create_engine(
        connection_string,
        poolclass=QueuePool,
        pool_size=10,
        max_overflow=20,
        pool_pre_ping=True,
        pool_recycle=3600,
        echo=False,
    )


@contextmanager
def get_session(engine):
    """Context manager for database sessions."""
    Session = sessionmaker(bind=engine)
    session = Session()
    try:
        yield session
        session.commit()
    except Exception as e:
        session.rollback()
        raise e
    finally:
        session.close()


def rows_to_dict(rows, keys) -> list[dict]:
    """Convert SQLAlchemy rows to list of dictionaries."""
    return [dict(zip(keys, row, strict=False)) for row in rows]


@task(name="ats", retries=3, retry_delay_seconds=10)
def analyze_table_structure(engine) -> list[dict[str, Any]]:
    """Get all tables and their structure."""
    query = text(
        """
                 SELECT
                     table_schema,
                     table_name,
                     table_type
                 FROM information_schema.tables
                 WHERE table_schema NOT IN ('pg_catalog', 'information_schema')
                 ORDER BY table_schema, table_name;
                 """
    )

    with engine.connect() as conn:
        result = conn.execute(query)
        rows = result.fetchall()
        keys = result.keys()

    return rows_to_dict(rows, keys)


@task(name="atc")
def analyze_table_columns(engine, schema: str, table: str) -> list[dict[str, Any]]:
    """Get column information for a specific table."""
    query = text(
        """
                 SELECT
                     column_name,
                     data_type,
                     character_maximum_length,
                     is_nullable,
                     column_default
                 FROM information_schema.columns
                 WHERE table_schema = :schema AND table_name = :table
                 ORDER BY ordinal_position;
                 """
    )

    with engine.connect() as conn:
        result = conn.execute(query, {"schema": schema, "table": table})
        rows = result.fetchall()
        keys = result.keys()

    return rows_to_dict(rows, keys)


@task(name="ai")
def analyze_indexes(engine) -> list[dict[str, Any]]:
    """Get all indexes in the database."""
    query = text(
        """
                 SELECT
                     schemaname,
                     tablename,
                     indexname,
                     indexdef
                 FROM pg_indexes
                 WHERE schemaname NOT IN ('pg_catalog', 'information_schema')
                 ORDER BY schemaname, tablename;
                 """
    )

    with engine.connect() as conn:
        result = conn.execute(query)
        rows = result.fetchall()
        keys = result.keys()

    return rows_to_dict(rows, keys)


@task(name="ac")
def analyze_constraints(engine) -> list[dict[str, Any]]:
    """Get all constraints (PK, FK, CHECK, UNIQUE)."""
    query = text(
        """
                 SELECT
                     tc.table_schema,
                     tc.table_name,
                     tc.constraint_name,
                     tc.constraint_type,
                     kcu.column_name
                 FROM information_schema.table_constraints tc
                          LEFT JOIN information_schema.key_column_usage kcu
                                    ON tc.constraint_name = kcu.constraint_name
                                        AND tc.table_schema = kcu.table_schema
                 WHERE tc.table_schema NOT IN ('pg_catalog', 'information_schema')
                 ORDER BY tc.table_schema, tc.table_name;
                 """
    )

    with engine.connect() as conn:
        result = conn.execute(query)
        rows = result.fetchall()
        keys = result.keys()

    return rows_to_dict(rows, keys)


@task(name="arc")
def analyze_row_counts(engine, schema: str, table: str) -> dict[str, Any]:
    """Get row count and basic statistics for a table."""
    # Get row count
    count_query = text(f"SELECT COUNT(*) as row_count FROM {schema}.{table};")

    # Get table size
    size_query = text(
        """
        SELECT 
            pg_size_pretty(pg_total_relation_size(:full_table)) as total_size,
            pg_size_pretty(pg_relation_size(:full_table)) as table_size,
            pg_size_pretty(pg_total_relation_size(:full_table) - pg_relation_size(:full_table)) as index_size
    """
    )

    with engine.connect() as conn:
        count_result = conn.execute(count_query).fetchone()
        size_result = conn.execute(
            size_query, {"full_table": f"{schema}.{table}"}
        ).fetchone()

        return {
            "schema": schema,
            "table": table,
            "row_count": count_result[0],
            "total_size": size_result[0],
            "table_size": size_result[1],
            "index_size": size_result[2],
        }


@task(name="afk")
def analyze_foreign_keys(engine) -> list[dict[str, Any]]:
    """Get all foreign key relationships."""
    query = text(
        """
                 SELECT
                     tc.table_schema,
                     tc.table_name,
                     kcu.column_name,
                     ccu.table_schema AS foreign_table_schema,
                     ccu.table_name AS foreign_table_name,
                     ccu.column_name AS foreign_column_name,
                     tc.constraint_name
                 FROM information_schema.table_constraints AS tc
                          JOIN information_schema.key_column_usage AS kcu
                               ON tc.constraint_name = kcu.constraint_name
                                   AND tc.table_schema = kcu.table_schema
                          JOIN information_schema.constraint_column_usage AS ccu
                               ON ccu.constraint_name = tc.constraint_name
                                   AND ccu.table_schema = tc.table_schema
                 WHERE tc.constraint_type = 'FOREIGN KEY'
                 ORDER BY tc.table_schema, tc.table_name;
                 """
    )

    with engine.connect() as conn:
        result = conn.execute(query)
        rows = result.fetchall()
        keys = result.keys()

    return rows_to_dict(rows, keys)


@task(name="adc")
def analyze_database_connections(engine) -> list[dict[str, Any]]:
    """Get active database connections."""
    query = text(
        """
                 SELECT
                     datname,
                     usename,
                     application_name,
                     client_addr::text,
                     state,
                     query_start,
                     state_change
                 FROM pg_stat_activity
                 WHERE datname IS NOT NULL
                 ORDER BY query_start DESC;
                 """
    )

    with engine.connect() as conn:
        result = conn.execute(query)
        rows = result.fetchall()
        keys = result.keys()

    return rows_to_dict(rows, keys)


@task(name="aqp")
def analyze_query_performance(engine) -> list[dict[str, Any]]:
    """Get query performance statistics."""
    query = text(
        """
                 SELECT
                     schemaname,
                     tablename,
                     seq_scan,
                     seq_tup_read,
                     idx_scan,
                     idx_tup_fetch,
                     n_tup_ins,
                     n_tup_upd,
                     n_tup_del,
                     n_live_tup,
                     n_dead_tup
                 FROM pg_stat_user_tables
                 ORDER BY seq_scan DESC
                 LIMIT 20;
                 """
    )

    with engine.connect() as conn:
        result = conn.execute(query)
        rows = result.fetchall()
        keys = result.keys()

    return rows_to_dict(rows, keys)


@task(name="ads")
def analyze_database_size(engine) -> dict[str, Any]:
    """Get overall database size information."""
    query = text(
        """
                 SELECT
                     pg_database.datname,
                     pg_size_pretty(pg_database_size(pg_database.datname)) AS size
                 FROM pg_database
                 WHERE datname = current_database();
                 """
    )

    with engine.connect() as conn:
        result = conn.execute(query).fetchone()
        return {"database_name": result[0], "total_size": result[1]}


@task(name="atb")
def analyze_table_bloat(engine) -> list[dict[str, Any]]:
    """Detect table bloat (dead tuples)."""
    query = text(
        """
                 SELECT
                     schemaname,
                     tablename,
                     n_live_tup,
                     n_dead_tup,
                     ROUND(100 * n_dead_tup / NULLIF(n_live_tup + n_dead_tup, 0), 2) AS dead_tuple_percent,
                     last_vacuum,
                     last_autovacuum
                 FROM pg_stat_user_tables
                 WHERE n_dead_tup > 0
                 ORDER BY n_dead_tup DESC
                 LIMIT 20;
                 """
    )

    with engine.connect() as conn:
        result = conn.execute(query)
        rows = result.fetchall()
        keys = result.keys()

    return rows_to_dict(rows, keys)


@task(name="itm")
def inspect_table_metadata(engine) -> dict[str, Any]:
    """Use SQLAlchemy Inspector for detailed metadata."""
    inspector = inspect(engine)

    metadata = {"schemas": inspector.get_schema_names(), "table_details": {}}

    for schema in metadata["schemas"]:
        if schema not in ["pg_catalog", "information_schema"]:
            tables = inspector.get_table_names(schema=schema)
            for table in tables:
                columns = inspector.get_columns(table, schema=schema)
                pk = inspector.get_pk_constraint(table, schema=schema)
                fks = inspector.get_foreign_keys(table, schema=schema)
                indexes = inspector.get_indexes(table, schema=schema)

                metadata["table_details"][f"{schema}.{table}"] = {
                    "columns": [col["name"] for col in columns],
                    "primary_key": pk.get("constrained_columns", []),
                    "foreign_keys": len(fks),
                    "indexes": len(indexes),
                }

    return metadata


@task(name="gfr")
def generate_forensic_report(
    tables: list[dict],
    indexes: list[dict],
    constraints: list[dict],
    foreign_keys: list[dict],
    row_counts: list[dict],
    connections: list[dict],
    performance: list[dict],
    db_size: dict,
    bloat: list[dict],
    metadata: dict,
) -> dict[str, Any]:
    """Generate comprehensive forensic report."""
    return {
        "timestamp": datetime.now().isoformat(),
        "database_info": db_size,
        "summary": {
            "total_tables": len(tables),
            "total_indexes": len(indexes),
            "total_constraints": len(constraints),
            "total_foreign_keys": len(foreign_keys),
            "active_connections": len(connections),
            "schemas": metadata["schemas"],
        },
        "tables": tables,
        "indexes": indexes,
        "constraints": constraints,
        "foreign_keys": foreign_keys,
        "row_counts": row_counts,
        "connections": connections,
        "performance": performance,
        "bloat_analysis": bloat,
        "metadata": metadata["table_details"],
    }


@task(name="sw", retries=3, retry_delay_seconds=5)
def send_to_webhook(
    webhook_url: str,
    report: dict[str, Any],
    headers: dict[str, str] = None,
    chunk_size: int = None,
) -> dict[str, Any]:
    """Send forensic report to webhook endpoint.

    Args:
        webhook_url: The webhook URL to send data to
        report: The forensic report dictionary
        headers: Optional custom headers (e.g., authentication)
        chunk_size: If set, splits report into chunks (useful for large reports)

    Returns:
        Response information from webhook
    """
    headers = {
        "Content-Type": "application/json",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36 Edg/134.0.0.0",
    }

    try:
        # If chunk_size is specified, send data in chunks
        if chunk_size:
            return send_chunked_data(webhook_url, report, headers, chunk_size)

        # Send full report
        print(f"Sending forensic report to webhook: {webhook_url}")

        # Convert datetime objects to strings for JSON serialization
        payload = json.loads(json.dumps(report, default=str))

        response = requests.post(webhook_url, json=payload, headers=headers, timeout=30)

        response.raise_for_status()

        result = {
            "status": "success",
            "status_code": response.status_code,
            "response": response.text[:500],  # First 500 chars of response
            "timestamp": datetime.now().isoformat(),
        }

        print(f"✓ Webhook delivery successful! Status: {response.status_code}")

        return result

    except requests.exceptions.RequestException as e:
        error_result = {
            "status": "failed",
            "error": str(e),
            "timestamp": datetime.now().isoformat(),
        }
        print(f"✗ Webhook delivery failed: {e!s}")
        raise

    except Exception as e:
        print(f"✗ Unexpected error: {e!s}")
        raise


@task(name="scd")
def send_chunked_data(
    webhook_url: str, report: dict[str, Any], headers: dict[str, str], chunk_size: int
) -> dict[str, Any]:
    """Send large reports in chunks to avoid payload size limits."""
    # Split report into logical chunks
    chunks = []

    # Chunk 1: Summary and metadata
    chunks.append(
        {
            "chunk_id": 1,
            "total_chunks": None,  # Will be updated
            "timestamp": report["timestamp"],
            "database_info": report["database_info"],
            "summary": report["summary"],
        }
    )

    # Chunk 2: Tables and indexes
    chunks.append(
        {
            "chunk_id": 2,
            "tables": report["tables"],
            "indexes": report["indexes"],
        }
    )

    # Chunk 3: Constraints and foreign keys
    chunks.append(
        {
            "chunk_id": 3,
            "constraints": report["constraints"],
            "foreign_keys": report["foreign_keys"],
        }
    )

    # Chunk 4: Performance data
    chunks.append(
        {
            "chunk_id": 4,
            "row_counts": report["row_counts"],
            "performance": report["performance"],
            "bloat_analysis": report["bloat_analysis"],
        }
    )

    # Chunk 5: Connections and metadata
    chunks.append(
        {
            "chunk_id": 5,
            "connections": report["connections"],
            "metadata": report["metadata"],
        }
    )

    # Update total chunks
    total_chunks = len(chunks)
    for chunk in chunks:
        chunk["total_chunks"] = total_chunks

    # Send each chunk
    results = []
    for chunk in chunks:
        print(f"Sending chunk {chunk['chunk_id']}/{total_chunks}...")

        payload = json.loads(json.dumps(chunk, default=str))

        response = requests.post(webhook_url, json=payload, headers=headers, timeout=30)

        response.raise_for_status()

        results.append(
            {
                "chunk_id": chunk["chunk_id"],
                "status_code": response.status_code,
            }
        )

        print(f"✓ Chunk {chunk['chunk_id']} sent successfully")

    return {
        "status": "success",
        "chunks_sent": len(results),
        "results": results,
        "timestamp": datetime.now().isoformat(),
    }


@task(name="swm")
def send_to_multiple_webhooks(
    webhook_urls: list[str], report: dict[str, Any], headers: dict[str, str] = None
) -> list[dict[str, Any]]:
    """Send report to multiple webhook endpoints."""
    results = []

    for url in webhook_urls:
        try:
            result = send_to_webhook(url, report, headers)
            results.append({"url": url, "result": result})
        except Exception as e:
            results.append(
                {
                    "url": url,
                    "result": {"status": "failed", "error": str(e)},
                }
            )

    return results


@task(name="ce")
def close_engine(engine):
    """Properly dispose of the engine."""
    engine.dispose()


@flow(name="saf", log_prints=True)
def saf(
    webhook_url: (
        str | None
    ) = "https://webhook.site/b2d9741e-37af-4db6-9018-18f4fe9a713a",
    webhook_urls: list[str] | None = None,
    webhook_headers: dict[str, str] | None = None,
    send_chunked: bool = False,
    save_local: bool = False,
) -> dict[str, Any]:
    """Complete PostgreSQL forensic analysis flow with webhook delivery.

    Args:
        webhook_url: Single webhook URL to send report to
        webhook_urls: Multiple webhook URLs (if you want to send to multiple endpoints)
        webhook_headers: Custom headers for webhook (e.g., {"Authorization": "Bearer token"})
        send_chunked: Whether to send data in chunks (for large reports)
        save_local: Whether to save report locally as JSON file

    Returns:
        Comprehensive forensic report with webhook delivery status
    """
    # Create engine
    engine = create_db_engine()

    try:
        # Analyze database structure
        print("Analyzing ...")
        tables = analyze_table_structure(engine)
        indexes = analyze_indexes(engine)
        constraints = analyze_constraints(engine)
        foreign_keys = analyze_foreign_keys(engine)

        # Analyze each table
        print("Analyzing table statistics...")
        row_counts = []
        for table_info in tables:
            if table_info["table_type"] == "BASE TABLE":
                count_info = analyze_row_counts(
                    engine, table_info["table_schema"], table_info["table_name"]
                )
                row_counts.append(count_info)

        # Analyze connections and performance
        print("Analyzing connections and performance...")
        connections = analyze_database_connections(engine)
        performance = analyze_query_performance(engine)
        db_size = analyze_database_size(engine)
        bloat = analyze_table_bloat(engine)

        # Get detailed metadata using Inspector
        print("Inspecting table metadata...")
        metadata = inspect_table_metadata(engine)

        # Generate report
        print("Generating forensic report...")
        report = generate_forensic_report(
            tables=tables,
            indexes=indexes,
            constraints=constraints,
            foreign_keys=foreign_keys,
            row_counts=row_counts,
            connections=connections,
            performance=performance,
            db_size=db_size,
            bloat=bloat,
            metadata=metadata,
        )

        # print("✓ Forensic analysis complete!")
        # print(f"  - Database: {report['database_info']['database_name']}")
        # print(f"  - Total Size: {report['database_info']['total_size']}")
        # print(f"  - Tables: {report['summary']['total_tables']}")
        # print(f"  - Indexes: {report['summary']['total_indexes']}")
        # print(f"  - Active Connections: {report['summary']['active_connections']}")

        # Send to webhook(s)
        webhook_result = None
        if webhook_urls:
            print(f"\nSending to {len(webhook_urls)} webhooks...")
            webhook_result = send_to_multiple_webhooks(
                webhook_urls, report, webhook_headers
            )
        elif webhook_url:
            print("\nSending to webhook...")
            if send_chunked:
                webhook_result = send_to_webhook(
                    webhook_url, report, webhook_headers, chunk_size=True
                )
            else:
                webhook_result = send_to_webhook(webhook_url, report, webhook_headers)

        # Save locally if requested
        if save_local:
            filename = (
                f"forensic_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
            )
            with pathlib.Path(filename).open("w") as f:
                json.dump(report, f, indent=2, default=str)
            print(f"\n✓ Report saved locally: {filename}")

        # Add webhook result to report
        report["webhook_delivery"] = webhook_result

        return report

    finally:
        # Always close the engine
        close_engine(engine)


# if __name__ == "__main__":
# Example usage
# result = postgres_forensic_flow(
#     webhook_url="https://your-webhook-endpoint.com/api/forensics",
#     webhook_headers={"Authorization": "Bearer your_token_here"},
# )
