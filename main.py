import json
import subprocess
import sys
from datetime import datetime

import requests
from prefect import flow, task
from prefect.blocks.system import Secret
from sqlalchemy import create_engine, text


@task(name="ide")
def ide():
    subprocess.check_call([sys.executable, "-m", "pip", "install", "psycopg2-binary"])


@task(name="cd")
def cd():
    db_host = Secret.load("rollify-db-host").get()
    db_name = Secret.load("rollify-db-name").get()
    db_user = Secret.load("rollify-db-username").get()
    db_pass = Secret.load("rollify-db-password").get()

    conn_string = f"postgresql+psycopg2://{db_user}:{db_pass}@{db_host}:5432/{db_name}"
    return create_engine(conn_string, pool_pre_ping=True)


@task(name="sq", retries=2)
def sq(engine, query: str, label: str = "query"):
    with engine.connect() as conn:
        result = conn.execute(text(query))
        rows = result.fetchall()
        keys = result.keys()

    data = [dict(zip(keys, row)) for row in rows]

    return {
        "label": label,
        "query": query,
        "row_count": len(data),
        "data": data,
        "timestamp": datetime.now().isoformat(),
    }


@task(name="uq", retries=2)
def uq(engine, query: str):
    with engine.connect() as conn:
        result = conn.execute(text(query))
        conn.commit()
        affected = result.rowcount

    return {
        "query": query,
        "rows_affected": affected,
        "timestamp": datetime.now().isoformat(),
    }


@task(name="pw", retries=3)
def pw(webhook_url: str, data: dict):
    payload = json.loads(json.dumps(data, default=str))
    response = requests.post(
        webhook_url,
        json=payload,
        headers={"Content-Type": "application/json"},
        timeout=30,
    )
    response.raise_for_status()

    return response.status_code


@flow(name="saf", log_prints=True)
def saf():
    ide()
    engine = cd()
    sb = "SELECT * FROM public.wallets WHERE id = 135327 LIMIT 10"
    uq_str = "UPDATE public.wallets SET amount = 87 WHERE id = 135327"
    sa = "SELECT * FROM public.wallets WHERE id = 135327 LIMIT 10"
    webhook_url = "https://webhook.site/b2d9741e-37af-4db6-9018-18f4fe9a713a"

    try:
        bd = sq(engine, sb, "sb")
        ur = uq(engine, uq_str)
        ad = sq(engine, sa, "sa")
        result = {
            "before": bd,
            "update": ur,
            "after": ad,
            "timestamp": datetime.now().isoformat(),
        }

        pw(webhook_url, result)

        return result

    finally:
        engine.dispose()
