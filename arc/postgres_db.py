import asyncpg
import uuid
import time
import os
from datetime import datetime

# PostgreSQL connection details
DB_USER = os.getenv("DB_USER", "admin")
DB_PASSWORD = os.getenv("DB_PASSWORD", "secret")
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "")
DB_NAME = os.getenv("DB_NAME", "mydb")

# DB_DSN = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
DB_DSN = "postgresql://root:mypassword@localhost/proxydb"


# === INIT ===
async def init_db():
    conn = await asyncpg.connect(dsn=DB_DSN)
    try:
        # Tokens Table
        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS tokens (
                token TEXT PRIMARY KEY,
                is_active BOOLEAN DEFAULT TRUE,
                created_at TEXT
            )
            """
        )

        # Nodes Table
        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS nodes (
                node_id TEXT PRIMARY KEY,
                is_online BOOLEAN DEFAULT TRUE,
                last_seen TEXT
            )
            """
        )

        # Sessions Table
        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS sessions (
                id SERIAL PRIMARY KEY,
                method TEXT,
                url TEXT,
                client_ip TEXT,
                node_id TEXT,
                status_code INTEGER,
                timestamp TEXT
            )
            """
        )
    finally:
        await conn.close()


# === TOKEN MANAGEMENT ===
async def validate_token(token: str) -> bool:
    conn = await asyncpg.connect(dsn=DB_DSN)
    try:
        row = await conn.fetchrow(
            "SELECT is_active FROM tokens WHERE token = $1", token
        )
        return bool(row and row["is_active"])
    finally:
        await conn.close()


async def add_token(token: str):
    conn = await asyncpg.connect(dsn=DB_DSN)
    try:
        await conn.execute(
            "INSERT INTO tokens (token, is_active, created_at) VALUES ($1, TRUE, $2)",
            token,
            datetime.utcnow().isoformat(),
        )
    finally:
        await conn.close()


async def disable_token(token: str):
    conn = await asyncpg.connect(dsn=DB_DSN)
    try:
        await conn.execute(
            "UPDATE tokens SET is_active = FALSE WHERE token = $1", token
        )
    finally:
        await conn.close()


# === NODE MANAGEMENT ===
async def get_available_nodes():
    conn = await asyncpg.connect(dsn=DB_DSN)
    try:
        rows = await conn.fetch(
            "SELECT node_id FROM nodes WHERE is_online = TRUE ORDER BY last_seen DESC"
        )
        return [row["node_id"] for row in rows]
    finally:
        await conn.close()


async def get_node_details(node_id):
    conn = await asyncpg.connect(dsn=DB_DSN)
    try:
        row = await conn.fetchrow("SELECT * FROM nodes WHERE node_id = $1", node_id)
        return row
    finally:
        await conn.close()


async def update_node_status(node_id: str, is_online: bool):
    conn = await asyncpg.connect(dsn=DB_DSN)
    try:
        await conn.execute(
            """
            INSERT INTO nodes (node_id, is_online, last_seen)
            VALUES ($1, $2, $3)
            ON CONFLICT (node_id) DO UPDATE SET is_online = EXCLUDED.is_online, last_seen = EXCLUDED.last_seen
            """,
            node_id,
            is_online,
            datetime.utcnow().isoformat(),
        )
    finally:
        await conn.close()


# === SESSION LOGGING ===
async def log_session(
    method: str, url: str, client_ip: str, node_id: str, status_code: int
):
    conn = await asyncpg.connect(dsn=DB_DSN)
    try:
        await conn.execute(
            """
            INSERT INTO sessions (method, url, client_ip, node_id, status_code, timestamp)
            VALUES ($1, $2, $3, $4, $5, $6)
            """,
            method,
            url,
            client_ip,
            node_id,
            status_code,
            datetime.utcnow().isoformat(),
        )
    finally:
        await conn.close()


async def add_node(node_id: str):
    conn = await asyncpg.connect(dsn=DB_DSN)
    try:
        await conn.execute(
            "INSERT INTO nodes (node_id, last_seen) VALUES ($1, $2) ON CONFLICT (node_id) DO UPDATE SET last_seen = EXCLUDED.last_seen",
            node_id,
            datetime.utcnow().isoformat(),
        )
    finally:
        await conn.close()


async def update_node_ping(node_id: str):
    await add_node(node_id)
