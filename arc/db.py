import aiosqlite
import uuid
import time
import os
from datetime import datetime

DB_PATH = os.path.join(os.path.dirname(__file__), "proxy.db")


# === INIT ===
async def init_db():
    async with aiosqlite.connect(DB_PATH) as conn:
        c = await conn.cursor()

        # Tokens Table
        await c.execute(
            """
        CREATE TABLE IF NOT EXISTS tokens (
            token TEXT PRIMARY KEY,
            is_active BOOLEAN DEFAULT 1,
            created_at TEXT
        )
        """
        )

        # Nodes Table
        await c.execute(
            """
        CREATE TABLE IF NOT EXISTS nodes (
            node_id TEXT PRIMARY KEY,
            is_online BOOLEAN DEFAULT 1,
            last_seen TEXT
        )
        """
        )

        # Sessions Table
        await c.execute(
            """
        CREATE TABLE IF NOT EXISTS sessions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            method TEXT,
            url TEXT,
            client_ip TEXT,
            node_id TEXT,
            status_code INTEGER,
            timestamp TEXT
        )
        """
        )

        await c.execute("PRAGMA journal_mode=WAL;")
        await conn.commit()


# === TOKEN MANAGEMENT ===
async def validate_token(token: str) -> bool:
    async with aiosqlite.connect(DB_PATH) as conn:
        c = await conn.cursor()
        await c.execute("SELECT is_active FROM tokens WHERE token = ?", (token,))
        row = await c.fetchone()
        return bool(row and row[0])


async def add_token(token: str):
    async with aiosqlite.connect(DB_PATH) as conn:
        c = await conn.cursor()
        await c.execute(
            "INSERT OR REPLACE INTO tokens (token, is_active, created_at) VALUES (?, 1, ?)",
            (token, datetime.utcnow().isoformat()),
        )
        await conn.commit()


async def disable_token(token: str):
    async with aiosqlite.connect(DB_PATH) as conn:
        c = await conn.cursor()
        await c.execute("UPDATE tokens SET is_active = 0 WHERE token = ?", (token,))
        await conn.commit()


# === NODE MANAGEMENT ===
async def get_available_nodes():
    async with aiosqlite.connect(DB_PATH) as conn:
        c = await conn.cursor()
        await c.execute(
            "SELECT node_id FROM nodes WHERE is_online = 1 ORDER BY last_seen DESC"
        )
        return [row[0] for row in await c.fetchall()]


async def get_node_details(node_id):
    async with aiosqlite.connect(DB_PATH) as conn:
        c = await conn.cursor()
        await c.execute("SELECT * FROM nodes WHERE node_id = ?", (node_id,))
        result = await c.fetchall()
        return result[0]


async def update_node_status(node_id: str, is_online: bool):
    async with aiosqlite.connect(DB_PATH) as conn:
        c = await conn.cursor()
        await c.execute(
            """
        INSERT INTO nodes (node_id, is_online, last_seen)
        VALUES (?, ?, ?)
        ON CONFLICT(node_id) DO UPDATE SET is_online=excluded.is_online, last_seen=excluded.last_seen
        """,
            (node_id, int(is_online), datetime.utcnow().isoformat()),
        )
        await conn.commit()


# === SESSION LOGGING ===
async def log_session(
    method: str, url: str, client_ip: str, node_id: str, status_code: int
):
    async with aiosqlite.connect(DB_PATH) as conn:
        c = await conn.cursor()
        await c.execute(
            """
        INSERT INTO sessions (method, url, client_ip, node_id, status_code, timestamp)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
            (
                method,
                url,
                client_ip,
                node_id,
                status_code,
                datetime.utcnow().isoformat(),
            ),
        )
        await conn.commit()


async def add_node(node_id: str):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT OR REPLACE INTO nodes (node_id, last_seen) VALUES (?, ?)",
            (node_id, datetime.utcnow().isoformat()),
        )
        await db.commit()


async def update_node_ping(node_id: str):
    await add_node(node_id)
