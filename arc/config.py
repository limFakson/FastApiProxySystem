import os, json, signal, logging
from pathlib import Path

cfg_path = Path(__file__).with_name("config.json")


def reload():
    global cfg
    try:
        with open(cfg_path) as f:
            cfg = json.load(f)
    except FileNotFoundError:
        cfg = {
            "gateway_url": "ws://localhost:8010/ws",
            "node_id": os.getenv("NODE_ID", "node_002"),
            "tokens": os.getenv("TOKENS", "TEST_TOKEN_123").split(","),
            "ping_interval": 30,
            "node_timeout": 50,
            "chunk_size": 64 * 1024,
            "log_level": "INFO",
        }


def _sigusr1(*_):
    reload()
    logging.getLogger().info("Config reloaded")


reload()
signal.signal(signal.SIGUSR1, _sigusr1)
