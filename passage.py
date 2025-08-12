# load environment variables to os
from dotenv import load_dotenv
load_dotenv()

import asyncio
import threading
import uvicorn

from services.health_checker import check_node_health
from services.queue_processor import process_request_queue
from services.tcp_handler import handle_client

async def start_tcp_proxy_server():
    server = await asyncio.start_server(handle_client, "0.0.0.0", 8880)
    print("🚀 TCP Proxy Server running on port 8880 (HTTP + HTTPS)")
    async with server:
        await server.serve_forever()


def run_fastapi():
    uvicorn.run("proxy:app", host="0.0.0.0", port=8010)


def run_health_monitor():
    asyncio.run(check_node_health())  # Start the health check task

def run_process_service():
    asyncio.run(process_request_queue())

if __name__ == "__main__":
    threading.Thread(target=run_fastapi, daemon=True).start()
    threading.Thread(target=run_health_monitor, daemon=True).start()
    threading.Thread(target=run_process_service, daemon=True).start()
    asyncio.run(start_tcp_proxy_server())
