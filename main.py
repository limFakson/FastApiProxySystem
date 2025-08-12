import sys
import asyncio
import threading
import uvicorn

from passage import (
    run_fastapi,
    start_tcp_proxy_server,
    run_health_monitor,
    process_request_queue,
)

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python main.py <service>")
        sys.exit(1)

    service = sys.argv[1]

    if service == "run_fastapi":
        uvicorn.run("proxy:app", host="0.0.0.0", port=8010)
    elif service == "start_tcp_proxy_server":
        asyncio.run(start_tcp_proxy_server())
    elif service == "run_health_monitor":
        asyncio.run(run_health_monitor())
    elif service == "process_request_queue":
        asyncio.run(process_request_queue())
    else:
        print(f"Unknown service: {service}")
        sys.exit(1)
