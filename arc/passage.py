from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Request, Query
from fastapi.responses import JSONResponse
from db import (
    validate_token,
    log_session,
    get_available_nodes,
    add_token,
    disable_token,
    add_node,
    get_node_details,
    update_node_status,
)
from modals import TokenRequest
import uvicorn
import uuid
import threading
import asyncio
import random
import time
from urllib.parse import urlparse

app = FastAPI()
connected_nodes = {}  # node_id -> websocket
active_requests = {}  # request_id -> asyncio.Future
active_tunnels = {}  # tunnel_id -> (client_writer, websocket)

PING_INTERVAL = 30  # seconds
NODE_TIMEOUT = 60  # seconds
WORKER_COUNT = 5
REQUEST_TIMEOUT = 40


async def get_best_available_node():
    nodes = await get_available_nodes()
    random.shuffle(nodes)

    for node_id in nodes:
        node = connected_nodes.get(node_id)
        if not node:
            continue

        async with node["lock"]:
            if node["status"] == "free":
                node["status"] = "busy"
                return node_id, node["websocket"]

    return None, None


@app.websocket("/ws")
async def node_websocket(websocket: WebSocket):
    await websocket.accept()
    node_id = None

    try:
        while True:
            data = await websocket.receive_json()

            if data["type"] == "register":
                node_id = data["node_id"]
                connected_nodes[node_id] = {
                    "websocket": websocket,
                    "last_ping": time.time(),
                    "status": "free",
                    "lock": asyncio.Lock()
                }
                await add_node(node_id)
                print(f"✅ Node '{node_id}' connected and registered in DB")

            elif data["type"] == "ping":
                node_id = data["node_id"]
                connected_nodes[node_id]["last_ping"] = time.time()
                connected_nodes[node_id]["websocket"] = websocket
                await update_node_status(node_id, True)
                print(f"🔁 Ping from node '{node_id}', connection updated")

            elif data["type"] == "http-response":
                request_id = data["request_id"]
                if future := active_requests.pop(request_id, None):
                    future.set_result(data)

            elif data["type"] == "https-tunnel-ready":
                tunnel_id = data["tunnel_id"]
                writer, _ = active_tunnels.get(tunnel_id, (None, None))
                if writer:
                    writer.write(b"HTTP/1.1 200 Connection Established\r\n\r\n")
                    await writer.drain()

            elif data["type"] == "https-tunnel-data":
                tunnel_id = data["tunnel_id"]
                writer, _ = active_tunnels.get(tunnel_id, (None, None))
                if writer:
                    try:
                        writer.write(bytes.fromhex(data["data"]))
                        await writer.drain()
                    except (ConnectionResetError, BrokenPipeError) as e:
                        print(f"❌ Client disconnected: {e}")
                        writer.close()
                        return

            elif data["type"] == "https-tunnel-error":
                tunnel_id = data["tunnel_id"]
                writer, _ = active_tunnels.pop(tunnel_id, (None, None))
                if writer:
                    writer.close()

    except WebSocketDisconnect:
        if node_id:
            connected_nodes.pop(node_id, None)
            print(f"🔌 Node '{node_id}' disconnected")


@app.post("/token/create")
async def create_token(req: TokenRequest):
    await add_token(req.token)
    return {"token": req.token}


@app.post("/token/disable")
async def disable_token_api(request: Request):
    data = await request.json()
    token = data.get("token")
    if not token:
        return JSONResponse(status_code=400, content={"error": "Token required"})
    await disable_token(token)
    return {"status": "disabled"}


# ==== TCP HANDLER ====
async def handle_client(reader, writer):
    try:
        token = None
        headers = {}

        first_line = await reader.readuntil(b"\r\n")
        if not first_line:
            writer.close()
            return

        line = first_line.decode().strip()

        # === 2. Read headers ===
        while True:
            header_line = await reader.readuntil(b"\r\n")
            if header_line in (b"\r\n", b"\n", b""):
                break
            key, value = header_line.decode().strip().split(":", 1)
            headers[key.lower()] = value.strip()

        if "proxy-authorization" not in headers:
            writer.write(b"HTTP/1.1 407 Proxy Authentication Required\r\n")
            writer.write(b'Proxy-Authenticate: Basic realm="Access to proxy"\r\n')
            writer.write(b"Content-Length: 0\r\n\r\n")
            await writer.drain()
            writer.close()
            return
        # Extract token from Proxy-
        if key == "Proxy-Authorization" or key.lower() == "proxy-authorization":
            import base64

            if "basic" in value.lower():
                b64token = value[6:].strip()
                try:
                    decoded = base64.b64decode(b64token).decode()
                    token = decoded.split(":", 1)[0]  # Just token, ignore password part
                except Exception as e:
                    print(f"❌ Token decode error: {e}")

        # === 3. Validate the token ===
        if not token or not await validate_token(token):
            writer.write(b"HTTP/1.1 401 Unauthorized\r\n\r\n")
            await writer.drain()
            writer.close()
            return

        method, path, *_ = line.split()

        # HTTPS Tunneling
        if method == "CONNECT":
            target_host, target_port = path.split(":")
            target_port = int(target_port)

            for i in range(1, 4):
                node_id, websocket = await get_best_available_node()
                if node_id:
                    break

                await asyncio.sleep(2)
                
            if not node_id:
                writer.write(b"HTTP/1.1 503 Service Unavailable\r\n\r\n")
                await writer.drain()
                writer.close()
                return

            try:
                # websocket = websocket["websocket"]
                if not websocket:
                    writer.write(b"HTTP/1.1 503 Service Unavailable\r\n\r\n")
                    await writer.drain()
                    writer.close()
                    return

                tunnel_id = str(uuid.uuid4())
                active_tunnels[tunnel_id] = (writer, websocket)

                await websocket.send_json(
                    {
                        "type": "https-connect",
                        "tunnel_id": tunnel_id,
                        "host": target_host,
                        "port": target_port,
                    }
                )

                while True:
                    data = await reader.read(4096)
                    if not data:
                        break
                    await websocket.send_json(
                        {
                            "type": "https-tunnel-data",
                            "tunnel_id": tunnel_id,
                            "data": data.hex(),
                        }
                    )
                return
            except Exception as e:
                print(f"❌ Tunnel error on node {node_id}: {e}")

                writer.write(b"HTTP/1.1 502 Bad Gateway\r\n\r\n")
                await writer.drain()
                writer.close()

            finally:
                if node := connected_nodes.get(node_id):
                    async with node["lock"]:
                        node["status"] = "free"

        else:
            # HTTP forwarding
            headers = {}
            while True:
                h = await reader.readuntil(b"\r\n")
                if h in (b"\r\n", b"\n", b""):
                    break
                key, value = h.decode().strip().split(":", 1)
                headers[key.lower()] = value.strip()

            host = headers.get("host")
            url = f"http://{host}{path}"

            for i in range(1,4):
                node_id, websocket = await get_best_available_node()
                if node_id:
                    break

                await asyncio.sleep(3)

            if not node_id:
                writer.write(b"HTTP/1.1 503 Service Unavailable\r\n\r\n")
                await writer.drain()
                writer.close()
                return

            try:
                if not websocket:
                    writer.write(b"HTTP/1.1 503 Service Unavailable\r\n\r\n")
                    await writer.drain()
                    writer.close()
                    return

                request_id = str(uuid.uuid4())
                await websocket.send_json(
                    {
                        "type": "http-request",
                        "request_id": request_id,
                        "method": method,
                        "url": url,
                        "headers": headers,
                        "body": "",
                    }
                )

                future = asyncio.get_event_loop().create_future()
                active_requests[request_id] = future
                response = await asyncio.wait_for(future, timeout=20)

                await log_session(
                    method,
                    url,
                    writer.get_extra_info("peername")[0],
                    node_id,
                    response["status_code"],
                )

                writer.write(f"HTTP/1.1 {response['status_code']} OK\r\n".encode())
                for k, v in response.get("headers", {}).items():
                    writer.write(f"{k}: {v}\r\n".encode())
                writer.write(b"\r\n")
                writer.write(response.get("body", "").encode())
                await writer.drain()
                return
            except Exception as e:
                print(f"❌ HTTP proxy error on node {node_id}: {e}")

                writer.write(b"HTTP/1.1 502 Bad Gateway\r\n\r\n")
                await writer.drain()
                writer.close()

            finally:
                if node := connected_nodes.get(node_id):
                    async with node["lock"]:
                        node["status"] = "free"

    except Exception as e:
        print("🔴 Fatal proxy error:", e)
        writer.close()


async def start_tcp_proxy_server():
    from db import init_db

    await init_db()
    workers = []
    for i in range(WORKER_COUNT):  # Configurable number of workers
        server = await asyncio.start_server(
            handle_client, 
            "0.0.0.0", 
            8880,
            reuse_port=True  # Allows multiple workers to bind to same port
        )
        workers.append(server)

    print(f"🚀 TCP Proxy Server running with {WORKER_COUNT} workers on port 8880")
    await asyncio.gather(*[server.serve_forever() for server in workers])


def run_fastapi():
    uvicorn.run(app, host="0.0.0.0", port=8010)


async def check_node_health():
    from datetime import datetime

    print("Node Health Monitor Started 🚀")

    try:
        while True:
            try:
                current_time = time.time()
                nodes = await get_available_nodes()
                for node_id in nodes:
                    node_deatils = await get_node_details(node_id)
                    last_ping = node_deatils[2]
                    if connected_nodes.get(node_id) is not None:
                        last_ping = connected_nodes[node_id]["last_ping"]

                    # Convert properly
                    if isinstance(last_ping, (int, float, str)) and str(last_ping).replace('.', '', 1).isdigit():
                        last_ping_dt = datetime.fromtimestamp(float(last_ping))
                    else:
                        last_ping_dt = datetime.fromisoformat(last_ping)

                    if (
                        current_time - last_ping_dt.timestamp()
                        > NODE_TIMEOUT
                    ):
                        print(f"❌ Node '{node_id}' timed out and will be deactivated")
                        connected_nodes.pop(node_id, None)
                        await update_node_status(
                            node_id, False
                        )  # Assuming you have a function to disable the node in the DB
                    elif (
                        connected_nodes[node_id]["status"] == "busy"
                        and current_time - connected_nodes[node_id]["last_ping"]
                        > REQUEST_TIMEOUT
                    ):
                        connected_nodes[node_id]["status"] = "free"
                        await asyncio.sleep(PING_INTERVAL)
            except Exception as e:
                print(f"Error occurred in health check: {e}")
    except Exception as e:
        print(f"Error in health check: {e}")


def run_health_monitor():
    asyncio.run(check_node_health())  # Start the health check task


if __name__ == "__main__":
    threading.Thread(target=run_fastapi, daemon=True).start()
    threading.Thread(target=run_health_monitor, daemon=True).start()
    asyncio.run(start_tcp_proxy_server())
