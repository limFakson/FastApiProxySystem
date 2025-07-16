import asyncio
import websockets
import json
import httpx

GATEWAY_URL = "ws://localhost:8010/ws"
NODE_ID = "node_002"  # dynamic node identifier
MAX_RETRIES = 15
RETRY_DELAY = 8
PING_INTERVAL = 30  # seconds

# Store all active HTTPS tunnels: tunnel_id → writer
active_tunnels = {}
tunnel_tasks = {}


async def perform_http_request(data):
    try:
        async with httpx.AsyncClient() as client:
            response = await client.request(
                method=data["method"],
                url=data["url"],
                headers=data.get("headers", {}),
                data=data.get("body", None),
                timeout=30,
            )
            return {
                "type": "http-response",
                "request_id": data["request_id"],
                "status_code": response.status_code,
                "headers": dict(response.headers),
                "body": response.text,
            }
    except Exception as e:
        return {
            "type": "http-response",
            "request_id": data["request_id"],
            "error": str(e),
        }


async def tunnel_reader(tunnel_id, reader, websocket):
    try:
        while True:
            data = await reader.read(4096)
            if not data:
                break
            await websocket.send(
                json.dumps(
                    {
                        "type": "https-tunnel-data",
                        "tunnel_id": tunnel_id,
                        "data": data.hex(),
                    }
                )
            )
    except Exception as e:
        await websocket.send(
            json.dumps(
                {"type": "https-tunnel-error", "tunnel_id": tunnel_id, "error": str(e)}
            )
        )
    finally:
        writer = active_tunnels.pop(tunnel_id, None)
        if writer:
            writer.close()
        task = tunnel_tasks.pop(tunnel_id, None)
        if task and not task.done():
            task.cancel()


async def periodic_ping(websocket):
    try:
        while True:
            await asyncio.sleep(PING_INTERVAL)
            await websocket.send(json.dumps({"type": "ping", "node_id": NODE_ID}))
    except Exception as e:
        print(f"Ping sending error for: {e}")


async def handle_ws():
    retries = 0
    while retries < MAX_RETRIES:
        try:
            async with websockets.connect(
                GATEWAY_URL,
                ping_interval=60,
                ping_timeout=80,
            ) as websocket:
                # Register node
                await websocket.send(
                    json.dumps({"type": "register", "node_id": NODE_ID})
                )
                print("✅ Connected and registered")
                retries = 0  # Reset retries on successful connection

                # Start periodic ping task
                ping_task = asyncio.create_task(periodic_ping(websocket))

                while True:
                    try:
                        message = await websocket.recv()
                        data = json.loads(message)

                        if data["type"] == "http-request":
                            response = await perform_http_request(data)
                            await websocket.send(json.dumps(response))

                        elif data["type"] == "https-connect":
                            print(f"https request received {data}")
                            host = data["host"]
                            port = data["port"]
                            tunnel_id = data["tunnel_id"]

                            try:
                                reader, writer = await asyncio.open_connection(
                                    host, port
                                )
                                active_tunnels[tunnel_id] = writer

                                # Send ready
                                await websocket.send(
                                    json.dumps(
                                        {
                                            "type": "https-tunnel-ready",
                                            "tunnel_id": tunnel_id,
                                        }
                                    )
                                )

                                # Start reading from target
                                task = asyncio.create_task(
                                    tunnel_reader(tunnel_id, reader, websocket)
                                )
                                tunnel_tasks[tunnel_id] = task

                            except Exception as e:
                                await websocket.send(
                                    json.dumps(
                                        {
                                            "type": "https-tunnel-error",
                                            "tunnel_id": tunnel_id,
                                            "error": str(e),
                                        }
                                    )
                                )

                        elif data["type"] == "https-tunnel-data":
                            tunnel_id = data["tunnel_id"]
                            writer = active_tunnels.get(tunnel_id)
                            if writer:
                                writer.write(bytes.fromhex(data["data"]))
                                await writer.drain()

                    except Exception as e:
                        print(f"WebSocket error: {e}")
                        # Cancel all tunnel readers
                        await task_cancellation(tunnel_tasks.copy())
                        tunnel_tasks.clear()
                        active_tunnels.clear()
                        break  # Exit inner while loop to reconnect

        except websockets.exceptions.ConnectionClosed as e:
            print(f"WebSocket disconnected: {e}. Retrying in {RETRY_DELAY} seconds...")
        except Exception as e:
            print(
                f"WebSocket connection error: {e}. Retrying in {RETRY_DELAY} seconds..."
            )

        retries += 1
        if retries < MAX_RETRIES:
            await asyncio.sleep(RETRY_DELAY)
        else:
            print("❌ Maximum retry attempts reached. Exiting.")
            break


async def task_cancellation(tunnel_tasks):
    for tid, task in tunnel_tasks.items():
        if not task.done():
            task.cancel()


if __name__ == "__main__":
    asyncio.run(handle_ws())
