# load environment variables to os
from dotenv import load_dotenv

load_dotenv()

from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Request, Query
from fastapi.responses import JSONResponse
from arc.db import (
    add_token,
    disable_token,
    add_node,
    update_node_status,
)
from arc.redis_helpers import (
    patch_connected_node,
    update_connected_node,
    delete_connected_node,
    delete_tunnel_from_redis,
    get_tunnel_from_redis,
    get_active_request,
    delete_active_request,
)
from arc.modals import TokenRequest
import json
import asyncio
import queue
import time
import pika

app = FastAPI()
# connected_nodes = {}  # node_id -> websocket
# active_requests = {}  # request_id -> asyncio.Future
# active_tunnels = {}  # tunnel_id -> (client_writer, websocket)

# RabbitMQ connection
parameters = pika.ConnectionParameters(
    host="localhost",
    port=5672,
    virtual_host="/",
    credentials=pika.PlainCredentials("guest", "guest"),
)

# Establish connection
connection = pika.BlockingConnection(parameters)
channel = connection.channel()
channel.queue_declare(queue="websocket_queue")


def callback(ch, method, properties, body):
    data = json.loads(body)
    node_id = data["node_id"]
    message = data["message"]
    if node_id in websocket_objects:
        asyncio.run(send_to_websocket(node_id, message))


channel.basic_consume(
    queue="websocket_queue", on_message_callback=callback, auto_ack=True
)

websocket_objects = {}

request_queue = queue.Queue()


@app.websocket("/ws")
async def node_websocket(websocket: WebSocket):
    await websocket.accept()
    node_id = None

    try:
        while True:
            data = await websocket.receive_json()

            if data["type"] == "register":
                node_id = data["node_id"]
                update_connected_node(
                    node_id,
                    {
                        "last_ping": time.time(),
                    },
                )
                patch_connected_node(node_id, {"status": "free"})
                await add_node(node_id)
                print(f"✅ Node '{node_id}' connected and registered in DB")

            elif data["type"] == "ping":
                node_id = data["node_id"]
                patch_connected_node(node_id, {"last_ping": time.time()})
                await update_node_status(node_id, True)
                print(f"🔁 Ping from node '{node_id}', connection updated")

            elif data["type"] == "http-response":
                request_id = data["request_id"]
                patch_connected_node(node_id, {"status": "free"})
                if future := get_active_request(request_id):
                    delete_active_request(request_id)
                    future.set_result(data)

            elif data["type"] == "https-tunnel-ready":
                tunnel_id = data["tunnel_id"]
                tunnel_data = get_tunnel_from_redis(tunnel_id)
                writer = tunnel_data.get("write", None)
                _ = tunnel_data.get("websocker", None)
                if writer:
                    writer.write(b"HTTP/1.1 200 Connection Established\r\n\r\n")
                    await writer.drain()

            elif data["type"] == "https-tunnel-data":
                tunnel_id = data["tunnel_id"]
                tunnel_data = get_tunnel_from_redis(tunnel_id)
                writer = tunnel_data.get("writer", None)
                if writer:
                    try:
                        writer.write(bytes.fromhex(data["data"]))
                        await writer.drain()
                    except (ConnectionResetError, BrokenPipeError) as e:
                        print(f"❌ Client disconnected: {e}")
                        writer.close()
                        return

                patch_connected_node(node_id, {"status": "free"})

            elif data["type"] == "https-tunnel-error":
                tunnel_id = data["tunnel_id"]
                tunnel_data = get_tunnel_from_redis(tunnel_id)
                writer = tunnel_data.get("writer", None)
                delete_tunnel_from_redis(tunnel_id)

                if writer:
                    writer.close()

                patch_connected_node(node_id, {"status": "free"})

    except WebSocketDisconnect:
        if node_id:
            delete_connected_node(node_id)
            print(f"🔌 Node '{node_id}' disconnected")


async def send_to_websocket(node_id: str, message: str):
    if node_id in websocket_objects:
        await websocket_objects[node_id].send_json({**message})
    else:
        print(f"{node_id} not found error 404")
        pass


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


@app.on_event("startup")
async def startup_event():
    from arc.db import init_db

    await init_db()
    loop = asyncio.get_running_loop()
    loop.run_in_executor(None, channel.start_consuming)
