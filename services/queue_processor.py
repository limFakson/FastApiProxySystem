import asyncio
import uuid

from arc.redis_helpers import (
    pull_request_from_queue,
    add_tunnel_to_redis,
    patch_connected_node,
    add_active_request,
)
from arc.config import get_available_free_node
from manager import send_message_to_node
from arc.db import log_session
from asgiref.sync import async_to_sync


def process_request_queue():
    print("🚀 Started processing queued request service!")
    while True:
        try:
            print("request pull making")
            request = pull_request_from_queue()
            if not request:
                asyncio.sleep(1)
                continue
            print(request)

            # Get stored variables
            writer = request.get("writer")
            reader = request.get("reader")

            if request.get("method") == "CONNECT":
                target_host = request.get("target_host")
                target_port = request.get("target_port")

                for i in range(1, 3):
                    try:
                        node_data, node_id = get_available_free_node()
                        if not node_data or node_data is None:
                            continue

                        tunnel_id = str(uuid.uuid4())
                        tunnel_data = {
                            "writer": writer,
                        }
                        add_tunnel_to_redis(tunnel_id, tunnel_data)

                        # Mark the node as busy
                        patch_connected_node(node_id, {"status": "busy"})

                        send_message_to_node(
                            node_id,
                            {
                                "type": "https-connect",
                                "tunnel_id": tunnel_id,
                                "host": target_host,
                                "port": target_port,
                            },
                        )

                        while True:
                            data = reader.read(4096)
                            if not data:
                                break

                            send_message_to_node(
                                node_id,
                                {
                                    "type": "https-tunnel-data",
                                    "tunnel_id": tunnel_id,
                                    "data": data.hex(),
                                },
                            )
                        return
                    except Exception as e:
                        print(f"❌ Tunnel error on node {node_id}: {e}")
                        patch_connected_node(node_id, {"status": "free"})
                        continue

                writer.write(b"HTTP/1.1 502 Bad Gateway\r\n\r\n")
                writer.drain()
                writer.close()

            else:
                url = request.get("url")
                headers = request.get("headers")
                method = request.get("method")

                for i in range(1, 3):
                    try:
                        node_data, node_id = get_available_free_node()
                        if not node_data or node_data is None:
                            continue

                        # Mark the node as busy
                        patch_connected_node(node_id, {"status": "busy"})

                        request_id = str(uuid.uuid4())
                        send_message_to_node(
                            node_id,
                            {
                                "type": "http-request",
                                "request_id": request_id,
                                "method": method,
                                "url": url,
                                "headers": headers,
                                "body": "",
                            },
                        )

                        future = asyncio.get_event_loop().create_future()
                        add_active_request(request_id, future)
                        response = asyncio.wait_for(future, timeout=20)

                        async_to_sync(log_session)(
                            method,
                            url,
                            writer.get_extra_info("peername")[0],
                            node_id,
                            response["status_code"],
                        )

                        writer.write(
                            f"HTTP/1.1 {response['status_code']} OK\r\n".encode()
                        )
                        for k, v in response.get("headers", {}).items():
                            writer.write(f"{k}: {v}\r\n".encode())
                        writer.write(b"\r\n")
                        writer.write(response.get("body", "").encode())
                        writer.drain()
                        return
                    except Exception as e:
                        print(f"❌ HTTP proxy error on node {node_id}: {e}")
                        patch_connected_node(node_id, {"status": "free"})
                        continue

                writer.write(b"HTTP/1.1 502 Bad Gateway\r\n\r\n")
                writer.drain()
                writer.close()
        except Exception as e:
            print(e)
