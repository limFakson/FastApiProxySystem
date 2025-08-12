import time
import asyncio

from arc.db import get_available_nodes,update_node_status, get_node_details
from arc.redis_helpers import delete_connected_node, get_redis_node_details

PING_INTERVAL = 30  # seconds
NODE_TIMEOUT = 60  # seconds

async def check_node_health():
    from datetime import datetime

    print("Node Health Monitor Started 🚀")

    try:
        while True:
            try:
                current_time = time.time()
                nodes = await get_available_nodes()
                for node_id in nodes:
                    node_details = await get_node_details(node_id)
                    last_ping = node_details[2]

                    redis_node_data = get_redis_node_details(node_id)
                    if redis_node_data is not None:
                        last_ping = redis_node_data["last_ping"]

                    # Convert properly
                    if (
                        isinstance(last_ping, (int, float, str))
                        and str(last_ping).replace(".", "", 1).isdigit()
                    ):
                        last_ping_dt = datetime.fromtimestamp(float(last_ping))
                    else:
                        last_ping_dt = datetime.fromisoformat(last_ping)

                    if current_time - last_ping_dt.timestamp() > NODE_TIMEOUT:
                        print(f"❌ Node '{node_id}' timed out and will be deactivated")
                        delete_connected_node(node_id)
                        await update_node_status(
                            node_id, False
                        )  # Assuming you have a function to disable the node in the DB
                await asyncio.sleep(PING_INTERVAL)
            except Exception as e:
                print(f"Error occurred in health check: {e}")
                await asyncio.sleep(10)
    except Exception as e:
        print(f"Error in health check: {e}")
