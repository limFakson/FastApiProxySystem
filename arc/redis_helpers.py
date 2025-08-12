import json
import redis
import time
import uuid
import logging

# Setup logger
logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(levelname)s: %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

# Connect to Redis
redis_client = redis.StrictRedis(host="localhost", port=6379, decode_responses=True)


# ---------------------------
# Queue Operations
# ---------------------------
def push_request_to_queue(request: dict, ttl: int = 60) -> str:
    """
    Push a request to the Redis queue with a TTL.
    Returns the generated request_id.
    """
    request_id = str(uuid.uuid4())
    request_data = {
        "id": request_id,
        "timestamp": time.time(),
        **{k: str(v) for k, v in request.items()},
    }

    # Push request ID to the queue
    redis_client.rpush("request_queue", request_id)

    # Store request data in a hash with a TTL
    redis_client.hset(f"request_data:{request_id}", mapping=request_data)
    redis_client.expire(f"request_data:{request_id}", ttl)

    log.info(f"Pushed request {request_id} to queue with TTL={ttl}s")
    return request_id


def pull_request_from_queue(timeout: int = 0) -> dict | None:
    """
    Pull a request from the Redis queue.
    If timeout=0, blocks until an item is available.
    """
    try:
        result = redis_client.blpop("request_queue", timeout=timeout)
        if result:
            request_id = result[1]
            request_data = redis_client.hgetall(f"request_data:{request_id}")

            if request_data:
                redis_client.delete(f"request_data:{request_id}")
                log.info(f"Pulled request {request_id} from queue")
                return request_data
            else:
                log.warning(f"No data found for request_id={request_id}")
        return None
    except Exception as e:
        log.exception("Error pulling request from queue")
        return None


# ---------------------------
# Connected Nodes
# ---------------------------
def update_connected_node(node_id: str, node_details: dict):
    redis_client.hset("connected_nodes", node_id, json.dumps(node_details))
    log.info(f"Updated connected node {node_id}")


def get_redis_node_details(node_id: str) -> dict | None:
    node_data = redis_client.hget("connected_nodes", node_id)
    return json.loads(node_data) if node_data else None


def get_all_connected_nodes() -> dict:
    nodes_data = redis_client.hgetall("connected_nodes")
    return {k: json.loads(v) for k, v in nodes_data.items()} if nodes_data else {}


def delete_connected_node(node_id: str):
    redis_client.hdel("connected_nodes", node_id)
    log.info(f"Deleted connected node {node_id}")


def patch_connected_node(node_id: str, patch_details: dict):
    current_details = get_redis_node_details(node_id)
    if current_details is None:
        log.warning(f"Node {node_id} not found.")
        return
    current_details.update(patch_details)
    update_connected_node(node_id, current_details)
    log.info(f"Patched node {node_id}")


# ---------------------------
# Tunnels
# ---------------------------
def add_tunnel_to_redis(tunnel_id: str, tunnel_data: dict):
    redis_client.hset(f"tunnel:{tunnel_id}", mapping=tunnel_data)
    log.info(f"Tunnel '{tunnel_id}' added to Redis")


def delete_tunnel_from_redis(tunnel_id: str):
    redis_client.delete(f"tunnel:{tunnel_id}")
    log.info(f"Tunnel '{tunnel_id}' deleted from Redis")


def get_tunnel_from_redis(tunnel_id: str) -> dict | None:
    tunnel_data = redis_client.hgetall(f"tunnel:{tunnel_id}")
    return tunnel_data if tunnel_data else None


# ---------------------------
# Active Requests (Local Memory)
# ---------------------------
active_requests = {}


def add_active_request(request_id: str, future):
    active_requests[request_id] = future
    log.info(f"Active request '{request_id}' added")


def delete_active_request(request_id: str):
    if request_id in active_requests:
        del active_requests[request_id]
        log.info(f"Active request '{request_id}' deleted")


def get_active_request(request_id: str):
    return active_requests.get(request_id)
