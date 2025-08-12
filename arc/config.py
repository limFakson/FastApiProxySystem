import random

from asgiref.sync import async_to_sync
from .db import get_available_nodes
from .redis_helpers import get_all_connected_nodes

def get_available_free_node() -> tuple:
    available_nodes = async_to_sync(get_available_nodes)()
    connected_nodes = get_all_connected_nodes()
    
    random.shuffle(available_nodes)
    for node_id in available_nodes:
        node_details = connected_nodes.get(node_id)
        if node_details["status"] == "free":
            return node_details, node_id
        
        continue
    
    return None, None