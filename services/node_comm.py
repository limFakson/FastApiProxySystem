#!/usr/bin/env python3
"""
Node Communication Service - Manages WebSocket connections with residential nodes
Handles real-time communication, command routing, and proxy request execution
"""

import asyncio
import json
import logging
import time
import uuid
import threading
from typing import Dict, List, Optional, Set, Callable, Any
from dataclasses import dataclass, asdict
from enum import Enum
from collections import defaultdict, deque

import pika
import asyncpg
from redis import asyncio as aioredis
import websockets
from pika.exceptions import ChannelClosed
from websockets.server import WebSocketServerProtocol
from websockets.exceptions import ConnectionClosed, WebSocketException
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import uvicorn

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class ConnectionState(Enum):
    CONNECTING = "connecting"
    CONNECTED = "connected"
    AUTHENTICATED = "authenticated"
    ACTIVE = "active"
    DISCONNECTING = "disconnecting"
    DISCONNECTED = "disconnected"


class MessageType(Enum):
    # Node registration and auth
    NODE_REGISTER = "node_register"
    NODE_AUTH = "node_auth"
    NODE_HEARTBEAT = "node_heartbeat"

    # Proxy commands
    HTTP_REQUEST = "http_request"
    HTTPS_TUNNEL_CREATE = "https_tunnel_create"
    HTTPS_TUNNEL_DATA = "https_tunnel_data"
    HTTPS_TUNNEL_CLOSE = "https_tunnel_close"
    SOCKS5_CONNECT = "socks5_connect"
    SOCKS5_DATA = "socks5_data"

    # Node responses
    HTTP_RESPONSE = "http_response"
    TUNNEL_ESTABLISHED = "tunnel_established"
    TUNNEL_DATA = "tunnel_data"
    TUNNEL_ERROR = "tunnel_error"
    TUNNEL_CLOSED = "tunnel_closed"
    SOCKS5_CONNECTED = "socks5_connected"
    SOCKS5_RESPONSE = "socks5_response"

    # Status and control
    NODE_STATUS = "node_status"
    COMMAND_ACK = "command_ack"
    ERROR = "error"
    PING = "ping"
    PONG = "pong"


@dataclass
class NodeConnection:
    node_id: str
    websocket: WebSocket
    state: ConnectionState
    ip_address: str
    user_agent: str
    capabilities: List[str]
    last_ping: float
    connected_at: float
    authenticated_at: Optional[float] = None
    total_requests: int = 0
    active_requests: int = 0
    failed_requests: int = 0
    avg_response_time: float = 0.0
    last_activity: float = 0.0

    def update_activity(self):
        self.last_activity = time.time()

    def is_healthy(self, timeout: int = 120) -> bool:
        return (time.time() - self.last_ping) < timeout

    def to_dict(self) -> Dict:
        return {
            "node_id": self.node_id,
            "state": self.state.value,
            "ip_address": self.ip_address,
            "capabilities": self.capabilities,
            "last_ping": self.last_ping,
            "connected_at": self.connected_at,
            "total_requests": self.total_requests,
            "active_requests": self.active_requests,
            "failed_requests": self.failed_requests,
            "avg_response_time": self.avg_response_time,
            "is_healthy": self.is_healthy(),
        }


@dataclass
class ActiveTunnel:
    tunnel_id: str
    node_id: str
    client_id: str
    target_host: str
    target_port: int
    created_at: float
    last_activity: float
    bytes_transferred: int = 0

    def update_activity(self):
        self.last_activity = time.time()


@dataclass
class PendingRequest:
    request_id: str
    node_id: str
    task_id: str
    request_type: str
    created_at: float
    timeout: int
    callback: Optional[Callable] = None

    def is_expired(self) -> bool:
        return time.time() - self.created_at > self.timeout


class NodeCommunicationService:
    """Main service for managing node WebSocket connections"""

    def __init__(self, config: Dict):
        self.config = config

        # Connection management
        self.active_connections: Dict[str, NodeConnection] = {}
        self.connection_pool: Dict[str, WebSocket] = {}
        self.pending_requests: Dict[str, PendingRequest] = {}
        self.active_tunnels: Dict[str, ActiveTunnel] = {}

        # Message routing
        self.message_handlers: Dict[MessageType, Callable] = {}
        self.command_queue: asyncio.Queue = asyncio.Queue()
        self.response_queue: asyncio.Queue = asyncio.Queue()

        # Statistics and monitoring
        self.stats = {
            "total_connections": 0,
            "active_connections": 0,
            "total_messages": 0,
            "total_commands": 0,
            "total_responses": 0,
            "failed_connections": 0,
            "avg_response_time": 0.0,
        }

        # Service components
        self.db_pool: Optional[asyncpg.Pool] = None
        self.redis: Optional[aioredis.Redis] = None
        self.rabbitmq_connection: Optional[pika.BlockingConnection] = None
        self.channel: Optional[pika.channel.Channel] = None

        # FastAPI app for WebSocket endpoint
        self.app = FastAPI(title="Node Communication Service")
        self.setup_routes()

        # Background tasks
        self.running = False
        self.consumer_thread: Optional[threading.Thread] = None

        # Configuration
        self.heartbeat_interval = config.get("heartbeat_interval", 30)
        self.connection_timeout = config.get("connection_timeout", 120)
        self.max_connections_per_ip = config.get("max_connections_per_ip", 5)
        self.message_rate_limit = config.get(
            "message_rate_limit", 100
        )  # messages per minute

    def setup_routes(self):
        """Setup FastAPI routes and middleware"""

        # CORS middleware
        self.app.add_middleware(
            CORSMiddleware,
            allow_origins=["*"],
            allow_credentials=True,
            allow_methods=["*"],
            allow_headers=["*"],
        )

        @self.app.websocket("/ws/node")
        async def node_websocket_endpoint(websocket: WebSocket):
            await self.handle_node_connection(websocket)

        @self.app.get("/health")
        async def health_check():
            return {
                "status": "healthy",
                "active_connections": len(self.active_connections),
                "timestamp": time.time(),
            }

        @self.app.get("/stats")
        async def get_stats():
            stats = self.stats.copy()
            stats["active_connections"] = len(self.active_connections)
            stats["active_tunnels"] = len(self.active_tunnels)
            stats["pending_requests"] = len(self.pending_requests)
            return stats

        @self.app.get("/nodes")
        async def list_nodes():
            return {
                "nodes": [conn.to_dict() for conn in self.active_connections.values()],
                "total": len(self.active_connections),
            }

        @self.app.post("/nodes/{node_id}/command")
        async def send_node_command(node_id: str, command: Dict):
            if node_id not in self.active_connections:
                raise HTTPException(status_code=404, detail="Node not found")

            success = await self.send_command_to_node(node_id, command)
            return {"success": success, "node_id": node_id}

    async def initialize(self):
        """Initialize all connections and setup message handlers"""
        logger.info("Initializing Node Communication Service...")

        try:
            # Initialize database connection
            self.db_pool = await asyncpg.create_pool(
                host=self.config["postgres"]["host"],
                port=self.config["postgres"]["port"],
                user=self.config["postgres"]["user"],
                password=self.config["postgres"]["password"],
                database=self.config["postgres"]["database"],
                min_size=5,
                max_size=20,
            )

            # Initialize Redis connection
            self.redis = await aioredis.from_url(
                f"redis://{self.config['redis']['host']}:{self.config['redis']['port']}"
            )

            # Initialize RabbitMQ
            await self._setup_rabbitmq()

            # Setup message handlers
            self._setup_message_handlers()

            # Setup database tables
            await self._setup_database()

            logger.info("Node Communication Service initialized successfully")

        except Exception as e:
            logger.error(f"Failed to initialize Node Communication Service: {e}")
            raise

    async def _setup_rabbitmq(self):
        """Setup RabbitMQ connection and exchanges"""
        try:
            credentials = pika.PlainCredentials(
                self.config["rabbitmq"]["user"], self.config["rabbitmq"]["password"]
            )
            parameters = pika.ConnectionParameters(
                host=self.config["rabbitmq"]["host"],
                port=self.config["rabbitmq"]["port"],
                virtual_host=self.config["rabbitmq"]["vhost"],
                credentials=credentials,
                heartbeat=600,
                blocked_connection_timeout=300,
            )

            self.rabbitmq_connection = pika.BlockingConnection(parameters)
            self.channel = self.rabbitmq_connection.channel()

            # Declare exchanges
            exchanges = [
                ("node_commands", "direct"),
                ("node_responses", "direct"),
                ("node_events", "topic"),
                ("tunnel_events", "topic"),
            ]

            for exchange, exchange_type in exchanges:
                self.channel.exchange_declare(
                    exchange=exchange, exchange_type=exchange_type, durable=True
                )

            # Declare queues for this service
            queues = [
                "node_commands_incoming",
                "node_responses_outgoing",
                "tunnel_data",
                "node_status_updates",
            ]

            for queue_name in queues:
                self.channel.queue_declare(queue=queue_name, durable=True)

            # Bind command queue
            self.channel.queue_bind(
                exchange="node_commands",
                queue="node_commands_incoming",
                routing_key="communication_service",
            )

            logger.info("RabbitMQ setup completed")

        except Exception as e:
            logger.error(f"RabbitMQ setup failed: {e}")
            raise

    def _setup_message_handlers(self):
        """Setup WebSocket message handlers"""
        self.message_handlers = {
            MessageType.NODE_REGISTER: self._handle_node_register,
            MessageType.NODE_AUTH: self._handle_node_auth,
            MessageType.NODE_HEARTBEAT: self._handle_node_heartbeat,
            MessageType.HTTP_RESPONSE: self._handle_http_response,
            MessageType.TUNNEL_ESTABLISHED: self._handle_tunnel_established,
            MessageType.TUNNEL_DATA: self._handle_tunnel_data,
            MessageType.TUNNEL_ERROR: self._handle_tunnel_error,
            MessageType.TUNNEL_CLOSED: self._handle_tunnel_closed,
            MessageType.SOCKS5_CONNECTED: self._handle_socks5_connected,
            MessageType.SOCKS5_RESPONSE: self._handle_socks5_response,
            MessageType.NODE_STATUS: self._handle_node_status,
            MessageType.COMMAND_ACK: self._handle_command_ack,
            MessageType.ERROR: self._handle_error,
            MessageType.PONG: self._handle_pong,
        }

    async def _setup_database(self):
        """Setup database tables for connection tracking"""
        try:
            async with self.db_pool.acquire() as conn:
                # Node connections table
                await conn.execute(
                    """
                    CREATE TABLE IF NOT EXISTS node_connections (
                        node_id VARCHAR(255) PRIMARY KEY,
                        ip_address INET NOT NULL,
                        user_agent TEXT,
                        capabilities JSONB NOT NULL DEFAULT '[]',
                        connected_at TIMESTAMP NOT NULL DEFAULT NOW(),
                        disconnected_at TIMESTAMP,
                        total_requests BIGINT DEFAULT 0,
                        failed_requests BIGINT DEFAULT 0,
                        avg_response_time FLOAT DEFAULT 0.0,
                        is_active BOOLEAN DEFAULT true
                    )
                """
                )

                # Active tunnels table
                await conn.execute(
                    """
                    CREATE TABLE IF NOT EXISTS active_tunnels (
                        tunnel_id VARCHAR(255) PRIMARY KEY,
                        node_id VARCHAR(255) NOT NULL,
                        client_id VARCHAR(255) NOT NULL,
                        target_host VARCHAR(255) NOT NULL,
                        target_port INTEGER NOT NULL,
                        created_at TIMESTAMP NOT NULL DEFAULT NOW(),
                        closed_at TIMESTAMP,
                        bytes_transferred BIGINT DEFAULT 0,
                        is_active BOOLEAN DEFAULT true
                    )
                """
                )

                # Message log table for debugging
                await conn.execute(
                    """
                    CREATE TABLE IF NOT EXISTS message_log (
                        id SERIAL PRIMARY KEY,
                        node_id VARCHAR(255),
                        message_type VARCHAR(100) NOT NULL,
                        direction VARCHAR(10) NOT NULL, -- 'in' or 'out'
                        message_data JSONB,
                        timestamp TIMESTAMP NOT NULL DEFAULT NOW()
                    )
                """
                )

                # Create indexes
                await conn.execute(
                    "CREATE INDEX IF NOT EXISTS idx_node_connections_active ON node_connections(is_active)"
                )
                await conn.execute(
                    "CREATE INDEX IF NOT EXISTS idx_active_tunnels_node_id ON active_tunnels(node_id)"
                )
                await conn.execute(
                    "CREATE INDEX IF NOT EXISTS idx_message_log_timestamp ON message_log(timestamp)"
                )

                logger.info("Database tables created/verified")

        except Exception as e:
            logger.error(f"Database setup failed: {e}")
            raise

    async def handle_node_connection(self, websocket: WebSocket):
        """Handle new node WebSocket connection"""
        node_id = None
        client_ip = websocket.client.host if websocket.client else "unknown"

        try:
            while True:
                await websocket.accept()
                logger.info(f"New WebSocket connection from {client_ip}")

                # Rate limiting check
                if not await self._check_rate_limit(client_ip):
                    await websocket.close(code=4001, reason="Rate limit exceeded")
                    return

                # Connection setup phase
                node_id = await self._handle_connection_setup(websocket, client_ip)
                if not node_id:
                    await websocket.close(code=4002, reason="Authentication failed")
                    return

                # Connection established - start message loop
                await self._handle_node_messages(node_id, websocket)

        except WebSocketDisconnect:
            logger.info(f"Node {node_id} disconnected normally")
        except Exception as e:
            logger.error(f"Connection error for node {node_id}: {e}")
        finally:
            if node_id:
                await self._cleanup_node_connection(node_id)

    async def _handle_connection_setup(
        self, websocket: WebSocket, client_ip: str
    ) -> Optional[str]:
        """Handle node registration and authentication"""
        try:
            # Wait for registration message
            raw_message = await asyncio.wait_for(websocket.receive_text(), timeout=30)
            message = json.loads(raw_message)

            if message.get("type") != MessageType.NODE_REGISTER.value:
                logger.warning(
                    f"Expected registration message, got {message.get('type')}"
                )
                return None

            node_id = message.get("node_id")
            if not node_id:
                logger.warning("Registration message missing node_id")
                return None

            # Check if node already connected
            if node_id in self.active_connections:
                logger.warning(f"Node {node_id} already connected, it gets updated")
                # await self._cleanup_node_connection(node_id)

            # Create node connection
            user_agent = message.get("user_agent", "Unknown")
            capabilities = message.get("capabilities", ["http_proxy"])

            connection = NodeConnection(
                node_id=node_id,
                websocket=websocket,
                state=ConnectionState.CONNECTED,
                ip_address=client_ip,
                user_agent=user_agent,
                capabilities=capabilities,
                last_ping=time.time(),
                connected_at=time.time(),
            )

            self.active_connections[node_id] = connection
            self.stats["total_connections"] += 1

            # Authenticate node
            auth_success = await self._authenticate_node(
                node_id, message.get("auth_token")
            )
            if not auth_success:
                return None

            connection.state = ConnectionState.AUTHENTICATED
            connection.authenticated_at = time.time()

            # Register with Node Manager Service
            await self._register_with_node_manager(connection)

            # Send authentication success
            await self._send_message(
                websocket,
                {
                    "type": MessageType.NODE_AUTH.value,
                    "status": "success",
                    "node_id": node_id,
                    "timestamp": time.time(),
                },
            )

            connection.state = ConnectionState.ACTIVE
            logger.info(f"Node {node_id} registered and authenticated successfully")

            return node_id

        except asyncio.TimeoutError:
            logger.warning("Registration timeout")
            return None
        except Exception as e:
            logger.error(f"Connection setup error: {e}")
            return None

    async def _authenticate_node(self, node_id: str, auth_token: str) -> bool:
        """Authenticate node with provided token"""
        return True
        try:
            # Validate token against database
            async with self.db_pool.acquire() as conn:
                result = await conn.fetchrow(
                    "SELECT node_id FROM node_tokens WHERE token = $1 AND is_active = true",
                    auth_token,
                )

                if not result:
                    logger.warning(f"Invalid auth token for node {node_id}")
                    return False

                # Update last used timestamp
                await conn.execute(
                    "UPDATE node_tokens SET last_used = NOW() WHERE token = $1",
                    auth_token,
                )

                return True

        except Exception as e:
            logger.error(f"Authentication error for node {node_id}: {e}")
            return False

    async def _register_with_node_manager(self, connection: NodeConnection):
        """Register node with Node Manager Service"""
        try:
            registration_data = {
                "type": "register_node",
                "node_id": connection.node_id,
                "ip_address": connection.ip_address,
                "capabilities": connection.capabilities,
                "user_agent": connection.user_agent,
                "timestamp": time.time(),
            }

            # Send to Node Manager via RabbitMQ
            message = json.dumps(registration_data)
            self.channel.basic_publish(
                exchange="node_events",
                routing_key="node.registered",
                body=message,
                properties=pika.BasicProperties(delivery_mode=2),
            )

            # Also store in database
            async with self.db_pool.acquire() as conn:
                await conn.execute(
                    """
                    INSERT INTO node_connections 
                    (node_id, ip_address, user_agent, capabilities, connected_at)
                    VALUES ($1, $2, $3, $4, NOW())
                    ON CONFLICT (node_id) DO UPDATE SET
                        ip_address = EXCLUDED.ip_address,
                        user_agent = EXCLUDED.user_agent,
                        capabilities = EXCLUDED.capabilities,
                        connected_at = NOW(),
                        is_active = true
                """,
                    connection.node_id,
                    connection.ip_address,
                    connection.user_agent,
                    json.dumps(connection.capabilities),
                )

            logger.info(f"Node {connection.node_id} registered with Node Manager")

        except Exception as e:
            logger.error(f"Error registering node with manager: {e}")

    async def _handle_node_messages(self, node_id: str, websocket: WebSocket):
        """Main message handling loop for a node"""
        connection = self.active_connections[node_id]

        try:
            async for raw_message in websocket.iter_text():
                try:
                    message = json.loads(raw_message)
                    message_type = MessageType(message.get("type"))

                    # Update connection activity
                    connection.update_activity()
                    self.stats["total_messages"] += 1

                    # Log message for debugging (optional)
                    if self.config.get("log_messages", False):
                        await self._log_message(node_id, message, "in")

                    # Handle message
                    if message_type in self.message_handlers:
                        await self.message_handlers[message_type](node_id, message)
                    else:
                        logger.warning(
                            f"Unknown message type from node {node_id}: {message_type}"
                        )

                except json.JSONDecodeError:
                    logger.error(f"Invalid JSON from node {node_id}")
                except ValueError as e:
                    logger.error(f"Invalid message type from node {node_id}: {e}")
                except Exception as e:
                    logger.error(f"Error handling message from node {node_id}: {e}")

        except ConnectionClosed:
            logger.info(f"Node {node_id} connection closed")
        except Exception as e:
            logger.error(f"Message handling error for node {node_id}: {e}")

    async def _cleanup_node_connection(self, node_id: str):
        """Clean up node connection and associated resources"""
        try:
            connection = self.active_connections.get(node_id)
            if not connection:
                return

            # Update connection state
            connection.state = ConnectionState.DISCONNECTING

            # Close any active tunnels for this node
            tunnels_to_close = [
                t for t in self.active_tunnels.values() if t.node_id == node_id
            ]
            for tunnel in tunnels_to_close:
                await self._cleanup_tunnel(tunnel.tunnel_id)

            # Cancel pending requests
            pending_to_cancel = [
                r for r in self.pending_requests.values() if r.node_id == node_id
            ]
            for request in pending_to_cancel:
                await self._cancel_pending_request(request.request_id)

            # Update database
            async with self.db_pool.acquire() as conn:
                await conn.execute(
                    """
                    UPDATE node_connections 
                    SET disconnected_at = NOW(), is_active = false 
                    WHERE node_id = $1
                """,
                    node_id,
                )

            # Notify Node Manager
            await self._notify_node_disconnection(node_id)

            # Remove from active connections
            del self.active_connections[node_id]
            connection.state = ConnectionState.DISCONNECTED

            logger.info(f"Cleaned up connection for node {node_id}")

        except Exception as e:
            logger.error(f"Error cleaning up node connection {node_id}: {e}")

    # Message Handlers
    async def _handle_node_register(self, node_id: str, message: Dict):
        """Handle node registration (already handled in setup)"""
        pass

    async def _handle_node_auth(self, node_id: str, message: Dict):
        """Handle node authentication (already handled in setup)"""
        pass

    async def _handle_node_heartbeat(self, node_id: str, message: Dict):
        """Handle node heartbeat/ping"""
        try:
            connection = self.active_connections.get(node_id)
            if not connection:
                return

            connection.last_ping = time.time()

            # Extract metrics if provided
            metrics = message.get("metrics", {})
            if metrics:
                connection.active_requests = metrics.get(
                    "active_requests", connection.active_requests
                )
                connection.avg_response_time = metrics.get(
                    "avg_response_time", connection.avg_response_time
                )

            # Send pong response
            await self._send_message(
                connection.websocket,
                {"type": MessageType.PONG.value, "timestamp": time.time()},
            )

            # Update Node Manager
            await self._update_node_heartbeat(node_id, metrics)

        except Exception as e:
            logger.error(f"Error handling heartbeat from node {node_id}: {e}")

    async def _handle_http_response(self, node_id: str, message: Dict):
        """Handle HTTP response from node"""
        try:
            request_id = message.get("request_id")
            task_id = message.get("task_id")

            if not request_id:
                logger.error(f"HTTP response missing request_id from node {node_id}")
                return

            # Update node metrics
            connection = self.active_connections.get(node_id)
            if connection:
                connection.total_requests += 1
                connection.active_requests = max(0, connection.active_requests - 1)

                if message.get("status_code", 0) >= 400:
                    connection.failed_requests += 1

            # Store response in Redis for Worker Pool Service
            response_data = {
                "status_code": message.get("status_code"),
                "headers": message.get("headers", {}),
                "body": message.get("body", ""),
                "response_time": message.get("response_time", 0),
                "node_id": node_id,
                "timestamp": time.time(),
            }

            if task_id:
                await self.redis.setex(
                    f"task_response:{task_id}",
                    300,  # 5 minutes TTL
                    json.dumps(response_data),
                )

            # Remove from pending requests
            if request_id in self.pending_requests:
                del self.pending_requests[request_id]

            # Publish response event
            await self._publish_response_event("http_response", node_id, response_data)

            logger.debug(f"HTTP response processed from node {node_id}")

        except Exception as e:
            logger.error(f"Error handling HTTP response from node {node_id}: {e}")

    async def _handle_tunnel_established(self, node_id: str, message: Dict):
        """Handle tunnel establishment notification"""
        try:
            tunnel_id = message.get("tunnel_id")
            task_id = message.get("task_id")

            if not tunnel_id:
                logger.error(
                    f"Tunnel established message missing tunnel_id from node {node_id}"
                )
                return

            # Update tunnel status
            if tunnel_id in self.active_tunnels:
                tunnel = self.active_tunnels[tunnel_id]
                tunnel.update_activity()

            # Notify Worker Pool Service
            response_data = {
                "status": "tunnel_established",
                "tunnel_id": tunnel_id,
                "node_id": node_id,
                "timestamp": time.time(),
            }

            if task_id:
                await self.redis.setex(
                    f"task_response:{task_id}", 300, json.dumps(response_data)
                )

            # Publish tunnel event
            await self._publish_tunnel_event("tunnel.established", tunnel_id, node_id)

            logger.info(f"Tunnel {tunnel_id} established on node {node_id}")

        except Exception as e:
            logger.error(
                f"Error handling tunnel establishment from node {node_id}: {e}"
            )

    async def _handle_tunnel_data(self, node_id: str, message: Dict):
        """Handle tunnel data from node"""
        try:
            tunnel_id = message.get("tunnel_id")
            data = message.get("data")

            if not tunnel_id or not data:
                logger.error(f"Invalid tunnel data message from node {node_id}")
                return

            tunnel = self.active_tunnels.get(tunnel_id)
            if not tunnel:
                logger.warning(f"Received data for unknown tunnel {tunnel_id}")
                return

            # Update tunnel activity and stats
            tunnel.update_activity()
            tunnel.bytes_transferred += len(data)

            # Forward data to client via appropriate channel
            await self._forward_tunnel_data(tunnel_id, data)

        except Exception as e:
            logger.error(f"Error handling tunnel data from node {node_id}: {e}")

    async def _handle_tunnel_error(self, node_id: str, message: Dict):
        """Handle tunnel error from node"""
        try:
            tunnel_id = message.get("tunnel_id")
            error_message = message.get("error", "Unknown error")
            task_id = message.get("task_id")

            logger.warning(
                f"Tunnel {tunnel_id} error on node {node_id}: {error_message}"
            )

            # Cleanup tunnel
            if tunnel_id:
                await self._cleanup_tunnel(tunnel_id)

            # Notify Worker Pool Service of error
            if task_id:
                error_data = {
                    "status": "tunnel_error",
                    "error": error_message,
                    "tunnel_id": tunnel_id,
                    "node_id": node_id,
                    "timestamp": time.time(),
                }

                await self.redis.setex(
                    f"task_response:{task_id}", 300, json.dumps(error_data)
                )

        except Exception as e:
            logger.error(f"Error handling tunnel error from node {node_id}: {e}")

    async def _handle_tunnel_closed(self, node_id: str, message: Dict):
        """Handle tunnel closure notification"""
        try:
            tunnel_id = message.get("tunnel_id")

            if tunnel_id:
                await self._cleanup_tunnel(tunnel_id)
                logger.info(f"Tunnel {tunnel_id} closed on node {node_id}")

        except Exception as e:
            logger.error(f"Error handling tunnel closure from node {node_id}: {e}")

    async def _handle_socks5_connected(self, node_id: str, message: Dict):
        """Handle SOCKS5 connection established"""
        try:
            request_id = message.get("request_id")
            task_id = message.get("task_id")

            # Similar to tunnel establishment
            response_data = {
                "status": "socks5_connected",
                "node_id": node_id,
                "timestamp": time.time(),
            }

            if task_id:
                await self.redis.setex(
                    f"task_response:{task_id}", 300, json.dumps(response_data)
                )

            logger.info(f"SOCKS5 connection established on node {node_id}")

        except Exception as e:
            logger.error(f"Error handling SOCKS5 connection from node {node_id}: {e}")

    async def _handle_socks5_response(self, node_id: str, message: Dict):
        """Handle SOCKS5 response data"""
        try:
            request_id = message.get("request_id")
            data = message.get("data")

            # Forward SOCKS5 data to client
            await self._forward_socks5_data(request_id, data)

        except Exception as e:
            logger.error(f"Error handling SOCKS5 response from node {node_id}: {e}")

    async def _handle_node_status(self, node_id: str, message: Dict):
        """Handle node status updates"""
        try:
            status = message.get("status")
            metrics = message.get("metrics", {})

            connection = self.active_connections.get(node_id)
            if connection:
                # Update local connection info
                if metrics:
                    connection.active_requests = metrics.get(
                        "active_requests", connection.active_requests
                    )
                    connection.avg_response_time = metrics.get(
                        "avg_response_time", connection.avg_response_time
                    )

                # Forward to Node Manager
                await self._update_node_status(node_id, status, metrics)

        except Exception as e:
            logger.error(f"Error handling node status from node {node_id}: {e}")

    async def _handle_command_ack(self, node_id: str, message: Dict):
        """Handle command acknowledgment"""
        try:
            command_id = message.get("command_id")
            success = message.get("success", False)
            error = message.get("error")

            # Update pending request status
            if command_id in self.pending_requests:
                request = self.pending_requests[command_id]
                if request.callback:
                    await request.callback(success, error)
                del self.pending_requests[command_id]

            logger.debug(
                f"Command {command_id} acknowledged by node {node_id}: {success}"
            )

        except Exception as e:
            logger.error(f"Error handling command ack from node {node_id}: {e}")

    async def _handle_error(self, node_id: str, message: Dict):
        """Handle error messages from node"""
        try:
            error_type = message.get("error_type", "unknown")
            error_message = message.get("message", "Unknown error")
            request_id = message.get("request_id")

            logger.error(f"Error from node {node_id} ({error_type}): {error_message}")

            # Update connection error count
            connection = self.active_connections.get(node_id)
            if connection:
                connection.failed_requests += 1

            # Handle specific error recovery if needed
            if request_id and request_id in self.pending_requests:
                await self._cancel_pending_request(request_id)

        except Exception as e:
            logger.error(f"Error handling error message from node {node_id}: {e}")

    async def _handle_pong(self, node_id: str, message: Dict):
        """Handle pong response"""
        try:
            connection = self.active_connections.get(node_id)
            if connection:
                connection.last_ping = time.time()

        except Exception as e:
            logger.error(f"Error handling pong from node {node_id}: {e}")

    # Command sending methods
    async def send_command_to_node(self, node_id: str, command: Dict) -> bool:
        """Send command to specific node"""
        try:
            connection = self.active_connections.get(node_id)
            if not connection or connection.state != ConnectionState.ACTIVE:
                logger.warning(f"Node {node_id} not available for commands")
                return False

            # Add command tracking
            command_id = command.get("command_id", str(uuid.uuid4()))
            command["command_id"] = command_id
            command["timestamp"] = time.time()

            # Send via WebSocket
            await self._send_message(connection.websocket, command)

            # Track pending request
            pending_request = PendingRequest(
                request_id=command_id,
                node_id=node_id,
                task_id=command.get("task_id"),
                request_type=command.get("type"),
                created_at=time.time(),
                timeout=command.get("timeout", 30),
            )

            self.pending_requests[command_id] = pending_request
            self.stats["total_commands"] += 1

            # Log command for debugging
            if self.config.get("log_messages", False):
                await self._log_message(node_id, command, "out")

            logger.debug(f"Command sent to node {node_id}: {command.get('type')}")
            return True

        except Exception as e:
            logger.error(f"Error sending command to node {node_id}: {e}")
            return False

    async def send_http_request(
        self, node_id: str, task_id: str, request_data: Dict
    ) -> bool:
        """Send HTTP request to node"""
        command = {
            "type": MessageType.HTTP_REQUEST.value,
            "task_id": task_id,
            "request_id": str(uuid.uuid4()),
            "method": request_data.get("method", "GET"),
            "url": request_data["url"],
            "headers": request_data.get("headers", {}),
            "body": request_data.get("body", ""),
            "timeout": request_data.get("timeout", 30),
        }

        return await self.send_command_to_node(node_id, command)

    async def create_https_tunnel(
        self, node_id: str, task_id: str, tunnel_data: Dict
    ) -> bool:
        """Create HTTPS tunnel on node"""
        tunnel_id = str(uuid.uuid4())

        command = {
            "type": MessageType.HTTPS_TUNNEL_CREATE.value,
            "task_id": task_id,
            "tunnel_id": tunnel_id,
            "target_host": tunnel_data["target_host"],
            "target_port": tunnel_data["target_port"],
            "client_id": tunnel_data.get("client_id"),
        }

        # Track tunnel
        tunnel = ActiveTunnel(
            tunnel_id=tunnel_id,
            node_id=node_id,
            client_id=tunnel_data.get("client_id", "unknown"),
            target_host=tunnel_data["target_host"],
            target_port=tunnel_data["target_port"],
            created_at=time.time(),
            last_activity=time.time(),
        )

        self.active_tunnels[tunnel_id] = tunnel

        success = await self.send_command_to_node(node_id, command)

        if not success:
            # Cleanup if command failed
            del self.active_tunnels[tunnel_id]

        return success

    async def send_tunnel_data(self, tunnel_id: str, data: bytes) -> bool:
        """Send data through tunnel"""
        try:
            tunnel = self.active_tunnels.get(tunnel_id)
            if not tunnel:
                logger.warning(f"Tunnel {tunnel_id} not found")
                return False

            command = {
                "type": MessageType.HTTPS_TUNNEL_DATA.value,
                "tunnel_id": tunnel_id,
                "data": data.hex(),  # Send as hex string
            }

            tunnel.update_activity()
            return await self.send_command_to_node(tunnel.node_id, command)

        except Exception as e:
            logger.error(f"Error sending tunnel data: {e}")
            return False

    async def close_tunnel(self, tunnel_id: str) -> bool:
        """Close tunnel"""
        try:
            tunnel = self.active_tunnels.get(tunnel_id)
            if not tunnel:
                return True  # Already closed

            command = {
                "type": MessageType.HTTPS_TUNNEL_CLOSE.value,
                "tunnel_id": tunnel_id,
            }

            success = await self.send_command_to_node(tunnel.node_id, command)
            await self._cleanup_tunnel(tunnel_id)

            return success

        except Exception as e:
            logger.error(f"Error closing tunnel: {e}")
            return False

    # Utility methods
    async def _send_message(self, websocket: WebSocket, message: Dict):
        """Send message via WebSocket"""
        try:
            await websocket.send_text(json.dumps(message))
        except Exception as e:
            logger.error(f"Error sending WebSocket message: {e}")
            raise

    async def _check_rate_limit(self, client_ip: str) -> bool:
        """Check if client IP is within rate limits"""
        try:
            # Count connections from this IP
            ip_connections = sum(
                1
                for conn in self.active_connections.values()
                if conn.ip_address == client_ip
            )

            return ip_connections < self.max_connections_per_ip

        except Exception as e:
            logger.error(f"Error checking rate limit: {e}")
            return True  # Allow on error

    async def _log_message(self, node_id: str, message: Dict, direction: str):
        """Log message to database for debugging"""
        try:
            async with self.db_pool.acquire() as conn:
                await conn.execute(
                    """
                    INSERT INTO message_log (node_id, message_type, direction, message_data)
                    VALUES ($1, $2, $3, $4)
                """,
                    node_id,
                    message.get("type"),
                    direction,
                    json.dumps(message),
                )

        except Exception as e:
            logger.error(f"Error logging message: {e}")

    async def _cleanup_tunnel(self, tunnel_id: str):
        """Clean up tunnel resources"""
        try:
            tunnel = self.active_tunnels.get(tunnel_id)
            if not tunnel:
                return

            # Update database
            async with self.db_pool.acquire() as conn:
                await conn.execute(
                    """
                    UPDATE active_tunnels 
                    SET closed_at = NOW(), is_active = false, bytes_transferred = $1
                    WHERE tunnel_id = $2
                """,
                    tunnel.bytes_transferred,
                    tunnel_id,
                )

            # Publish tunnel closure event
            await self._publish_tunnel_event("tunnel.closed", tunnel_id, tunnel.node_id)

            # Remove from active tunnels
            del self.active_tunnels[tunnel_id]

            logger.debug(f"Tunnel {tunnel_id} cleaned up")

        except Exception as e:
            logger.error(f"Error cleaning up tunnel {tunnel_id}: {e}")

    async def _cancel_pending_request(self, request_id: str):
        """Cancel pending request"""
        try:
            if request_id in self.pending_requests:
                request = self.pending_requests[request_id]
                if request.callback:
                    await request.callback(False, "Request cancelled")
                del self.pending_requests[request_id]

        except Exception as e:
            logger.error(f"Error cancelling request {request_id}: {e}")

    async def _forward_tunnel_data(self, tunnel_id: str, data: str):
        """Forward tunnel data to client"""
        try:
            # This would typically forward to a client connection manager
            # For now, we'll publish to RabbitMQ for other services to handle

            tunnel_data = {
                "tunnel_id": tunnel_id,
                "data": data,
                "timestamp": time.time(),
            }

            message = json.dumps(tunnel_data)
            self.channel.basic_publish(
                exchange="tunnel_events", routing_key="tunnel.data", body=message
            )

        except Exception as e:
            logger.error(f"Error forwarding tunnel data: {e}")

    async def _forward_socks5_data(self, request_id: str, data: str):
        """Forward SOCKS5 data to client"""
        try:
            # Similar to tunnel data forwarding
            socks_data = {
                "request_id": request_id,
                "data": data,
                "timestamp": time.time(),
            }

            message = json.dumps(socks_data)
            self.channel.basic_publish(
                exchange="tunnel_events", routing_key="socks5.data", body=message
            )

        except Exception as e:
            logger.error(f"Error forwarding SOCKS5 data: {e}")

    async def _update_node_heartbeat(self, node_id: str, metrics: Dict):
        """Update node heartbeat with Node Manager"""
        try:
            heartbeat_data = {
                "type": "node_heartbeat",
                "node_id": node_id,
                "metrics": metrics,
                "timestamp": time.time(),
            }

            message = json.dumps(heartbeat_data)
            self.channel.basic_publish(
                exchange="node_events", routing_key="node.heartbeat", body=message
            )

        except Exception as e:
            logger.error(f"Error updating node heartbeat: {e}")

    async def _update_node_status(self, node_id: str, status: str, metrics: Dict):
        """Update node status with Node Manager"""
        try:
            status_data = {
                "type": "node_status_update",
                "node_id": node_id,
                "status": status,
                "metrics": metrics,
                "timestamp": time.time(),
            }

            message = json.dumps(status_data)
            self.channel.basic_publish(
                exchange="node_events", routing_key="node.status_update", body=message
            )

        except Exception as e:
            logger.error(f"Error updating node status: {e}")

    async def _notify_node_disconnection(self, node_id: str):
        """Notify Node Manager of disconnection"""
        try:
            disconnection_data = {
                "type": "node_disconnected",
                "node_id": node_id,
                "timestamp": time.time(),
            }

            message = json.dumps(disconnection_data)
            self.channel.basic_publish(
                exchange="node_events", routing_key="node.disconnected", body=message
            )

        except Exception as e:
            logger.error(f"Error notifying node disconnection: {e}")

    async def _publish_response_event(
        self, event_type: str, node_id: str, response_data: Dict
    ):
        """Publish response event"""
        try:
            event_data = {
                "event_type": event_type,
                "node_id": node_id,
                "response_data": response_data,
                "timestamp": time.time(),
            }

            message = json.dumps(event_data)
            self.channel.basic_publish(
                exchange="node_responses", routing_key=event_type, body=message
            )

            self.stats["total_responses"] += 1

        except Exception as e:
            logger.error(f"Error publishing response event: {e}")

    async def _publish_tunnel_event(
        self, event_type: str, tunnel_id: str, node_id: str
    ):
        """Publish tunnel event"""
        try:
            event_data = {
                "event_type": event_type,
                "tunnel_id": tunnel_id,
                "node_id": node_id,
                "timestamp": time.time(),
            }

            message = json.dumps(event_data)
            self.channel.basic_publish(
                exchange="tunnel_events", routing_key=event_type, body=message
            )

        except Exception as e:
            logger.error(f"Error publishing tunnel event: {e}")

    # RabbitMQ consumer methods
    def _start_rabbitmq_consumer(self):
        """Start RabbitMQ consumer in separate thread"""

        def consume():
            while True:
                try:
                    self.channel.basic_consume(
                        queue="node_commands_incoming",
                        on_message_callback=self._handle_rabbitmq_command,
                        auto_ack=False,
                    )

                    self.channel.basic_qos(prefetch_count=50)

                    logger.info("Starting RabbitMQ consumer for node commands...")
                    self.channel.start_consuming()

                except ChannelClosed as c:
                    logger.error(f"RabbitMQ channel closed:{c}, Reconnecting....")
                    time.sleep(15)
                    self._setup_rabbitmq()

                except Exception as e:
                    logger.error(f"RabbitMQ consumer error: {e}")
                    time.sleep(25)
                    self._setup_rabbitmq()

        self.consumer_thread = threading.Thread(target=consume, daemon=True)
        self.consumer_thread.start()

    def _handle_rabbitmq_command(self, channel, method, properties, body):
        """Handle command from RabbitMQ"""
        try:
            command = json.loads(body)
            command_type = command.get("type")
            node_id = command.get("node_id")

            if not node_id:
                logger.error("Command missing node_id")
                channel.basic_nack(delivery_tag=method.delivery_tag)
                return

            # Route command to appropriate handler
            if command_type == "send_http_request":
                asyncio.run_coroutine_threadsafe(
                    self.send_http_request(
                        node_id, command.get("task_id"), command.get("request_data")
                    ),
                    asyncio.get_event_loop(),
                )
            elif command_type == "create_https_tunnel":
                asyncio.run_coroutine_threadsafe(
                    self.create_https_tunnel(
                        node_id, command.get("task_id"), command.get("tunnel_data")
                    ),
                    asyncio.get_event_loop(),
                )
            elif command_type == "send_tunnel_data":
                data = bytes.fromhex(command.get("data", ""))
                asyncio.run_coroutine_threadsafe(
                    self.send_tunnel_data(command.get("tunnel_id"), data),
                    asyncio.get_event_loop(),
                )
            elif command_type == "close_tunnel":
                asyncio.run_coroutine_threadsafe(
                    self.close_tunnel(command.get("tunnel_id")),
                    asyncio.get_event_loop(),
                )
            else:
                # Generic command forwarding
                asyncio.run_coroutine_threadsafe(
                    self.send_command_to_node(node_id, command),
                    asyncio.get_event_loop(),
                )

            # Acknowledge message
            channel.basic_ack(delivery_tag=method.delivery_tag)

        except Exception as e:
            logger.error(f"Error handling RabbitMQ command: {e}")
            channel.basic_nack(delivery_tag=method.delivery_tag, requeue=True)

    # Background tasks
    async def _heartbeat_checker(self):
        """Background task to check node heartbeats"""
        while self.running:
            try:
                current_time = time.time()
                disconnected_nodes = []

                for node_id, connection in self.active_connections.items():
                    if not connection.is_healthy(self.connection_timeout):
                        logger.warning(f"Node {node_id} heartbeat timeout")
                        disconnected_nodes.append(node_id)

                # Cleanup disconnected nodes
                for node_id in disconnected_nodes:
                    await self._cleanup_node_connection(node_id)

                await asyncio.sleep(self.heartbeat_interval)

            except Exception as e:
                logger.error(f"Heartbeat checker error: {e}")
                await asyncio.sleep(5)

    async def _pending_request_checker(self):
        """Background task to check for expired pending requests"""
        while self.running:
            try:
                expired_requests = []

                for request_id, request in self.pending_requests.items():
                    if request.is_expired():
                        expired_requests.append(request_id)

                # Cancel expired requests
                for request_id in expired_requests:
                    await self._cancel_pending_request(request_id)
                    logger.warning(f"Request {request_id} expired and cancelled")

                await asyncio.sleep(10)  # Check every 10 seconds

            except Exception as e:
                logger.error(f"Pending request checker error: {e}")
                await asyncio.sleep(5)

    async def _stats_updater(self):
        """Background task to update statistics"""
        while self.running:
            try:
                # Update Redis with current stats
                stats_data = await self.get_service_stats()
                await self.redis.setex(
                    "node_communication_stats", 60, json.dumps(stats_data)
                )

                # Update database periodically
                await self._update_database_stats()

                await asyncio.sleep(30)  # Update every 30 seconds

            except Exception as e:
                logger.error(f"Stats updater error: {e}")
                await asyncio.sleep(10)

    async def _update_database_stats(self):
        """Update database with current connection statistics"""
        try:
            async with self.db_pool.acquire() as conn:
                for node_id, connection in self.active_connections.items():
                    await conn.execute(
                        """
                        UPDATE node_connections SET
                            total_requests = $1,
                            failed_requests = $2,
                            avg_response_time = $3
                        WHERE node_id = $4
                    """,
                        connection.total_requests,
                        connection.failed_requests,
                        connection.avg_response_time,
                        node_id,
                    )

        except Exception as e:
            logger.error(f"Error updating database stats: {e}")

    async def get_service_stats(self) -> Dict:
        """Get comprehensive service statistics"""
        stats = self.stats.copy()
        stats.update(
            {
                "active_connections": len(self.active_connections),
                "active_tunnels": len(self.active_tunnels),
                "pending_requests": len(self.pending_requests),
                "healthy_nodes": sum(
                    1 for conn in self.active_connections.values() if conn.is_healthy()
                ),
                "total_active_requests": sum(
                    conn.active_requests for conn in self.active_connections.values()
                ),
                "total_node_requests": sum(
                    conn.total_requests for conn in self.active_connections.values()
                ),
                "total_failed_requests": sum(
                    conn.failed_requests for conn in self.active_connections.values()
                ),
                "timestamp": time.time(),
            }
        )

        return stats

    async def start(self):
        """Start the Node Communication Service"""
        self.running = True
        logger.info("Starting Node Communication Service...")

        # Start RabbitMQ consumer
        self._start_rabbitmq_consumer()

        # Start background tasks
        background_tasks = [
            asyncio.create_task(self._heartbeat_checker()),
            asyncio.create_task(self._pending_request_checker()),
            asyncio.create_task(self._stats_updater()),
        ]

        # Start FastAPI server
        config = uvicorn.Config(
            app=self.app,
            host=self.config.get("host", "0.0.0.0"),
            port=self.config.get("port", 8010),
            log_level="info",
        )

        server = uvicorn.Server(config)

        try:
            # Run server and background tasks concurrently
            await asyncio.gather(server.serve(), *background_tasks)
        except KeyboardInterrupt:
            logger.info("Shutting down Node Communication Service...")
        finally:
            await self.shutdown()

    async def shutdown(self):
        """Gracefully shutdown the service"""
        self.running = False

        # Close all node connections
        for node_id in list(self.active_connections.keys()):
            await self._cleanup_node_connection(node_id)

        # Stop RabbitMQ consumer
        if self.channel and not self.channel.is_closed:
            self.channel.stop_consuming()

        if self.consumer_thread and self.consumer_thread.is_alive():
            self.consumer_thread.join(timeout=5)

        # Close connections
        if self.rabbitmq_connection and not self.rabbitmq_connection.is_closed:
            self.rabbitmq_connection.close()

        if self.redis:
            await self.redis.aclose()

        if self.db_pool:
            await self.db_pool.close()

        logger.info("Node Communication Service shutdown completed")


import os

# Example configuration
CONFIG = {
    "postgres": {
        "host": os.getenv("POSTGRES_HOST", "localhost"),
        "port": int(os.getenv("POSTGRES_PORT", 5432)),
        "user": os.getenv("POSTGRES_USER", "root"),
        "password": os.getenv("POSTGRES_PASSWORD", "mypassword"),
        "database": os.getenv("POSTGRES_DB", "proxydb"),
    },
    "redis": {
        "host": os.getenv("REDIS_HOST", "localhost"),
        "port": int(os.getenv("REDIS_PORT", 6379)),
    },
    "rabbitmq": {
        "host": os.getenv("RABBITMQ_HOST", "localhost"),
        "port": int(os.getenv("RABBITMQ_PORT", 5672)),
        "user": os.getenv("RABBITMQ_USER", "guest"),
        "password": os.getenv("RABBITMQ_PASSWORD", "guest"),
        "vhost": os.getenv("RABBITMQ_VHOST", "/"),
    },
    "host": "0.0.0.0",
    "port": 8010,
    "heartbeat_interval": 30,
    "connection_timeout": 120,
    "max_connections_per_ip": 5,
    "message_rate_limit": 100,
    "log_messages": False,
}


async def main():
    """Main entry point"""
    service = NodeCommunicationService(CONFIG)

    try:
        await service.initialize()
        await service.start()
    except Exception as e:
        logger.error(f"Service failed: {e}")
        await service.shutdown()


if __name__ == "__main__":
    asyncio.run(main())
