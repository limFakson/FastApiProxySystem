#!/usr/bin/env python3
"""
Response Handler Service - Collects and delivers proxy responses to clients
==========================================================================

This service handles the final stage of request processing:
- Collects completed responses from Worker Pool Service via RabbitMQ
- Manages response delivery to waiting clients through various channels
- Handles response caching, compression, and transformation
- Provides response analytics and monitoring
- Manages client connection state and cleanup

Compatible with existing Client Gateway, Node Communication, Worker Pool, and Node Manager services.
"""

import asyncio
import json
import logging
import time
import uuid
import gzip
import base64
import threading
from typing import Dict, List, Optional, Any, Set, Callable
from dataclasses import dataclass, asdict
from datetime import datetime, timedelta
from enum import Enum
from collections import defaultdict, deque

import pika
import asyncpg
from redis import asyncio as aioredis
from aiohttp import web, ClientSession

# from aiohttp.web import WebSocket, WSMsgType
import uvicorn
from fastapi import FastAPI, HTTPException, BackgroundTasks, WebSocket
from fastapi.responses import JSONResponse, StreamingResponse
from pydantic import BaseModel, Field

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class ResponseStatus(Enum):
    PENDING = "pending"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"
    TIMEOUT = "timeout"
    DELIVERED = "delivered"


class DeliveryMethod(Enum):
    HTTP_POLLING = "http_polling"
    HTTP_STREAMING = "http_streaming"
    WEBSOCKET = "websocket"
    CALLBACK = "callback"
    REDIS_CACHE = "redis_cache"


@dataclass
class PendingResponse:
    response_id: str
    task_id: str
    client_id: str
    request_id: str
    delivery_method: DeliveryMethod
    created_at: float
    timeout: int
    callback_url: Optional[str] = None
    websocket_id: Optional[str] = None
    response_data: Optional[Dict] = None
    status: ResponseStatus = ResponseStatus.PENDING
    delivered_at: Optional[float] = None
    error_message: Optional[str] = None
    retry_count: int = 0

    def is_expired(self) -> bool:
        return time.time() - self.created_at > self.timeout

    def can_retry(self) -> bool:
        return self.retry_count < 3 and self.status == ResponseStatus.FAILED


@dataclass
class ClientConnection:
    client_id: str
    connection_id: str
    connection_type: str  # 'websocket', 'http_streaming', 'polling'
    connected_at: float
    last_activity: float
    pending_responses: Set[str]
    websocket: Optional[WebSocket] = None

    def update_activity(self):
        self.last_activity = time.time()

    def is_active(self, timeout: int = 300) -> bool:
        return time.time() - self.last_activity < timeout


@dataclass
class ResponseMetrics:
    total_responses: int = 0
    successful_deliveries: int = 0
    failed_deliveries: int = 0
    timeout_deliveries: int = 0
    avg_delivery_time: float = 0.0
    responses_per_minute: float = 0.0
    last_updated: float = 0.0

    def update_delivery_time(self, delivery_time: float):
        if self.total_responses == 0:
            self.avg_delivery_time = delivery_time
        else:
            # Exponential moving average
            self.avg_delivery_time = 0.9 * self.avg_delivery_time + 0.1 * delivery_time


class ResponseHandlerService:
    """Main service for handling response delivery to clients"""

    def __init__(self, config: Dict):
        self.config = config

        # Response management
        self.pending_responses: Dict[str, PendingResponse] = {}
        self.client_connections: Dict[str, ClientConnection] = {}
        self.websocket_connections: Dict[str, WebSocket] = {}

        # Response queues by delivery method
        self.response_queues: Dict[DeliveryMethod, asyncio.Queue] = {
            method: asyncio.Queue() for method in DeliveryMethod
        }

        # Metrics and monitoring
        self.metrics = ResponseMetrics()
        self.delivery_times: deque = deque(maxlen=1000)

        # Service connections
        self.db_pool: Optional[asyncpg.Pool] = None
        self.redis: Optional[aioredis.Redis] = None
        self.rabbitmq_connection: Optional[pika.BlockingConnection] = None
        self.channel: Optional[pika.channel.Channel] = None

        # FastAPI app for HTTP endpoints
        self.app = FastAPI(title="Response Handler Service")
        self.setup_routes()

        # Background processing
        self.running = False
        self.consumer_thread: Optional[threading.Thread] = None
        self.response_processors: List[asyncio.Task] = []

        # Configuration
        self.default_timeout = config.get("default_response_timeout", 300)
        self.cleanup_interval = config.get("cleanup_interval", 60)
        self.max_response_size = config.get("max_response_size", 10 * 1024 * 1024)
        self.compression_threshold = config.get("compression_threshold", 1024)

    def setup_routes(self):
        """Setup FastAPI routes for response retrieval"""

        @self.app.websocket("/ws/responses/{client_id}")
        async def websocket_endpoint(websocket: WebSocket, client_id: str):
            await self.handle_websocket_connection(websocket, client_id)

        @self.app.get("/responses/{response_id}")
        async def get_response(response_id: str, client_id: str = None):
            return await self.get_response_http(response_id, client_id)

        @self.app.get("/responses/{client_id}/poll")
        async def poll_responses(client_id: str, timeout: int = 30):
            return await self.poll_client_responses(client_id, timeout)

        @self.app.get("/responses/{client_id}/stream")
        async def stream_responses(client_id: str):
            return StreamingResponse(
                self.stream_client_responses(client_id), media_type="text/plain"
            )

        @self.app.post("/responses/register")
        async def register_response_listener(request: Dict):
            return await self.register_response_listener(request)

        @self.app.get("/health")
        async def health_check():
            return {
                "status": "healthy",
                "pending_responses": len(self.pending_responses),
                "active_connections": len(self.client_connections),
                "timestamp": time.time(),
            }

        @self.app.get("/stats")
        async def get_stats():
            return await self.get_service_stats()

        @self.app.delete("/responses/{response_id}")
        async def cancel_response(response_id: str, client_id: str):
            return await self.cancel_pending_response(response_id, client_id)

    async def initialize(self):
        """Initialize all connections and setup message handlers"""
        logger.info("Initializing Response Handler Service...")

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

            # Setup database tables
            await self._setup_database()

            # Load pending responses from Redis (crash recovery)
            await self._recover_pending_responses()

            logger.info("Response Handler Service initialized successfully")

        except Exception as e:
            logger.error(f"Failed to initialize Response Handler Service: {e}")
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
                ("task_events", "topic"),
                ("response_events", "topic"),
                ("client_events", "topic"),
            ]

            for exchange, exchange_type in exchanges:
                self.channel.exchange_declare(
                    exchange=exchange, exchange_type=exchange_type, durable=True
                )

            # Declare response queues
            queues = [
                "response_handler_incoming",
                "response_delivery_callbacks",
                "response_delivery_failed",
                "client_notifications",
            ]

            for queue_name in queues:
                self.channel.queue_declare(queue=queue_name, durable=True)

            # Bind to task completion events from Worker Pool
            self.channel.queue_bind(
                exchange="task_events",
                queue="response_handler_incoming",
                routing_key="task.completed",
            )

            self.channel.queue_bind(
                exchange="task_events",
                queue="response_handler_incoming",
                routing_key="task.failed",
            )

            # Bind to response delivery events
            self.channel.queue_bind(
                exchange="response_events",
                queue="response_delivery_callbacks",
                routing_key="response.callback.*",
            )

            logger.info("RabbitMQ setup completed")

        except Exception as e:
            logger.error(f"RabbitMQ setup failed: {e}")
            raise

    async def _setup_database(self):
        """Setup database tables for response tracking"""
        try:
            async with self.db_pool.acquire() as conn:
                # Response delivery log
                await conn.execute(
                    """
                    CREATE TABLE IF NOT EXISTS response_deliveries (
                        id SERIAL PRIMARY KEY,
                        response_id VARCHAR(255) UNIQUE NOT NULL,
                        task_id VARCHAR(255) NOT NULL,
                        client_id VARCHAR(255) NOT NULL,
                        request_id VARCHAR(255) NOT NULL,
                        delivery_method VARCHAR(50) NOT NULL,
                        status VARCHAR(50) NOT NULL,
                        created_at TIMESTAMP NOT NULL DEFAULT NOW(),
                        delivered_at TIMESTAMP,
                        delivery_time_ms INTEGER,
                        error_message TEXT,
                        retry_count INTEGER DEFAULT 0,
                        callback_url TEXT,
                        response_size_bytes INTEGER DEFAULT 0
                    )
                """
                )

                # Client connections tracking
                await conn.execute(
                    """
                    CREATE TABLE IF NOT EXISTS client_connections (
                        connection_id VARCHAR(255) PRIMARY KEY,
                        client_id VARCHAR(255) NOT NULL,
                        connection_type VARCHAR(50) NOT NULL,
                        connected_at TIMESTAMP NOT NULL DEFAULT NOW(),
                        disconnected_at TIMESTAMP,
                        last_activity TIMESTAMP NOT NULL DEFAULT NOW(),
                        total_responses INTEGER DEFAULT 0,
                        is_active BOOLEAN DEFAULT true
                    )
                """
                )

                # Response content cache (for large responses)
                await conn.execute(
                    """
                    CREATE TABLE IF NOT EXISTS response_cache (
                        response_id VARCHAR(255) PRIMARY KEY,
                        content_data BYTEA NOT NULL,
                        content_type VARCHAR(100),
                        is_compressed BOOLEAN DEFAULT false,
                        created_at TIMESTAMP NOT NULL DEFAULT NOW(),
                        expires_at TIMESTAMP NOT NULL,
                        access_count INTEGER DEFAULT 0,
                        last_accessed TIMESTAMP DEFAULT NOW()
                    )
                """
                )

                # Create indexes
                await conn.execute(
                    """
                    CREATE INDEX IF NOT EXISTS idx_response_deliveries_client_id 
                    ON response_deliveries(client_id)
                """
                )
                await conn.execute(
                    """
                    CREATE INDEX IF NOT EXISTS idx_response_deliveries_status 
                    ON response_deliveries(status)
                """
                )
                await conn.execute(
                    """
                    CREATE INDEX IF NOT EXISTS idx_response_deliveries_created_at 
                    ON response_deliveries(created_at)
                """
                )
                await conn.execute(
                    """
                    CREATE INDEX IF NOT EXISTS idx_client_connections_client_id 
                    ON client_connections(client_id)
                """
                )
                await conn.execute(
                    """
                    CREATE INDEX IF NOT EXISTS idx_response_cache_expires_at 
                    ON response_cache(expires_at)
                """
                )

                logger.info("Database tables created/verified")

        except Exception as e:
            logger.error(f"Database setup failed: {e}")
            raise

    async def _recover_pending_responses(self):
        """Recover pending responses from Redis after restart"""
        try:
            pattern = "pending_response:*"
            cursor = "0"

            while cursor != 0:
                cursor, keys = await self.redis.scan(cursor, match=pattern, count=100)

                for key in keys:
                    response_data = await self.redis.get(key)
                    if response_data:
                        try:
                            data = json.loads(response_data)
                            response = PendingResponse(**data)

                            # Check if still valid
                            if not response.is_expired():
                                self.pending_responses[response.response_id] = response
                                logger.debug(
                                    f"Recovered pending response {response.response_id}"
                                )
                            else:
                                await self.redis.delete(key)

                        except Exception as e:
                            logger.error(f"Error recovering response from {key}: {e}")
                            await self.redis.delete(key)

            logger.info(f"Recovered {len(self.pending_responses)} pending responses")

        except Exception as e:
            logger.error(f"Error recovering pending responses: {e}")

    async def handle_websocket_connection(self, websocket: WebSocket, client_id: str):
        """Handle WebSocket connection for real-time response delivery"""
        connection_id = str(uuid.uuid4())

        try:
            await websocket.accept()
            logger.info(f"WebSocket connection established for client {client_id}")

            # Create client connection tracking
            connection = ClientConnection(
                client_id=client_id,
                connection_id=connection_id,
                connection_type="websocket",
                connected_at=time.time(),
                last_activity=time.time(),
                pending_responses=set(),
                websocket=websocket,
            )

            self.client_connections[connection_id] = connection
            self.websocket_connections[connection_id] = websocket

            # Register in database
            await self._register_client_connection(connection)

            # Send any pending responses immediately
            await self._deliver_pending_responses_to_client(client_id, connection_id)

            # Handle incoming messages (pings, acks, etc.)
            async for message in websocket.iter_text():
                try:
                    msg_data = json.loads(message)
                    await self._handle_websocket_message(connection_id, msg_data)
                    connection.update_activity()
                except json.JSONDecodeError:
                    logger.warning(f"Invalid JSON from WebSocket {connection_id}")
                except Exception as e:
                    logger.error(f"Error handling WebSocket message: {e}")

        except Exception as e:
            logger.error(f"WebSocket connection error for client {client_id}: {e}")
        finally:
            await self._cleanup_client_connection(connection_id)

    async def _handle_websocket_message(self, connection_id: str, message: Dict):
        """Handle incoming WebSocket messages"""
        try:
            msg_type = message.get("type")

            if msg_type == "ping":
                # Send pong response
                connection = self.client_connections.get(connection_id)
                if connection and connection.websocket:
                    await connection.websocket.send_text(
                        json.dumps({"type": "pong", "timestamp": time.time()})
                    )

            elif msg_type == "ack":
                # Client acknowledges response receipt
                response_id = message.get("response_id")
                if response_id:
                    await self._mark_response_delivered(response_id)

            elif msg_type == "subscribe":
                # Subscribe to specific response types or patterns
                await self._handle_subscription(connection_id, message)

            else:
                logger.warning(f"Unknown WebSocket message type: {msg_type}")

        except Exception as e:
            logger.error(f"Error handling WebSocket message: {e}")

    async def _register_client_connection(self, connection: ClientConnection):
        """Register client connection in database"""
        try:
            async with self.db_pool.acquire() as conn:
                await conn.execute(
                    """
                    INSERT INTO client_connections 
                    (connection_id, client_id, connection_type, connected_at, last_activity)
                    VALUES ($1, $2, $3, $4, $5)
                """,
                    connection.connection_id,
                    connection.client_id,
                    connection.connection_type,
                    datetime.fromtimestamp(connection.connected_at),
                    datetime.fromtimestamp(connection.last_activity),
                )

        except Exception as e:
            logger.error(f"Error registering client connection: {e}")

    async def _cleanup_client_connection(self, connection_id: str):
        """Clean up client connection resources"""
        try:
            connection = self.client_connections.get(connection_id)
            if not connection:
                return

            # Update database
            async with self.db_pool.acquire() as conn:
                await conn.execute(
                    """
                    UPDATE client_connections 
                    SET disconnected_at = NOW(), is_active = false
                    WHERE connection_id = $1
                """,
                    connection_id,
                )

            # Clean up memory
            self.client_connections.pop(connection_id, None)
            self.websocket_connections.pop(connection_id, None)

            # Handle any pending responses for this connection
            pending_to_requeue = []
            for response_id, response in list(self.pending_responses.items()):
                if response.client_id == connection.client_id:
                    if response.delivery_method == DeliveryMethod.WEBSOCKET:
                        # Convert to HTTP polling for alternative delivery
                        response.delivery_method = DeliveryMethod.HTTP_POLLING
                        pending_to_requeue.append(response)

            for response in pending_to_requeue:
                await self.response_queues[DeliveryMethod.HTTP_POLLING].put(response)

            logger.info(f"Cleaned up connection {connection_id}")

        except Exception as e:
            logger.error(f"Error cleaning up connection {connection_id}: {e}")

    def _start_rabbitmq_consumer(self):
        """Start RabbitMQ consumer in separate thread"""

        def consume():
            try:
                self.channel.basic_consume(
                    queue="response_handler_incoming",
                    on_message_callback=self._handle_response_message,
                    auto_ack=False,
                )

                self.channel.basic_qos(prefetch_count=50)

                logger.info("Starting RabbitMQ consumer for task responses...")
                self.channel.start_consuming()

            except Exception as e:
                logger.error(f"RabbitMQ consumer error: {e}")

        self.consumer_thread = threading.Thread(target=consume, daemon=True)
        self.consumer_thread.start()

    def _handle_response_message(self, channel, method, properties, body):
        """Handle incoming response from Worker Pool Service"""
        try:
            response_data = json.loads(body)

            # Extract relevant information
            task_id = response_data.get("task_id")
            event_type = method.routing_key  # task.completed or task.failed

            if not task_id:
                logger.error("Response message missing task_id")
                channel.basic_nack(delivery_tag=method.delivery_tag)
                return

            # Process the response asynchronously
            asyncio.run_coroutine_threadsafe(
                self._process_task_response(task_id, event_type, response_data),
                asyncio.get_event_loop(),
            )

            # Acknowledge message
            channel.basic_ack(delivery_tag=method.delivery_tag)

        except Exception as e:
            logger.error(f"Error handling response message: {e}")
            channel.basic_nack(delivery_tag=method.delivery_tag, requeue=True)

    async def _process_task_response(
        self, task_id: str, event_type: str, response_data: Dict
    ):
        """Process completed task response and prepare for delivery"""
        try:
            # Check if this is for a pending response
            matching_responses = [
                r for r in self.pending_responses.values() if r.task_id == task_id
            ]

            if not matching_responses:
                # Create new response from task completion
                response = await self._create_response_from_task(
                    task_id, event_type, response_data
                )
                if not response:
                    return
            else:
                response = matching_responses[0]
                response.response_data = response_data
                response.status = (
                    ResponseStatus.COMPLETED
                    if event_type == "task.completed"
                    else ResponseStatus.FAILED
                )

            # Store response data
            await self._store_response_data(response)

            # Queue for delivery based on method
            await self.response_queues[response.delivery_method].put(response)

            # Update metrics
            self.metrics.total_responses += 1

            logger.debug(f"Processed response for task {task_id}")

        except Exception as e:
            logger.error(f"Error processing task response {task_id}: {e}")

    async def _create_response_from_task(
        self, task_id: str, event_type: str, response_data: Dict
    ) -> Optional[PendingResponse]:
        """Create response object from task completion event"""
        try:
            # Get task info from Redis/database
            task_info = await self._get_task_info(task_id)
            if not task_info:
                logger.warning(f"No task info found for {task_id}")
                return None

            response_id = str(uuid.uuid4())

            # Determine delivery method (default to redis cache)
            delivery_method = DeliveryMethod.REDIS_CACHE

            # Check for active WebSocket connection
            client_connections = [
                c
                for c in self.client_connections.values()
                if c.client_id == task_info.get("client_id")
            ]

            if client_connections:
                delivery_method = DeliveryMethod.WEBSOCKET

            response = PendingResponse(
                response_id=response_id,
                task_id=task_id,
                client_id=task_info.get("client_id", "unknown"),
                request_id=task_info.get("request_id", "unknown"),
                delivery_method=delivery_method,
                created_at=time.time(),
                timeout=self.default_timeout,
                response_data=response_data,
                status=(
                    ResponseStatus.COMPLETED
                    if event_type == "task.completed"
                    else ResponseStatus.FAILED
                ),
            )

            self.pending_responses[response_id] = response
            return response

        except Exception as e:
            logger.error(f"Error creating response from task {task_id}: {e}")
            return None

    async def _get_task_info(self, task_id: str) -> Optional[Dict]:
        """Get task information from Redis or database"""
        try:
            # First try Redis cache
            task_data = await self.redis.get(f"task:{task_id}")
            if task_data:
                return json.loads(task_data)

            # Fall back to database
            async with self.db_pool.acquire() as conn:
                row = await conn.fetchrow(
                    """
                    SELECT task_id, client_id, request_data->>'request_id' as request_id
                    FROM tasks WHERE task_id = $1
                """,
                    task_id,
                )

                if row:
                    return dict(row)

            return None

        except Exception as e:
            logger.error(f"Error getting task info for {task_id}: {e}")
            return None

    async def _store_response_data(self, response: PendingResponse):
        """Store response data in appropriate storage"""
        try:
            response_content = response.response_data
            content_size = len(json.dumps(response_content).encode("utf-8"))

            # Store in Redis for fast access
            cache_key = f"response:{response.response_id}"

            # Compress large responses
            if content_size > self.compression_threshold:
                compressed_data = gzip.compress(
                    json.dumps(response_content).encode("utf-8")
                )
                encoded_data = base64.b64encode(compressed_data).decode("utf-8")

                await self.redis.hset(
                    cache_key,
                    mapping={
                        "data": encoded_data,
                        "compressed": "true",
                        "size": content_size,
                        "created_at": response.created_at,
                    },
                )
            else:
                await self.redis.hset(
                    cache_key,
                    mapping={
                        "data": json.dumps(response_content),
                        "compressed": "false",
                        "size": content_size,
                        "created_at": response.created_at,
                    },
                )

            # Set expiration
            await self.redis.expire(cache_key, self.default_timeout)

            # Store large responses in database as backup
            if content_size > self.max_response_size:
                await self._store_large_response_in_db(response, content_size)

            # Log delivery record
            await self._log_response_delivery(response, content_size)

        except Exception as e:
            logger.error(f"Error storing response data: {e}")

    async def _store_large_response_in_db(
        self, response: PendingResponse, content_size: int
    ):
        """Store large response content in database"""
        try:
            content_data = json.dumps(response.response_data).encode("utf-8")
            is_compressed = False

            # Compress if beneficial
            if content_size > self.compression_threshold:
                compressed = gzip.compress(content_data)
                if len(compressed) < len(content_data) * 0.9:  # At least 10% reduction
                    content_data = compressed
                    is_compressed = True

            async with self.db_pool.acquire() as conn:
                await conn.execute(
                    """
                    INSERT INTO response_cache 
                    (response_id, content_data, content_type, is_compressed, expires_at)
                    VALUES ($1, $2, $3, $4, $5)
                    ON CONFLICT (response_id) DO UPDATE SET
                        content_data = EXCLUDED.content_data,
                        is_compressed = EXCLUDED.is_compressed,
                        expires_at = EXCLUDED.expires_at
                """,
                    response.response_id,
                    content_data,
                    "application/json",
                    is_compressed,
                    datetime.fromtimestamp(time.time() + self.default_timeout),
                )

        except Exception as e:
            logger.error(f"Error storing large response in database: {e}")

    async def _log_response_delivery(
        self, response: PendingResponse, content_size: int
    ):
        """Log response delivery attempt"""
        try:
            async with self.db_pool.acquire() as conn:
                await conn.execute(
                    """
                    INSERT INTO response_deliveries 
                    (response_id, task_id, client_id, request_id, delivery_method, 
                     status, callback_url, response_size_bytes)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                """,
                    response.response_id,
                    response.task_id,
                    response.client_id,
                    response.request_id,
                    response.delivery_method.value,
                    response.status.value,
                    response.callback_url,
                    content_size,
                )

        except Exception as e:
            logger.error(f"Error logging response delivery: {e}")

    # Response delivery processors
    async def _websocket_delivery_processor(self):
        """Process WebSocket response deliveries"""
        while self.running:
            try:
                response = await self.response_queues[DeliveryMethod.WEBSOCKET].get()
                await self._deliver_via_websocket(response)
            except Exception as e:
                logger.error(f"WebSocket delivery processor error: {e}")
                await asyncio.sleep(1)

    async def _deliver_via_websocket(self, response: PendingResponse):
        """Deliver response via WebSocket"""
        try:
            # Find active WebSocket connections for client
            client_connections = [
                c
                for c in self.client_connections.values()
                if c.client_id == response.client_id
                and c.connection_type == "websocket"
            ]

            if not client_connections:
                # No WebSocket connection, fall back to caching
                response.delivery_method = DeliveryMethod.REDIS_CACHE
                await self.response_queues[DeliveryMethod.REDIS_CACHE].put(response)
                return

            start_time = time.time()
            success = False

            for connection in client_connections:
                try:
                    if connection.websocket:
                        message = {
                            "type": "response",
                            "response_id": response.response_id,
                            "task_id": response.task_id,
                            "request_id": response.request_id,
                            "status": response.status.value,
                            "data": response.response_data,
                            "timestamp": time.time(),
                        }

                        await connection.websocket.send_text(json.dumps(message))
                        connection.update_activity()
                        success = True

                        logger.debug(
                            f"Delivered response {response.response_id} via WebSocket"
                        )
                        break

                except Exception as e:
                    logger.error(
                        f"Error delivering to WebSocket {connection.connection_id}: {e}"
                    )
                    # Remove failed connection
                    await self._cleanup_client_connection(connection.connection_id)

            if success:
                response.status = ResponseStatus.DELIVERED
                response.delivered_at = time.time()

                # Update metrics
                delivery_time = time.time() - start_time
                self.metrics.successful_deliveries += 1
                self.metrics.update_delivery_time(delivery_time)
                self.delivery_times.append(delivery_time)

                # Update database
                await self._update_delivery_status(response, delivery_time)
            else:
                response.status = ResponseStatus.FAILED
                response.error_message = "No active WebSocket connections"
                self.metrics.failed_deliveries += 1

                # Try fallback delivery method
                response.delivery_method = DeliveryMethod.REDIS_CACHE
                await self.response_queues[DeliveryMethod.REDIS_CACHE].put(response)

        except Exception as e:
            logger.error(f"Error in WebSocket delivery: {e}")
            response.status = ResponseStatus.FAILED
            response.error_message = str(e)
            self.metrics.failed_deliveries += 1

    async def _http_polling_delivery_processor(self):
        """Process HTTP polling response deliveries"""
        while self.running:
            try:
                response = await self.response_queues[DeliveryMethod.HTTP_POLLING].get()
                await self._deliver_via_http_polling(response)
            except Exception as e:
                logger.error(f"HTTP polling delivery processor error: {e}")
                await asyncio.sleep(1)

    async def _deliver_via_http_polling(self, response: PendingResponse):
        """Store response for HTTP polling retrieval"""
        try:
            # Response is already stored in Redis/database by _store_response_data
            # Just need to mark it as ready for polling
            response.status = ResponseStatus.COMPLETED

            # Store in client-specific polling queue
            polling_key = f"client_responses:{response.client_id}"
            await self.redis.lpush(polling_key, response.response_id)
            await self.redis.expire(polling_key, self.default_timeout)

            # Notify client if they have an active polling request
            await self._notify_polling_client(response.client_id, response.response_id)

            logger.debug(f"Response {response.response_id} ready for polling")

        except Exception as e:
            logger.error(f"Error in HTTP polling delivery: {e}")

    async def _callback_delivery_processor(self):
        """Process callback response deliveries"""
        while self.running:
            try:
                response = await self.response_queues[DeliveryMethod.CALLBACK].get()
                await self._deliver_via_callback(response)
            except Exception as e:
                logger.error(f"Callback delivery processor error: {e}")
                await asyncio.sleep(1)

    async def _deliver_via_callback(self, response: PendingResponse):
        """Deliver response via HTTP callback"""
        try:
            if not response.callback_url:
                logger.error(f"No callback URL for response {response.response_id}")
                response.status = ResponseStatus.FAILED
                return

            start_time = time.time()

            # Prepare callback payload
            payload = {
                "response_id": response.response_id,
                "task_id": response.task_id,
                "request_id": response.request_id,
                "status": response.status.value,
                "data": response.response_data,
                "timestamp": time.time(),
            }

            # Make HTTP callback
            async with ClientSession() as session:
                try:
                    async with session.post(
                        response.callback_url,
                        json=payload,
                        timeout=30,
                        headers={"Content-Type": "application/json"},
                    ) as resp:
                        if resp.status == 200:
                            response.status = ResponseStatus.DELIVERED
                            response.delivered_at = time.time()

                            delivery_time = time.time() - start_time
                            self.metrics.successful_deliveries += 1
                            self.metrics.update_delivery_time(delivery_time)

                            await self._update_delivery_status(response, delivery_time)

                            logger.info(
                                f"Callback delivered for response {response.response_id}"
                            )
                        else:
                            raise Exception(f"Callback returned status {resp.status}")

                except asyncio.TimeoutError:
                    raise Exception("Callback timeout")

        except Exception as e:
            logger.error(f"Callback delivery failed for {response.response_id}: {e}")
            response.status = ResponseStatus.FAILED
            response.error_message = str(e)
            self.metrics.failed_deliveries += 1

            # Retry logic
            if response.can_retry():
                response.retry_count += 1
                await asyncio.sleep(self.config.get("retry_delay", 5))
                await self.response_queues[DeliveryMethod.CALLBACK].put(response)
            else:
                await self._handle_failed_delivery(response)

    async def _redis_cache_delivery_processor(self):
        """Process Redis cache deliveries (store and wait)"""
        while self.running:
            try:
                response = await self.response_queues[DeliveryMethod.REDIS_CACHE].get()
                await self._deliver_via_redis_cache(response)
            except Exception as e:
                logger.error(f"Redis cache delivery processor error: {e}")
                await asyncio.sleep(1)

    async def _deliver_via_redis_cache(self, response: PendingResponse):
        """Store response in Redis cache for later retrieval"""
        try:
            # Response data is already stored by _store_response_data
            response.status = ResponseStatus.COMPLETED

            # Add to client's response list for tracking
            client_responses_key = f"client_responses:{response.client_id}"
            await self.redis.lpush(client_responses_key, response.response_id)
            await self.redis.expire(client_responses_key, self.default_timeout)

            logger.debug(
                f"Response {response.response_id} cached for client {response.client_id}"
            )

        except Exception as e:
            logger.error(f"Error caching response: {e}")
            response.status = ResponseStatus.FAILED

    # HTTP API handlers
    async def get_response_http(self, response_id: str, client_id: str = None):
        """Get specific response via HTTP"""
        try:
            # Check if response exists
            if response_id not in self.pending_responses:
                return JSONResponse(
                    status_code=404, content={"error": "Response not found"}
                )

            response = self.pending_responses[response_id]

            # Verify client access
            if client_id and response.client_id != client_id:
                return JSONResponse(status_code=403, content={"error": "Access denied"})

            # Get response data
            response_data = await self._get_response_data(response_id)
            if not response_data:
                return JSONResponse(
                    status_code=404, content={"error": "Response data not found"}
                )

            # Mark as delivered
            await self._mark_response_delivered(response_id)

            return JSONResponse(
                content={
                    "response_id": response_id,
                    "task_id": response.task_id,
                    "request_id": response.request_id,
                    "status": response.status.value,
                    "data": response_data,
                    "delivered_at": time.time(),
                }
            )

        except Exception as e:
            logger.error(f"Error getting response {response_id}: {e}")
            return JSONResponse(
                status_code=500, content={"error": "Internal server error"}
            )

    async def poll_client_responses(self, client_id: str, timeout: int = 30):
        """Poll for responses for a specific client"""
        try:
            start_time = time.time()

            while time.time() - start_time < timeout:
                # Check for available responses
                response_key = f"client_responses:{client_id}"
                response_id = await self.redis.rpop(response_key)

                if response_id:
                    response_data = await self._get_response_data(response_id)
                    if response_data:
                        # Mark as delivered
                        await self._mark_response_delivered(response_id)

                        response = self.pending_responses.get(response_id)
                        return JSONResponse(
                            content={
                                "response_id": response_id,
                                "task_id": response.task_id if response else None,
                                "request_id": response.request_id if response else None,
                                "status": "completed",
                                "data": response_data,
                                "delivered_at": time.time(),
                            }
                        )

                # Wait before next check
                await asyncio.sleep(0.5)

            # Timeout reached
            return JSONResponse(
                status_code=204,  # No Content
                content={"message": "No responses available"},
            )

        except Exception as e:
            logger.error(f"Error polling responses for client {client_id}: {e}")
            return JSONResponse(
                status_code=500, content={"error": "Internal server error"}
            )

    async def stream_client_responses(self, client_id: str):
        """Stream responses for a client via Server-Sent Events"""

        async def response_generator():
            try:
                connection_id = str(uuid.uuid4())

                # Register streaming connection
                connection = ClientConnection(
                    client_id=client_id,
                    connection_id=connection_id,
                    connection_type="http_streaming",
                    connected_at=time.time(),
                    last_activity=time.time(),
                    pending_responses=set(),
                )

                self.client_connections[connection_id] = connection
                await self._register_client_connection(connection)

                # Send initial connection event
                yield f"data: {json.dumps({'type': 'connected', 'timestamp': time.time()})}\n\n"

                try:
                    while True:
                        # Check for responses
                        response_key = f"client_responses:{client_id}"
                        response_id = await self.redis.rpop(response_key)

                        if response_id:
                            response_data = await self._get_response_data(response_id)
                            if response_data:
                                response = self.pending_responses.get(response_id)
                                event_data = {
                                    "type": "response",
                                    "response_id": response_id,
                                    "task_id": response.task_id if response else None,
                                    "request_id": (
                                        response.request_id if response else None
                                    ),
                                    "status": "completed",
                                    "data": response_data,
                                    "timestamp": time.time(),
                                }

                                yield f"data: {json.dumps(event_data)}\n\n"
                                await self._mark_response_delivered(response_id)
                                connection.update_activity()

                        # Send periodic heartbeat
                        if time.time() - connection.last_activity > 30:
                            yield f"data: {json.dumps({'type': 'heartbeat', 'timestamp': time.time()})}\n\n"
                            connection.update_activity()

                        await asyncio.sleep(1)

                except asyncio.CancelledError:
                    logger.info(f"Stream cancelled for client {client_id}")
                finally:
                    await self._cleanup_client_connection(connection_id)

            except Exception as e:
                logger.error(f"Error in response stream for client {client_id}: {e}")
                yield f"data: {json.dumps({'type': 'error', 'message': str(e)})}\n\n"

        return response_generator()

    async def register_response_listener(self, request: Dict):
        """Register a response listener with specific delivery method"""
        try:
            response_id = request.get("response_id")
            task_id = request.get("task_id")
            client_id = request.get("client_id")
            delivery_method = request.get("delivery_method", "redis_cache")
            callback_url = request.get("callback_url")
            timeout = request.get("timeout", self.default_timeout)

            if not all([client_id, (response_id or task_id)]):
                return JSONResponse(
                    status_code=400, content={"error": "Missing required parameters"}
                )

            # Create pending response entry
            if not response_id:
                response_id = str(uuid.uuid4())

            response = PendingResponse(
                response_id=response_id,
                task_id=task_id or response_id,
                client_id=client_id,
                request_id=request.get("request_id", response_id),
                delivery_method=DeliveryMethod(delivery_method),
                created_at=time.time(),
                timeout=timeout,
                callback_url=callback_url,
            )

            self.pending_responses[response_id] = response

            # Store in Redis for persistence
            await self.redis.setex(
                f"pending_response:{response_id}",
                timeout,
                json.dumps(asdict(response), default=str),
            )

            return JSONResponse(
                content={
                    "response_id": response_id,
                    "status": "registered",
                    "delivery_method": delivery_method,
                    "timeout": timeout,
                }
            )

        except Exception as e:
            logger.error(f"Error registering response listener: {e}")
            return JSONResponse(
                status_code=500, content={"error": "Internal server error"}
            )

    async def cancel_pending_response(self, response_id: str, client_id: str):
        """Cancel a pending response"""
        try:
            response = self.pending_responses.get(response_id)
            if not response:
                return JSONResponse(
                    status_code=404, content={"error": "Response not found"}
                )

            if response.client_id != client_id:
                return JSONResponse(status_code=403, content={"error": "Access denied"})

            # Remove from tracking
            self.pending_responses.pop(response_id, None)
            await self.redis.delete(f"pending_response:{response_id}")
            await self.redis.delete(f"response:{response_id}")

            return JSONResponse(
                content={"response_id": response_id, "status": "cancelled"}
            )

        except Exception as e:
            logger.error(f"Error cancelling response {response_id}: {e}")
            return JSONResponse(
                status_code=500, content={"error": "Internal server error"}
            )

    # Helper methods
    async def _get_response_data(self, response_id: str) -> Optional[Dict]:
        """Get response data from cache or database"""
        try:
            # Try Redis cache first
            cache_key = f"response:{response_id}"
            cached_data = await self.redis.hgetall(cache_key)

            if cached_data:
                data = cached_data.get("data", "")
                is_compressed = cached_data.get("compressed", "false") == "true"

                if is_compressed:
                    # Decompress data
                    compressed_data = base64.b64decode(data)
                    decompressed = gzip.decompress(compressed_data)
                    return json.loads(decompressed.decode("utf-8"))
                else:
                    return json.loads(data)

            # Try database backup
            async with self.db_pool.acquire() as conn:
                row = await conn.fetchrow(
                    """
                    SELECT content_data, is_compressed 
                    FROM response_cache 
                    WHERE response_id = $1 AND expires_at > NOW()
                """,
                    response_id,
                )

                if row:
                    content_data = row["content_data"]
                    is_compressed = row["is_compressed"]

                    if is_compressed:
                        decompressed = gzip.decompress(content_data)
                        return json.loads(decompressed.decode("utf-8"))
                    else:
                        return json.loads(content_data.decode("utf-8"))

            return None

        except Exception as e:
            logger.error(f"Error getting response data for {response_id}: {e}")
            return None

    async def _mark_response_delivered(self, response_id: str):
        """Mark response as delivered"""
        try:
            response = self.pending_responses.get(response_id)
            if response:
                response.status = ResponseStatus.DELIVERED
                response.delivered_at = time.time()

            # Update database
            async with self.db_pool.acquire() as conn:
                await conn.execute(
                    """
                    UPDATE response_deliveries 
                    SET status = 'delivered', delivered_at = NOW(),
                        delivery_time_ms = EXTRACT(EPOCH FROM (NOW() - created_at)) * 1000
                    WHERE response_id = $1
                """,
                    response_id,
                )

            # Clean up from pending
            self.pending_responses.pop(response_id, None)
            await self.redis.delete(f"pending_response:{response_id}")

        except Exception as e:
            logger.error(f"Error marking response delivered: {e}")

    async def _update_delivery_status(
        self, response: PendingResponse, delivery_time: float
    ):
        """Update delivery status in database"""
        try:
            async with self.db_pool.acquire() as conn:
                await conn.execute(
                    """
                    UPDATE response_deliveries 
                    SET status = $1, delivered_at = $2, delivery_time_ms = $3,
                        error_message = $4, retry_count = $5
                    WHERE response_id = $6
                """,
                    response.status.value,
                    (
                        datetime.fromtimestamp(response.delivered_at)
                        if response.delivered_at
                        else None
                    ),
                    int(delivery_time * 1000),
                    response.error_message,
                    response.retry_count,
                    response.response_id,
                )

        except Exception as e:
            logger.error(f"Error updating delivery status: {e}")

    async def _notify_polling_client(self, client_id: str, response_id: str):
        """Notify polling clients about new response"""
        try:
            # This could be enhanced to notify specific polling connections
            # For now, responses are available in the polling queue
            pass

        except Exception as e:
            logger.error(f"Error notifying polling client: {e}")

    async def _handle_failed_delivery(self, response: PendingResponse):
        """Handle failed delivery"""
        try:
            # Log the failure
            await self._update_delivery_status(response, 0)

            # Publish failure event
            failure_event = {
                "response_id": response.response_id,
                "task_id": response.task_id,
                "client_id": response.client_id,
                "delivery_method": response.delivery_method.value,
                "error_message": response.error_message,
                "retry_count": response.retry_count,
                "timestamp": time.time(),
            }

            message = json.dumps(failure_event)
            self.channel.basic_publish(
                exchange="response_events",
                routing_key="response.delivery.failed",
                body=message,
                properties=pika.BasicProperties(delivery_mode=2),
            )

        except Exception as e:
            logger.error(f"Error handling failed delivery: {e}")

    async def _deliver_pending_responses_to_client(
        self, client_id: str, connection_id: str
    ):
        """Deliver any pending responses to newly connected client"""
        try:
            # Check for pending responses
            client_responses_key = f"client_responses:{client_id}"
            response_ids = await self.redis.lrange(client_responses_key, 0, -1)

            connection = self.client_connections.get(connection_id)
            if not connection or not connection.websocket:
                return

            for response_id in response_ids:
                response_data = await self._get_response_data(response_id)
                if response_data:
                    response = self.pending_responses.get(response_id)
                    if response:
                        message = {
                            "type": "response",
                            "response_id": response_id,
                            "task_id": response.task_id,
                            "request_id": response.request_id,
                            "status": response.status.value,
                            "data": response_data,
                            "timestamp": time.time(),
                        }

                        try:
                            await connection.websocket.send_text(json.dumps(message))
                            await self._mark_response_delivered(response_id)

                            # Remove from client queue
                            await self.redis.lrem(client_responses_key, 1, response_id)

                        except Exception as e:
                            logger.error(f"Error delivering pending response: {e}")
                            break

        except Exception as e:
            logger.error(f"Error delivering pending responses: {e}")

    # Background maintenance tasks
    async def _cleanup_expired_responses(self):
        """Clean up expired responses and connections"""
        while self.running:
            try:
                current_time = time.time()

                # Clean up expired pending responses
                expired_responses = []
                for response_id, response in list(self.pending_responses.items()):
                    if response.is_expired():
                        expired_responses.append(response_id)

                for response_id in expired_responses:
                    response = self.pending_responses.pop(response_id, None)
                    if response:
                        response.status = ResponseStatus.TIMEOUT
                        self.metrics.timeout_deliveries += 1
                        await self._handle_failed_delivery(response)

                # Clean up inactive client connections
                inactive_connections = []
                for connection_id, connection in list(self.client_connections.items()):
                    if not connection.is_active():
                        inactive_connections.append(connection_id)

                for connection_id in inactive_connections:
                    await self._cleanup_client_connection(connection_id)

                # Clean up expired cached responses in database
                async with self.db_pool.acquire() as conn:
                    await conn.execute(
                        """
                        DELETE FROM response_cache 
                        WHERE expires_at < NOW()
                    """
                    )

                # Update metrics
                self.metrics.last_updated = current_time
                if self.delivery_times:
                    self.metrics.responses_per_minute = len(self.delivery_times) / max(
                        (current_time - self.delivery_times[0]) / 60, 1
                    )

                await asyncio.sleep(self.cleanup_interval)

            except Exception as e:
                logger.error(f"Cleanup task error: {e}")
                await asyncio.sleep(10)

    async def _handle_subscription(self, connection_id: str, message: Dict):
        """Handle client subscription to specific response patterns"""
        try:
            # This could be enhanced to support pattern-based subscriptions
            # For now, basic implementation
            pattern = message.get("pattern", "*")

            connection = self.client_connections.get(connection_id)
            if connection:
                # Store subscription pattern (simplified)
                subscription_key = f"subscriptions:{connection_id}"
                await self.redis.set(subscription_key, pattern, ex=3600)

        except Exception as e:
            logger.error(f"Error handling subscription: {e}")

    async def get_service_stats(self) -> Dict:
        """Get comprehensive service statistics"""
        try:
            stats = {
                "pending_responses": len(self.pending_responses),
                "active_connections": len(self.client_connections),
                "websocket_connections": len(self.websocket_connections),
                "total_responses": self.metrics.total_responses,
                "successful_deliveries": self.metrics.successful_deliveries,
                "failed_deliveries": self.metrics.failed_deliveries,
                "timeout_deliveries": self.metrics.timeout_deliveries,
                "avg_delivery_time": self.metrics.avg_delivery_time,
                "responses_per_minute": self.metrics.responses_per_minute,
                "success_rate": (
                    (
                        self.metrics.successful_deliveries
                        / max(self.metrics.total_responses, 1)
                    )
                    * 100
                ),
                "queue_sizes": {
                    method.value: queue.qsize()
                    for method, queue in self.response_queues.items()
                },
                "timestamp": time.time(),
            }

            # Add database stats
            async with self.db_pool.acquire() as conn:
                db_stats = await conn.fetchrow(
                    """
                    SELECT 
                        COUNT(*) as total_deliveries,
                        COUNT(*) FILTER (WHERE status = 'delivered') as delivered_count,
                        COUNT(*) FILTER (WHERE status = 'failed') as failed_count,
                        AVG(delivery_time_ms) as avg_delivery_time_ms
                    FROM response_deliveries 
                    WHERE created_at > NOW() - INTERVAL '1 hour'
                """
                )

                if db_stats:
                    stats.update(
                        {
                            "hourly_deliveries": db_stats["total_deliveries"] or 0,
                            "hourly_delivered": db_stats["delivered_count"] or 0,
                            "hourly_failed": db_stats["failed_count"] or 0,
                            "avg_delivery_time_ms": float(
                                db_stats["avg_delivery_time_ms"] or 0
                            ),
                        }
                    )

            return stats

        except Exception as e:
            logger.error(f"Error getting service stats: {e}")
            return {"error": str(e)}

    async def start(self):
        """Start the Response Handler Service"""
        self.running = True
        logger.info("Starting Response Handler Service...")

        # Start RabbitMQ consumer
        self._start_rabbitmq_consumer()

        # Start response delivery processors
        self.response_processors = [
            asyncio.create_task(self._websocket_delivery_processor()),
            asyncio.create_task(self._http_polling_delivery_processor()),
            asyncio.create_task(self._callback_delivery_processor()),
            asyncio.create_task(self._redis_cache_delivery_processor()),
            asyncio.create_task(self._cleanup_expired_responses()),
        ]

        # Start FastAPI server
        config = uvicorn.Config(
            app=self.app,
            host=self.config.get("host", "0.0.0.0"),
            port=self.config.get("port", 8082),
            log_level="info",
        )

        server = uvicorn.Server(config)

        try:
            # Run server and processors concurrently
            await asyncio.gather(server.serve(), *self.response_processors)
        except KeyboardInterrupt:
            logger.info("Shutting down Response Handler Service...")
        finally:
            await self.shutdown()

    async def shutdown(self):
        """Gracefully shutdown the service"""
        self.running = False

        # Cancel response processors
        for processor in self.response_processors:
            processor.cancel()

        # Close all client connections
        for connection_id in list(self.client_connections.keys()):
            await self._cleanup_client_connection(connection_id)

        # Stop RabbitMQ consumer
        if self.channel and not self.channel.is_closed:
            self.channel.stop_consuming()

        if self.consumer_thread and self.consumer_thread.is_alive():
            self.consumer_thread.join(timeout=5)

        # Close connections
        if self.rabbitmq_connection and not self.rabbitmq_connection.is_closed:
            self.rabbitmq_connection.close()

        if self.redis:
            await self.redis.close()

        if self.db_pool:
            await self.db_pool.close()

        logger.info("Response Handler Service shutdown completed")

import os

# Configuration compatible with existing services
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
    "port": 8082,
    "default_response_timeout": 300,
    "cleanup_interval": 60,
    "max_response_size": 10 * 1024 * 1024,
    "compression_threshold": 1024,
    "retry_delay": 5,
}


async def main():
    """Main entry point"""
    service = ResponseHandlerService(CONFIG)

    try:
        await service.initialize()
        await service.start()
    except Exception as e:
        logger.error(f"Service failed: {e}")
        await service.shutdown()


if __name__ == "__main__":
    asyncio.run(main())
