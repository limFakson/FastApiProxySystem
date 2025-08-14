#!/usr/bin/env python3
"""
Client-Facing API Gateway Service
=================================

This service acts as the front door for all client requests, handling:
- HTTP/HTTPS proxy requests
- SOCKS proxy requests
- Authentication and authorization
- Request validation and sanitization
- Queuing requests to RabbitMQ for processing
- Rate limiting and quota management
- Request tracking and correlation

Compatible with existing Node Manager, Node Communication, and Worker Pool services.
"""

import asyncio
import json
import time
import uuid
import base64
import logging
import traceback
from typing import Dict, Any, Optional, Tuple
from datetime import datetime, timedelta
from dataclasses import dataclass, asdict
from urllib.parse import urlparse

import aiohttp
import asyncpg
from aiohttp import web, ClientSession
from aiohttp.web_request import Request
from aiohttp.web_response import Response
import pika
from redis import asyncio as aioredis
from pydantic import BaseModel, ValidationError

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


# Configuration
@dataclass
class Config:
    # Server settings
    host: str = "0.0.0.0"
    port: int = 8080
    max_request_size: int = 50 * 1024 * 1024  # 50MB

    # Database settings
    postgres: dict = None
    redis: dict = None
    rabbitmq: dict = None

    # Queue names (matching existing services)
    task_exchange: str = "task_queue"
    task_events_exchange: str = "task_events"

    # Rate limiting
    rate_limit_window: int = 3600  # 1 hour
    default_rate_limit: int = 1000  # requests per hour

    # Request timeout
    request_timeout: int = 300  # 5 minutes


# Request models compatible with existing services
class ProxyTaskRequest(BaseModel):
    task_id: str
    task_type: str  # "http_request", "https_tunnel", "socks5_proxy"
    priority: int = 2  # 1=low, 2=normal, 3=high, 4=critical
    client_id: str
    request_data: Dict[str, Any]
    timeout: int = 30
    max_retries: int = 3


class User(BaseModel):
    id: str
    username: str
    api_key: str
    is_active: bool
    rate_limit: int
    created_at: datetime
    last_seen: Optional[datetime] = None


class APIGateway:
    def __init__(self, config: Dict):
        self.config = config

        # Extract nested config
        self.postgres_config = config.get("postgres", {})
        self.redis_config = config.get("redis", {})
        self.rabbitmq_config = config.get("rabbitmq", {})

        self.db_pool: Optional[asyncpg.Pool] = None
        self.redis: Optional[aioredis.Redis] = None
        self.rabbitmq_connection: Optional[pika.BlockingConnection] = None
        self.channel: Optional[pika.channel.Channel] = None

        self.app = web.Application(
            client_max_size=config.get("max_request_size", 50 * 1024 * 1024)
        )
        self.active_connections: Dict[str, asyncio.Event] = {}

        # Configuration from existing services
        self.rate_limit_window = config.get("rate_limit_window", 3600)
        self.default_rate_limit = config.get("default_rate_limit", 1000)
        self.request_timeout = config.get("request_timeout", 300)

        # Setup routes
        self._setup_routes()

    def _setup_routes(self):
        """Setup HTTP routes"""
        self.app.router.add_route("*", "/{path:.*}", self.handle_http_request)

    async def init_db(self):
        """Initialize PostgreSQL connection pool"""
        try:
            self.db_pool = await asyncpg.create_pool(
                host=self.postgres_config["host"],
                port=self.postgres_config["port"],
                database=self.postgres_config["database"],
                user=self.postgres_config["user"],
                password=self.postgres_config["password"],
                min_size=5,
                max_size=20,
                command_timeout=30,
            )

            # Ensure users table exists (compatible with existing schema)
            await self._setup_database_schema()
            logger.info("PostgreSQL connection pool initialized")
        except Exception as e:
            logger.error(f"Failed to initialize database: {e}")
            raise

    async def _setup_database_schema(self):
        """Setup database schema for API Gateway"""
        try:
            async with self.db_pool.acquire() as conn:
                # Users table for authentication
                await conn.execute(
                    """
                    CREATE TABLE IF NOT EXISTS users (
                        id VARCHAR(255) PRIMARY KEY,
                        username VARCHAR(255) UNIQUE NOT NULL,
                        api_key VARCHAR(512) UNIQUE NOT NULL,
                        is_active BOOLEAN DEFAULT TRUE,
                        rate_limit INTEGER DEFAULT 1000,
                        created_at TIMESTAMP DEFAULT NOW(),
                        last_seen TIMESTAMP
                    )
                """
                )

                # Request logs table
                await conn.execute(
                    """
                    CREATE TABLE IF NOT EXISTS request_logs (
                        id SERIAL PRIMARY KEY,
                        request_id VARCHAR(255) UNIQUE NOT NULL,
                        user_id VARCHAR(255) NOT NULL,
                        method VARCHAR(10) NOT NULL,
                        url TEXT NOT NULL,
                        status_code INTEGER,
                        response_time FLOAT,
                        bytes_transferred BIGINT DEFAULT 0,
                        created_at TIMESTAMP DEFAULT NOW(),
                        completed_at TIMESTAMP,
                        FOREIGN KEY (user_id) REFERENCES users(id)
                    )
                """
                )

                # Create indexes
                await conn.execute(
                    "CREATE INDEX IF NOT EXISTS idx_users_api_key ON users(api_key)"
                )
                await conn.execute(
                    "CREATE INDEX IF NOT EXISTS idx_users_active ON users(is_active)"
                )
                await conn.execute(
                    "CREATE INDEX IF NOT EXISTS idx_request_logs_user_id ON request_logs(user_id)"
                )
                await conn.execute(
                    "CREATE INDEX IF NOT EXISTS idx_request_logs_created_at ON request_logs(created_at)"
                )

        except Exception as e:
            logger.error(f"Database schema setup failed: {e}")
            raise

    async def init_redis(self):
        """Initialize Redis connection"""
        try:
            self.redis = await aioredis.from_url(
                f'redis://{self.redis_config["host"]}:{self.redis_config["port"]}'
            )
            logger.info("Redis connection initialized")
        except Exception as e:
            logger.error(f"Failed to initialize Redis: {e}")
            raise

    async def init_rabbitmq(self):
        """Initialize RabbitMQ connection compatible with existing services"""
        try:
            if (
                self.rabbitmq_connection
                and self.channel
                and self.channel.is_open
                and not self.rabbitmq_connection.is_closed()
            ):
                return

            credentials = pika.PlainCredentials(
                self.rabbitmq_config["user"], self.rabbitmq_config["password"]
            )
            parameters = pika.ConnectionParameters(
                host=self.rabbitmq_config["host"],
                port=self.rabbitmq_config["port"],
                virtual_host=self.rabbitmq_config.get("vhost", "/"),
                credentials=credentials,
                heartbeat=600,
                blocked_connection_timeout=300,
            )

            self.rabbitmq_connection = pika.BlockingConnection(parameters)
            self.channel = self.rabbitmq_connection.channel()

            # Declare exchanges (matching existing services)
            exchanges = [
                ("task_queue", "direct"),
                ("task_events", "topic"),
                ("node_commands", "direct"),
                ("worker_events", "topic"),
            ]

            for exchange, exchange_type in exchanges:
                self.channel.exchange_declare(
                    exchange=exchange, exchange_type=exchange_type, durable=True
                )

            # Declare task queues with priorities (matching Worker Pool Service)
            task_queues = [
                ("tasks.critical", 255),
                ("tasks.high", 10),
                ("tasks.normal", 5),
                ("tasks.low", 1),
            ]

            for queue_name, priority in task_queues:
                self.channel.queue_declare(
                    queue=queue_name,
                    durable=True,
                    arguments={
                        "x-max-priority": priority,
                        "x-message-ttl": 300000,  # 5 minutes TTL
                    },
                )

                # Bind to task exchange
                self.channel.queue_bind(
                    exchange="task_queue",
                    queue=queue_name,
                    routing_key=queue_name.replace("tasks.", ""),
                )

            # Response queue for gateway
            self.channel.queue_declare(queue="api_gateway_responses", durable=True)

            logger.info("RabbitMQ connection and queues initialized")
        except Exception as e:
            logger.error(f"Failed to initialize RabbitMQ: {e}")
            raise

    def _is_connection_valid(self):
        return (
            self.connection
            and not self.connection.is_closed
            and self.channel
            and not self.channel.is_closed
        )

    async def authenticate_user(self, request: Request) -> Optional[User]:
        """Authenticate user from request headers"""
        try:
            # Try different authentication methods
            auth_header = request.headers.get("Authorization")
            proxy_auth = request.headers.get("Proxy-Authorization")
            api_key = request.headers.get("X-API-Key")

            token = None

            if auth_header and auth_header.startswith("Bearer "):
                token = auth_header[7:]
            elif auth_header and auth_header.startswith("Basic "):
                try:
                    decoded = base64.b64decode(auth_header[6:]).decode("utf-8")
                    token = decoded.split(":", 1)[0]
                except:
                    pass
            elif proxy_auth and proxy_auth.startswith("Basic "):
                try:
                    decoded = base64.b64decode(proxy_auth[6:]).decode("utf-8")
                    token = decoded.split(":", 1)[0]
                except:
                    pass
            elif api_key:
                token = api_key

            if not token:
                return None

            # Query user from database
            async with self.db_pool.acquire() as conn:
                row = await conn.fetchrow(
                    "SELECT id, username, api_key, is_active, rate_limit, created_at, last_seen "
                    "FROM users WHERE api_key = $1 AND is_active = TRUE",
                    token,
                )

                if row:
                    user = User(
                        id=row["id"],
                        username=row["username"],
                        api_key=row["api_key"],
                        is_active=row["is_active"],
                        rate_limit=row["rate_limit"],
                        created_at=row["created_at"],
                        last_seen=row["last_seen"],
                    )

                    # Update last seen
                    await conn.execute(
                        "UPDATE users SET last_seen = $1 WHERE id = $2",
                        datetime.utcnow(),
                        user.id,
                    )

                    return user

        except Exception as e:
            logger.error(f"Authentication error: {e}")

        return None

    async def check_rate_limit(self, user: User) -> Tuple[bool, int]:
        """Check if user is within rate limits"""
        try:
            now = int(time.time())
            window_start = now - self.rate_limit_window

            # Get current request count from Redis
            key = f"rate_limit:{user.id}:{window_start // self.rate_limit_window}"
            current_count = await self.redis.get(key) or 0
            current_count = int(current_count)

            if current_count >= user.rate_limit:
                return False, user.rate_limit - current_count

            # Increment counter
            await self.redis.incr(key)
            await self.redis.expire(key, self.rate_limit_window)

            return True, user.rate_limit - current_count - 1

        except Exception as e:
            logger.error(f"Rate limit check error: {e}")
            return True, user.rate_limit  # Allow on error

    async def log_request(self, user: User, request: Request, request_id: str):
        """Log request to database"""
        try:
            async with self.db_pool.acquire() as conn:
                await conn.execute(
                    """
                    INSERT INTO request_logs (request_id, user_id, method, url, created_at)
                    VALUES ($1, $2, $3, $4, $5)
                    """,
                    request_id,
                    user.id,
                    request.method,
                    str(request.url),
                    datetime.utcnow(),
                )
        except Exception as e:
            logger.error(f"Request logging error: {e}")

    async def handle_http_request(self, request: Request) -> Response:
        """Handle incoming HTTP/HTTPS proxy requests"""
        request_id = str(uuid.uuid4())
        start_time = time.time()

        try:
            # Authenticate user
            user = await self.authenticate_user(request)
            if not user:
                return web.Response(
                    status=407,
                    headers={
                        "Proxy-Authenticate": 'Basic realm="Proxy Authentication Required"'
                    },
                    text="Proxy Authentication Required",
                )

            # Check rate limits
            allowed, remaining = await self.check_rate_limit(user)
            if not allowed:
                return web.Response(
                    status=429,
                    headers={
                        "X-RateLimit-Remaining": str(remaining),
                        "Retry-After": str(self.rate_limit_window),
                    },
                    text="Rate limit exceeded",
                )

            # Log request
            await self.log_request(user, request, request_id)

            # Handle CONNECT method (HTTPS tunneling)
            if request.method == "CONNECT":
                return await self.handle_connect_request(request, user, request_id)

            # Handle regular HTTP requests
            return await self.handle_proxy_request(request, user, request_id)

        except Exception as e:
            logger.error(f"Request handling error: {e}")
            logger.error(traceback.format_exc())
            return web.Response(status=500, text="Internal Server Error")

    async def handle_connect_request(
        self, request: Request, user: User, request_id: str
    ) -> Response:
        """Handle HTTPS CONNECT requests"""
        try:
            # Parse target host and port
            target_parts = request.path_qs.split(":")
            if len(target_parts) != 2:
                return web.Response(status=400, text="Invalid CONNECT target")

            target_host = target_parts[0]
            try:
                target_port = int(target_parts[1])
            except ValueError:
                return web.Response(status=400, text="Invalid port number")

            # Create task for Worker Pool Service
            task_data = ProxyTaskRequest(
                task_id=request_id,
                task_type="https_tunnel",
                priority=2,  # Normal priority
                client_id=user.id,
                request_data={
                    "target_host": target_host,
                    "target_port": target_port,
                    "client_socket_id": request_id,  # Use request_id as socket identifier
                },
                timeout=self.request_timeout,
            )

            # Queue task to appropriate priority queue
            success = await self.queue_task(task_data)
            if not success:
                return web.Response(status=500, text="Failed to queue request")

            # For CONNECT requests, return 200 immediately
            # The actual tunnel will be handled by Worker Pool and Node Communication services
            return web.Response(
                status=200,
                text="Connection established",
                headers={"X-Request-ID": request_id, "Connection": "keep-alive"},
            )

        except Exception as e:
            logger.error(f"CONNECT request error: {e}")
            return web.Response(status=500, text="Internal Server Error")

    async def handle_proxy_request(
        self, request: Request, user: User, request_id: str
    ) -> Response:
        """Handle regular HTTP proxy requests"""
        try:
            # Read request body
            body = None
            if request.can_read_body:
                body_bytes = await request.read()
                if body_bytes:
                    body = base64.b64encode(body_bytes).decode("utf-8")

            # Prepare headers (exclude hop-by-hop headers)
            headers = dict(request.headers)
            hop_by_hop = [
                "connection",
                "keep-alive",
                "proxy-authenticate",
                "proxy-authorization",
                "te",
                "trailers",
                "upgrade",
            ]
            for header in hop_by_hop:
                headers.pop(header, None)

            # Create task for Worker Pool Service
            task_data = ProxyTaskRequest(
                task_id=request_id,
                task_type="http_request",
                priority=2,  # Normal priority
                client_id=user.id,
                request_data={
                    "method": request.method,
                    "url": str(request.url),
                    "headers": headers,
                    "body": body,
                },
                timeout=min(
                    self.request_timeout, 60
                ),  # HTTP requests get shorter timeout
            )

            # Queue task
            success = await self.queue_task(task_data)
            start_time = time.time()
            if not success:
                return web.Response(status=500, text="Failed to queue request")

            # Wait for response from Worker Pool Service
            response_data = await self.wait_for_response(request_id, task_data.timeout)

            if response_data:
                # Parse response from Worker Pool Service format
                status_code = response_data.get("status_code", 200)
                response_headers = response_data.get("headers", {})
                response_body = response_data.get("body", "")

                # Decode body if it's base64 encoded
                if response_body:
                    try:
                        body_bytes = base64.b64decode(response_body)
                    except:
                        body_bytes = response_body.encode("utf-8")
                else:
                    body_bytes = b""

                # Update request log
                await self.update_request_log(
                    request_id, status_code, time.time() - start_time, len(body_bytes)
                )

                return web.Response(
                    status=status_code, headers=response_headers, body=body_bytes
                )
            else:
                return web.Response(status=504, text="Gateway Timeout")

        except Exception as e:
            logger.error(f"Proxy request error: {e}")
            return web.Response(status=500, text="Internal Server Error")

    async def queue_task(self, task_data: ProxyTaskRequest) -> bool:
        """Queue task to RabbitMQ (compatible with Worker Pool Service)"""
        try:
            # Determine queue based on priority
            queue_mapping = {
                1: "tasks.low",
                2: "tasks.normal",
                3: "tasks.high",
                4: "tasks.critical",
            }

            await self.init_rabbitmq()

            queue_name = queue_mapping.get(task_data.priority, "tasks.normal")
            routing_key = queue_name.replace("tasks.", "")

            # Convert to dict format expected by Worker Pool Service
            message_data = {
                "task_id": task_data.task_id,
                "task_type": task_data.task_type,
                "priority": task_data.priority,
                "client_id": task_data.client_id,
                "request_data": task_data.request_data,
                "timeout": task_data.timeout,
                "max_retries": task_data.max_retries,
                "created_at": time.time(),
            }

            message = json.dumps(message_data)

            self.channel.basic_publish(
                exchange="task_queue",
                routing_key=routing_key,
                body=message,
                properties=pika.BasicProperties(
                    priority=task_data.priority,
                    delivery_mode=2,  # Make message persistent
                    headers={
                        "task_id": task_data.task_id,
                        "client_id": task_data.client_id,
                        "timestamp": str(time.time()),
                    },
                ),
            )

            logger.info(f"Queued task {task_data.task_id} to {queue_name}")

            return True

        except Exception as e:
            logger.error(f"Failed to queue task: {e}")
            return False

    async def wait_for_response(
        self, task_id: str, timeout: int
    ) -> Optional[Dict[str, Any]]:
        """Wait for task response (compatible with Worker Pool Service response format)"""
        try:
            # Worker Pool Service stores responses in Redis with this key format
            response_key = f"task_response:{task_id}"

            for _ in range(timeout * 10):  # Check every 100ms
                response_data = await self.redis.get(response_key)
                if response_data:
                    await self.redis.delete(response_key)  # Cleanup
                    return json.loads(response_data)
                await asyncio.sleep(0.1)

            return None

        except Exception as e:
            logger.error(f"Error waiting for response for task {task_id}: {e}")
            return None

    async def update_request_log(
        self,
        request_id: str,
        status_code: int,
        response_time: float,
        bytes_transferred: int,
    ):
        """Update request log with response details"""
        try:
            async with self.db_pool.acquire() as conn:
                await conn.execute(
                    """
                    UPDATE request_logs 
                    SET status_code = $1, response_time = $2, bytes_transferred = $3, completed_at = $4
                    WHERE request_id = $5
                    """,
                    status_code,
                    response_time,
                    bytes_transferred,
                    datetime.utcnow(),
                    request_id,
                )
        except Exception as e:
            logger.error(f"Error updating request log: {e}")

    async def start(self):
        """Start the API Gateway service"""
        logger.info("Starting API Gateway service...")

        # Initialize connections
        await self.init_db()
        await self.init_redis()

        # Start HTTP server
        runner = web.AppRunner(self.app)
        await runner.setup()

        site = web.TCPSite(
            runner, self.config.get("host", "0.0.0.0"), self.config.get("port", 8080)
        )
        await site.start()

        logger.info(
            f"API Gateway listening on {self.config.get('host', '0.0.0.0')}:{self.config.get('port', 8080)}"
        )

    async def stop(self):
        """Stop the API Gateway service"""
        logger.info("Stopping API Gateway service...")

        if self.rabbitmq_connection and not self.rabbitmq_connection.is_closed:
            self.rabbitmq_connection.close()
        if self.redis:
            await self.redis.close()
        if self.db_pool:
            await self.db_pool.close()


import os
from dotenv import load_dotenv

load_dotenv()

# Configuration compatible with existing services
CONFIG = {
    "host": "0.0.0.0",
    "port": 8080,
    "max_request_size": 50 * 1024 * 1024,
    "rate_limit_window": 3600,
    "default_rate_limit": 1000,
    "request_timeout": 300,
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
}


async def main():
    """Main entry point"""
    gateway = APIGateway(CONFIG)

    try:
        await gateway.start()

        # Keep running
        while True:
            await asyncio.sleep(1)

    except KeyboardInterrupt:
        logger.info("Received shutdown signal")
    finally:
        await gateway.stop()


if __name__ == "__main__":
    asyncio.run(main())
