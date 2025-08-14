#!/usr/bin/env python3
"""
Enhanced Residential Node Client - Mobile/Desktop Proxy Node
==========================================================

This is the client application that runs on residential devices (mobile phones, computers)
to provide proxy services. It connects to the Node Communication Service and handles:

- HTTP/HTTPS proxy requests
- SOCKS5 proxy connections
- Secure WebSocket communication with authentication
- Automatic reconnection and health monitoring
- Traffic metrics and reporting
- Connection pooling and optimization
- Security features and request filtering

Compatible with the Node Communication Service and the broader proxy system.
"""

import asyncio
import json
import logging
import time
import uuid
import platform
import psutil
import gzip
import base64
import ssl
import socket
from typing import Dict, List, Optional, Set, Callable, Any, Tuple
from dataclasses import dataclass, asdict
from datetime import datetime, timedelta
from enum import Enum
from collections import defaultdict, deque
import threading
import signal
import sys
import os

import websockets
from websockets.client import WebSocketClientProtocol
from websockets.exceptions import ConnectionClosed, WebSocketException
import httpx
import aiohttp
from aiohttp import ClientSession
import aiosocks
from aiosocks import Socks5Addr
import certifi

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("node_client.log", mode="a"),
    ],
)
logger = logging.getLogger(__name__)


class NodeState(Enum):
    DISCONNECTED = "disconnected"
    CONNECTING = "connecting"
    CONNECTED = "connected"
    AUTHENTICATING = "authenticating"
    AUTHENTICATED = "authenticated"
    ACTIVE = "active"
    ERROR = "error"
    SHUTDOWN = "shutdown"


class RequestType(Enum):
    HTTP = "http_request"
    HTTPS_TUNNEL = "https_tunnel_create"
    SOCKS5 = "socks5_connect"


@dataclass
class NodeConfig:
    # Server connection
    server_url: str = "ws://localhost:8010/ws/node"
    auth_token: str = ""
    node_id: str = ""

    # Connection settings
    ping_interval: int = 30
    ping_timeout: int = 10
    connection_timeout: int = 30
    max_reconnect_attempts: int = 50
    reconnect_delay: int = 5
    max_reconnect_delay: int = 300

    # Performance settings
    max_concurrent_requests: int = 20
    request_timeout: int = 30
    tunnel_buffer_size: int = 8192
    max_response_size: int = 10 * 1024 * 1024  # 10MB

    # Security settings
    verify_ssl: bool = True
    allowed_domains: List[str] = None
    blocked_domains: List[str] = None
    max_request_rate: int = 100  # requests per minute

    # Features
    enable_compression: bool = True
    enable_metrics: bool = True
    enable_filtering: bool = True
    enable_caching: bool = False


@dataclass
class RequestMetrics:
    total_requests: int = 0
    successful_requests: int = 0
    failed_requests: int = 0
    total_bytes_sent: int = 0
    total_bytes_received: int = 0
    avg_response_time: float = 0.0
    requests_per_minute: float = 0.0
    last_updated: float = 0.0

    def update_response_time(self, response_time: float):
        if self.successful_requests == 0:
            self.avg_response_time = response_time
        else:
            # Exponential moving average
            self.avg_response_time = 0.9 * self.avg_response_time + 0.1 * response_time


@dataclass
class ActiveTunnel:
    tunnel_id: str
    target_host: str
    target_port: int
    reader: Optional[asyncio.StreamReader] = None
    writer: Optional[asyncio.StreamWriter] = None
    created_at: float = 0.0
    bytes_transferred: int = 0
    last_activity: float = 0.0

    def update_activity(self):
        self.last_activity = time.time()


@dataclass
class ActiveRequest:
    request_id: str
    request_type: RequestType
    created_at: float
    timeout: int
    session: Optional[ClientSession] = None

    def is_expired(self) -> bool:
        return time.time() - self.created_at > self.timeout


class ResidentialNodeClient:
    """Main client class for residential proxy nodes"""

    def __init__(self, config: NodeConfig):
        self.config = config
        self.state = NodeState.DISCONNECTED

        # Connection management
        self.websocket: Optional[WebSocketClientProtocol] = None
        self.connection_lock = asyncio.Lock()
        self.reconnect_attempts = 0
        self.last_connection_time = 0.0

        # Request handling
        self.active_requests: Dict[str, ActiveRequest] = {}
        self.active_tunnels: Dict[str, ActiveTunnel] = {}
        self.request_semaphore = asyncio.Semaphore(config.max_concurrent_requests)

        # Sessions for HTTP requests
        self.http_session: Optional[ClientSession] = None
        self.session_lock = asyncio.Lock()

        # Metrics and monitoring
        self.metrics = RequestMetrics()
        self.response_times: deque = deque(maxlen=1000)
        self.last_ping_time = 0.0

        # Rate limiting
        self.request_times: deque = deque(maxlen=config.max_request_rate)

        # Background tasks
        self.running = False
        self.background_tasks: List[asyncio.Task] = []

        # Node information
        self.node_info = self._get_node_info()

        # Shutdown handling
        self._setup_signal_handlers()

    def _get_node_info(self) -> Dict:
        """Get node system information"""
        try:
            return {
                "platform": platform.platform(),
                "python_version": platform.python_version(),
                "cpu_count": psutil.cpu_count(),
                "memory_total": psutil.virtual_memory().total,
                "network_interfaces": len(psutil.net_if_addrs()),
                "local_ip": self._get_local_ip(),
                "capabilities": [
                    "http_proxy",
                    "https_tunnel",
                    "socks5_proxy",
                    "compression",
                    "metrics",
                ],
            }
        except Exception as e:
            logger.error(f"Error getting node info: {e}")
            return {"capabilities": ["http_proxy"]}

    def _get_local_ip(self) -> str:
        """Get local IP address"""
        try:
            # Connect to a remote address to determine local IP
            with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
                s.connect(("8.8.8.8", 80))
                return s.getsockname()[0]
        except:
            return "127.0.0.1"

    def _setup_signal_handlers(self):
        """Setup signal handlers for graceful shutdown"""

        def signal_handler(signum, frame):
            logger.info(f"Received signal {signum}, shutting down...")
            asyncio.create_task(self.shutdown())

        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)

    async def start(self):
        """Start the node client"""
        self.running = True
        logger.info(f"Starting residential node client {self.config.node_id}")

        try:
            # Initialize HTTP session
            await self._initialize_http_session()

            # Start background tasks
            self.background_tasks = [
                asyncio.create_task(self._connection_manager()),
                asyncio.create_task(self._metrics_updater()),
                asyncio.create_task(self._cleanup_expired_requests()),
                asyncio.create_task(self._health_monitor()),
            ]

            # Wait for all tasks
            await asyncio.gather(*self.background_tasks, return_exceptions=True)

        except Exception as e:
            logger.error(f"Error starting node client: {e}")
        finally:
            await self.shutdown()

    async def shutdown(self):
        """Gracefully shutdown the node client"""
        logger.info("Shutting down node client...")
        self.running = False
        self.state = NodeState.SHUTDOWN

        # Cancel background tasks
        for task in self.background_tasks:
            if not task.done():
                task.cancel()

        # Close all active tunnels
        for tunnel in list(self.active_tunnels.values()):
            await self._close_tunnel(tunnel.tunnel_id)

        # Close WebSocket connection
        if self.websocket:
            try:
                await self.websocket.close()
            except:
                pass

        # Close HTTP session
        if self.http_session:
            await self.http_session.close()

        logger.info("Node client shutdown completed")

    async def _initialize_http_session(self):
        """Initialize HTTP client session"""
        try:
            connector = aiohttp.TCPConnector(
                limit=self.config.max_concurrent_requests,
                limit_per_host=10,
                ttl_dns_cache=300,
                use_dns_cache=True,
                ssl=(
                    ssl.create_default_context(cafile=certifi.where())
                    if self.config.verify_ssl
                    else False
                ),
            )

            timeout = aiohttp.ClientTimeout(total=self.config.request_timeout)

            self.http_session = ClientSession(
                connector=connector,
                timeout=timeout,
                headers={"User-Agent": f"ResidentialNode/{self.config.node_id}"},
            )

        except Exception as e:
            logger.error(f"Error initializing HTTP session: {e}")
            raise

    async def _connection_manager(self):
        """Manage WebSocket connection with auto-reconnection"""
        while self.running:
            try:
                if self.state in [NodeState.DISCONNECTED, NodeState.ERROR]:
                    await self._connect_to_server()

                # Wait before next check
                await asyncio.sleep(5)

            except Exception as e:
                logger.error(f"Connection manager error: {e}")
                await asyncio.sleep(10)

    async def _connect_to_server(self):
        """Connect to the proxy server"""
        if self.reconnect_attempts >= self.config.max_reconnect_attempts:
            logger.error("Maximum reconnection attempts reached")
            self.running = False
            return

        try:
            async with self.connection_lock:
                self.state = NodeState.CONNECTING

                # Calculate backoff delay
                delay = min(
                    self.config.reconnect_delay * (2**self.reconnect_attempts),
                    self.config.max_reconnect_delay,
                )

                if self.reconnect_attempts > 0:
                    logger.info(
                        f"Reconnecting in {delay} seconds... (attempt {self.reconnect_attempts + 1})"
                    )
                    await asyncio.sleep(delay)

                # Create SSL context
                ssl_context = None
                if self.config.server_url.startswith("ws://"):
                    # ssl_context = ssl.create_default_context(cafile=certifi.where())
                    if not self.config.verify_ssl:
                        ssl_context.check_hostname = False
                        ssl_context.verify_mode = ssl.CERT_NONE

                # Connect to server
                logger.info(f"Connecting to {self.config.server_url}")

                self.websocket = await websockets.connect(
                    self.config.server_url,
                    # ssl=ssl_context,
                    ping_interval=self.config.ping_interval,
                    ping_timeout=self.config.ping_timeout,
                    close_timeout=10,
                    max_size=2**23,  # 8MB max message size
                    compression="deflate" if self.config.enable_compression else None,
                )

                self.state = NodeState.CONNECTED
                self.last_connection_time = time.time()
                logger.info("WebSocket connection established")

                # Register with server
                await self._register_with_server()

                # Start message handling
                await self._handle_messages()

        except Exception as e:
            logger.error(f"Connection error: {e}")
            self.state = NodeState.ERROR
            self.reconnect_attempts += 1

            if self.websocket:
                try:
                    await self.websocket.close()
                except:
                    pass
                self.websocket = None

    async def _register_with_server(self):
        """Register node with the server"""
        try:
            self.state = NodeState.AUTHENTICATING

            registration_message = {
                "type": "node_register",
                "node_id": self.config.node_id,
                "auth_token": self.config.auth_token,
                "capabilities": self.node_info["capabilities"],
                "user_agent": f"ResidentialNode/{self.config.node_id}",
                "system_info": self.node_info,
                "timestamp": time.time(),
            }

            await self._send_message(registration_message)

            # Wait for authentication response
            auth_timeout = 30
            start_time = time.time()

            while time.time() - start_time < auth_timeout:
                if self.state == NodeState.ACTIVE:
                    break
                await asyncio.sleep(0.1)

            if self.state != NodeState.ACTIVE:
                raise Exception("Authentication timeout")

            self.reconnect_attempts = 0
            logger.info("Node registered and authenticated successfully")

        except Exception as e:
            logger.error(f"Registration error: {e}")
            raise

    async def _handle_messages(self):
        """Handle incoming messages from server"""
        try:
            async for raw_message in self.websocket:
                try:
                    message = json.loads(raw_message)
                    await self._process_message(message)

                except json.JSONDecodeError:
                    logger.error("Received invalid JSON message")
                except Exception as e:
                    logger.error(f"Error processing message: {e}")

        except ConnectionClosed:
            logger.info("WebSocket connection closed")
            self.state = NodeState.DISCONNECTED
        except Exception as e:
            logger.error(f"Message handling error: {e}")
            self.state = NodeState.ERROR

    async def _process_message(self, message: Dict):
        """Process incoming message from server"""
        try:
            message_type = message.get("type")

            if message_type == "node_auth":
                await self._handle_auth_response(message)
            elif message_type == "http_request":
                await self._handle_http_request(message)
            elif message_type == "https_tunnel_create":
                await self._handle_https_tunnel_create(message)
            elif message_type == "https_tunnel_data":
                await self._handle_https_tunnel_data(message)
            elif message_type == "https_tunnel_close":
                await self._handle_https_tunnel_close(message)
            elif message_type == "socks5_connect":
                await self._handle_socks5_connect(message)
            elif message_type == "socks5_data":
                await self._handle_socks5_data(message)
            elif message_type == "ping":
                await self._handle_ping(message)
            elif message_type == "node_status":
                await self._handle_status_request(message)
            else:
                logger.warning(f"Unknown message type: {message_type}")

        except Exception as e:
            logger.error(f"Error processing message type {message.get('type')}: {e}")

    async def _handle_auth_response(self, message: Dict):
        """Handle authentication response"""
        try:
            status = message.get("status")

            if status == "success":
                self.state = NodeState.ACTIVE
                logger.info("Authentication successful")
            else:
                error = message.get("error", "Authentication failed")
                logger.error(f"Authentication failed: {error}")
                raise Exception(f"Authentication failed: {error}")

        except Exception as e:
            logger.error(f"Auth response error: {e}")
            raise

    async def _handle_http_request(self, message: Dict):
        """Handle HTTP request"""
        request_id = message.get("request_id")

        try:
            # Check rate limiting
            if not self._check_rate_limit():
                await self._send_error_response(request_id, "Rate limit exceeded")
                return

            # Check domain filtering
            url = message.get("url", "")
            if not self._is_url_allowed(url):
                await self._send_error_response(request_id, "Domain not allowed")
                return

            # Acquire semaphore for concurrent request limiting
            async with self.request_semaphore:
                await self._execute_http_request(message)

        except Exception as e:
            logger.error(f"HTTP request error: {e}")
            await self._send_error_response(request_id, str(e))

    async def _execute_http_request(self, message: Dict):
        """Execute HTTP request"""
        request_id = message.get("request_id")
        start_time = time.time()

        try:
            # Create active request tracking
            active_request = ActiveRequest(
                request_id=request_id,
                request_type=RequestType.HTTP,
                created_at=start_time,
                timeout=message.get("timeout", self.config.request_timeout),
            )

            self.active_requests[request_id] = active_request

            # Extract request parameters
            method = message.get("method", "GET")
            url = message.get("url")
            headers = message.get("headers", {})
            body = message.get("body", "")

            # Filter headers
            filtered_headers = self._filter_headers(headers)

            # Make request
            async with self.http_session.request(
                method=method,
                url=url,
                headers=filtered_headers,
                data=body if body else None,
                timeout=aiohttp.ClientTimeout(total=active_request.timeout),
                allow_redirects=True,
                max_redirects=5,
            ) as response:

                # Read response
                response_body = await response.read()

                # Check response size
                if len(response_body) > self.config.max_response_size:
                    raise Exception("Response too large")

                # Prepare response headers
                response_headers = dict(response.headers)

                # Send response
                response_message = {
                    "type": "http_response",
                    "request_id": request_id,
                    "task_id": message.get("task_id"),
                    "status_code": response.status,
                    "headers": response_headers,
                    "body": base64.b64encode(response_body).decode("utf-8"),
                    "response_time": time.time() - start_time,
                    "node_id": self.config.node_id,
                    "timestamp": time.time(),
                }

                await self._send_message(response_message)

                # Update metrics
                self.metrics.successful_requests += 1
                self.metrics.total_bytes_sent += len(body.encode() if body else b"")
                self.metrics.total_bytes_received += len(response_body)
                self.metrics.update_response_time(time.time() - start_time)
                self.response_times.append(time.time() - start_time)

                logger.debug(
                    f"HTTP request completed: {method} {url} -> {response.status}"
                )

        except asyncio.TimeoutError:
            await self._send_error_response(request_id, "Request timeout")
            self.metrics.failed_requests += 1
        except Exception as e:
            await self._send_error_response(request_id, str(e))
            self.metrics.failed_requests += 1
        finally:
            # Cleanup
            self.active_requests.pop(request_id, None)
            self.metrics.total_requests += 1

    async def _handle_https_tunnel_create(self, message: Dict):
        """Handle HTTPS tunnel creation"""
        tunnel_id = message.get("tunnel_id")

        try:
            target_host = message.get("target_host")
            target_port = message.get("target_port", 443)

            # Check if tunnel already exists
            if tunnel_id in self.active_tunnels:
                await self._send_tunnel_error(tunnel_id, "Tunnel already exists")
                return

            # Create connection to target
            reader, writer = await asyncio.wait_for(
                asyncio.open_connection(target_host, target_port),
                timeout=self.config.connection_timeout,
            )

            # Create tunnel tracking
            tunnel = ActiveTunnel(
                tunnel_id=tunnel_id,
                target_host=target_host,
                target_port=target_port,
                reader=reader,
                writer=writer,
                created_at=time.time(),
                last_activity=time.time(),
            )

            self.active_tunnels[tunnel_id] = tunnel

            # Start tunnel reader task
            asyncio.create_task(self._tunnel_reader(tunnel))

            # Send establishment confirmation
            await self._send_message(
                {
                    "type": "tunnel_established",
                    "tunnel_id": tunnel_id,
                    "task_id": message.get("task_id"),
                    "timestamp": time.time(),
                }
            )

            logger.info(
                f"HTTPS tunnel created: {tunnel_id} -> {target_host}:{target_port}"
            )

        except Exception as e:
            logger.error(f"Tunnel creation error: {e}")
            await self._send_tunnel_error(tunnel_id, str(e))

    async def _tunnel_reader(self, tunnel: ActiveTunnel):
        """Read data from tunnel target and send to server"""
        try:
            while tunnel.tunnel_id in self.active_tunnels:
                data = await tunnel.reader.read(self.config.tunnel_buffer_size)

                if not data:
                    break

                tunnel.update_activity()
                tunnel.bytes_transferred += len(data)

                # Send data to server
                await self._send_message(
                    {
                        "type": "tunnel_data",
                        "tunnel_id": tunnel.tunnel_id,
                        "data": data.hex(),
                    }
                )

        except Exception as e:
            logger.error(f"Tunnel reader error: {e}")
            await self._send_tunnel_error(tunnel.tunnel_id, str(e))
        finally:
            await self._close_tunnel(tunnel.tunnel_id)

    async def _handle_https_tunnel_data(self, message: Dict):
        """Handle HTTPS tunnel data"""
        try:
            tunnel_id = message.get("tunnel_id")
            data_hex = message.get("data", "")

            tunnel = self.active_tunnels.get(tunnel_id)
            if not tunnel or not tunnel.writer:
                logger.warning(f"Received data for unknown tunnel: {tunnel_id}")
                return

            # Write data to target
            data = bytes.fromhex(data_hex)
            tunnel.writer.write(data)
            await tunnel.writer.drain()

            tunnel.update_activity()
            tunnel.bytes_transferred += len(data)

        except Exception as e:
            logger.error(f"Tunnel data error: {e}")
            tunnel_id = message.get("tunnel_id")
            if tunnel_id:
                await self._send_tunnel_error(tunnel_id, str(e))

    async def _handle_https_tunnel_close(self, message: Dict):
        """Handle HTTPS tunnel close"""
        tunnel_id = message.get("tunnel_id")
        await self._close_tunnel(tunnel_id)

    async def _close_tunnel(self, tunnel_id: str):
        """Close HTTPS tunnel"""
        try:
            tunnel = self.active_tunnels.pop(tunnel_id, None)
            if tunnel:
                if tunnel.writer:
                    tunnel.writer.close()
                    try:
                        await tunnel.writer.wait_closed()
                    except:
                        pass

                # Notify server
                await self._send_message(
                    {
                        "type": "tunnel_closed",
                        "tunnel_id": tunnel_id,
                        "bytes_transferred": tunnel.bytes_transferred,
                        "timestamp": time.time(),
                    }
                )

                logger.info(f"Tunnel closed: {tunnel_id}")

        except Exception as e:
            logger.error(f"Error closing tunnel {tunnel_id}: {e}")

    async def _handle_socks5_connect(self, message: Dict):
        """Handle SOCKS5 connection request"""
        request_id = message.get("request_id")

        try:
            target_host = message.get("target_host")
            target_port = message.get("target_port")

            # Create SOCKS5 connection (simplified implementation)
            reader, writer = await asyncio.wait_for(
                asyncio.open_connection(target_host, target_port),
                timeout=self.config.connection_timeout,
            )

            # Send connection established
            await self._send_message(
                {
                    "type": "socks5_connected",
                    "request_id": request_id,
                    "task_id": message.get("task_id"),
                    "timestamp": time.time(),
                }
            )

            # Start data forwarding (simplified)
            asyncio.create_task(self._socks5_forwarder(request_id, reader, writer))

        except Exception as e:
            logger.error(f"SOCKS5 connection error: {e}")
            await self._send_error_response(request_id, str(e))

    async def _socks5_forwarder(
        self,
        request_id: str,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ):
        """Forward SOCKS5 data"""
        try:
            while True:
                data = await reader.read(self.config.tunnel_buffer_size)
                if not data:
                    break

                await self._send_message(
                    {
                        "type": "socks5_response",
                        "request_id": request_id,
                        "data": data.hex(),
                    }
                )

        except Exception as e:
            logger.error(f"SOCKS5 forwarder error: {e}")
        finally:
            writer.close()
            try:
                await writer.wait_closed()
            except:
                pass

    async def _handle_socks5_data(self, message: Dict):
        """Handle SOCKS5 data"""
        # Implementation would be more complex for full SOCKS5 support
        pass

    async def _handle_ping(self, message: Dict):
        """Handle ping from server"""
        try:
            # Send pong response
            await self._send_message(
                {
                    "type": "pong",
                    "timestamp": time.time(),
                    "metrics": self._get_current_metrics(),
                }
            )

            self.last_ping_time = time.time()

        except Exception as e:
            logger.error(f"Ping handling error: {e}")

    async def _handle_status_request(self, message: Dict):
        """Handle status request from server"""
        try:
            status_data = {
                "type": "node_status",
                "node_id": self.config.node_id,
                "status": (
                    "active" if self.state == NodeState.ACTIVE else self.state.value
                ),
                "metrics": self._get_current_metrics(),
                "active_requests": len(self.active_requests),
                "active_tunnels": len(self.active_tunnels),
                "uptime": time.time() - self.last_connection_time,
                "timestamp": time.time(),
            }

            await self._send_message(status_data)

        except Exception as e:
            logger.error(f"Status request error: {e}")

    async def _send_message(self, message: Dict):
        """Send message to server"""
        try:
            if self.websocket:
                await self.websocket.send(json.dumps(message))
            else:
                logger.warning("Cannot send message: WebSocket not connected")

        except Exception as e:
            logger.error(f"Error sending message: {e}")
            self.state = NodeState.ERROR

    async def _send_error_response(self, request_id: str, error_message: str):
        """Send error response"""
        try:
            error_response = {
                "type": "error",
                "request_id": request_id,
                "error_type": "request_error",
                "message": error_message,
                "timestamp": time.time(),
            }

            await self._send_message(error_response)

        except Exception as e:
            logger.error(f"Error sending error response: {e}")

    async def _send_tunnel_error(self, tunnel_id: str, error_message: str):
        """Send tunnel error"""
        try:
            error_response = {
                "type": "tunnel_error",
                "tunnel_id": tunnel_id,
                "error": error_message,
                "timestamp": time.time(),
            }

            await self._send_message(error_response)

        except Exception as e:
            logger.error(f"Error sending tunnel error: {e}")

    def _check_rate_limit(self) -> bool:
        """Check if request is within rate limits"""
        try:
            current_time = time.time()

            # Remove old requests (older than 1 minute)
            while self.request_times and current_time - self.request_times[0] > 60:
                self.request_times.popleft()

            # Check if we can accept new request
            if len(self.request_times) >= self.config.max_request_rate:
                return False

            # Add current request
            self.request_times.append(current_time)
            return True

        except Exception as e:
            logger.error(f"Rate limit check error: {e}")
            return True  # Allow on error

    def _is_url_allowed(self, url: str) -> bool:
        """Check if URL is allowed based on filtering rules"""
        try:
            if not self.config.enable_filtering:
                return True

            from urllib.parse import urlparse

            parsed = urlparse(url)
            domain = parsed.netloc.lower()

            # Check blocked domains
            if self.config.blocked_domains:
                for blocked in self.config.blocked_domains:
                    if blocked.lower() in domain:
                        return False

            # Check allowed domains (if specified)
            if self.config.allowed_domains:
                for allowed in self.config.allowed_domains:
                    if allowed.lower() in domain:
                        return True
                return False  # Not in allowed list

            return True

        except Exception as e:
            logger.error(f"URL filtering error: {e}")
            return True  # Allow on error

    def _filter_headers(self, headers: Dict[str, str]) -> Dict[str, str]:
        """Filter and clean request headers"""
        try:
            # Headers to remove for security/privacy
            blocked_headers = {
                "x-forwarded-for",
                "x-real-ip",
                "x-forwarded-proto",
                "x-original-forwarded-for",
                "cf-connecting-ip",
                "true-client-ip",
            }

            filtered = {}
            for key, value in headers.items():
                if key.lower() not in blocked_headers:
                    filtered[key] = value

            # Add our own headers
            filtered["User-Agent"] = filtered.get(
                "User-Agent", f"ResidentialNode/{self.config.node_id}"
            )

            return filtered

        except Exception as e:
            logger.error(f"Header filtering error: {e}")
            return headers

    def _get_current_metrics(self) -> Dict:
        """Get current node metrics"""
        try:
            current_time = time.time()

            # Calculate requests per minute
            recent_responses = [t for t in self.response_times if current_time - t < 60]
            rpm = len(recent_responses)

            # Get system metrics
            cpu_percent = psutil.cpu_percent(interval=None)
            memory = psutil.virtual_memory()

            return {
                "total_requests": self.metrics.total_requests,
                "successful_requests": self.metrics.successful_requests,
                "failed_requests": self.metrics.failed_requests,
                "requests_per_minute": rpm,
                "avg_response_time": self.metrics.avg_response_time,
                "total_bytes_sent": self.metrics.total_bytes_sent,
                "total_bytes_received": self.metrics.total_bytes_received,
                "active_requests": len(self.active_requests),
                "active_tunnels": len(self.active_tunnels),
                "cpu_percent": cpu_percent,
                "memory_percent": memory.percent,
                "memory_available": memory.available,
                "connection_state": self.state.value,
                "last_ping": (
                    current_time - self.last_ping_time if self.last_ping_time > 0 else 0
                ),
                "uptime": (
                    current_time - self.last_connection_time
                    if self.last_connection_time > 0
                    else 0
                ),
            }

        except Exception as e:
            logger.error(f"Error getting metrics: {e}")
            return {}

    # Background tasks
    async def _metrics_updater(self):
        """Background task to update and send metrics"""
        while self.running:
            try:
                if self.state == NodeState.ACTIVE:
                    # Send heartbeat with metrics
                    await self._send_message(
                        {
                            "type": "node_heartbeat",
                            "node_id": self.config.node_id,
                            "metrics": self._get_current_metrics(),
                            "timestamp": time.time(),
                        }
                    )

                await asyncio.sleep(60)  # Send metrics every minute

            except Exception as e:
                logger.error(f"Metrics updater error: {e}")
                await asyncio.sleep(30)

    async def _cleanup_expired_requests(self):
        """Background task to cleanup expired requests"""
        while self.running:
            try:
                current_time = time.time()
                expired_requests = []

                for request_id, request in list(self.active_requests.items()):
                    if request.is_expired():
                        expired_requests.append(request_id)

                for request_id in expired_requests:
                    request = self.active_requests.pop(request_id, None)
                    if request:
                        await self._send_error_response(request_id, "Request timeout")
                        self.metrics.failed_requests += 1
                        logger.warning(f"Request {request_id} expired")

                # Cleanup old tunnels
                inactive_tunnels = []
                for tunnel_id, tunnel in list(self.active_tunnels.items()):
                    if current_time - tunnel.last_activity > 300:  # 5 minutes inactive
                        inactive_tunnels.append(tunnel_id)

                for tunnel_id in inactive_tunnels:
                    await self._close_tunnel(tunnel_id)
                    logger.info(f"Closed inactive tunnel: {tunnel_id}")

                await asyncio.sleep(30)  # Check every 30 seconds

            except Exception as e:
                logger.error(f"Cleanup task error: {e}")
                await asyncio.sleep(60)

    async def _health_monitor(self):
        """Background task to monitor node health"""
        while self.running:
            try:
                # Check WebSocket connection health
                if self.websocket:
                    logger.warning("WebSocket connection lost")
                    self.state = NodeState.DISCONNECTED

                # Check if we haven't received a ping in too long
                if (
                    self.last_ping_time > 0
                    and time.time() - self.last_ping_time
                    > self.config.ping_interval * 3
                ):
                    logger.warning(
                        "No ping received for too long, connection may be dead"
                    )
                    self.state = NodeState.ERROR

                # Monitor system resources
                memory = psutil.virtual_memory()
                if memory.percent > 90:
                    logger.warning(f"High memory usage: {memory.percent}%")

                cpu_percent = psutil.cpu_percent(interval=1)
                if cpu_percent > 90:
                    logger.warning(f"High CPU usage: {cpu_percent}%")

                await asyncio.sleep(30)

            except Exception as e:
                logger.error(f"Health monitor error: {e}")
                await asyncio.sleep(60)


class NodeConfigLoader:
    """Helper class to load node configuration"""

    @staticmethod
    def load_from_file(config_path: str) -> NodeConfig:
        """Load configuration from file"""
        try:
            import yaml

            with open(config_path, "r") as f:
                config_data = yaml.safe_load(f)

            print(config_data)
            return NodeConfig(**config_data)

        except Exception as e:
            logger.error(f"Error loading config from {config_path}: {e}")
            return NodeConfig()

    @staticmethod
    def load_from_env() -> NodeConfig:
        """Load configuration from environment variables"""
        try:
            config = NodeConfig()

            # Connection settings
            config.server_url = os.getenv("PROXY_SERVER_URL", config.server_url)
            config.auth_token = os.getenv("PROXY_AUTH_TOKEN", config.auth_token)
            config.node_id = os.getenv("PROXY_NODE_ID", config.node_id)

            # Performance settings
            config.max_concurrent_requests = int(
                os.getenv("PROXY_MAX_CONCURRENT", config.max_concurrent_requests)
            )
            config.request_timeout = int(
                os.getenv("PROXY_REQUEST_TIMEOUT", config.request_timeout)
            )

            # Security settings
            config.verify_ssl = os.getenv("PROXY_VERIFY_SSL", "true").lower() == "true"

            # Features
            config.enable_compression = (
                os.getenv("PROXY_ENABLE_COMPRESSION", "true").lower() == "true"
            )
            config.enable_metrics = (
                os.getenv("PROXY_ENABLE_METRICS", "true").lower() == "true"
            )
            config.enable_filtering = (
                os.getenv("PROXY_ENABLE_FILTERING", "true").lower() == "true"
            )

            return config

        except Exception as e:
            logger.error(f"Error loading config from environment: {e}")
            return NodeConfig()

    @staticmethod
    def create_default_config() -> NodeConfig:
        """Create default configuration with auto-generated node ID"""
        config = NodeConfig()

        # Generate unique node ID
        import hashlib
        import platform

        machine_id = f"{platform.node()}-{platform.machine()}-{uuid.uuid4().hex[:8]}"
        config.node_id = f"node_{hashlib.md5(machine_id.encode()).hexdigest()[:12]}"

        return config


def main():
    """Main entry point"""
    import argparse

    parser = argparse.ArgumentParser(description="Residential Proxy Node Client")
    parser.add_argument("--config", "-c", help="Configuration file path")
    parser.add_argument("--server-url", "-s", help="Proxy server WebSocket URL")
    parser.add_argument("--auth-token", "-t", help="Authentication token")
    parser.add_argument("--node-id", "-n", help="Node identifier")
    parser.add_argument(
        "--debug", "-d", action="store_true", help="Enable debug logging"
    )
    parser.add_argument(
        "--verify-ssl",
        action="store_true",
        default=True,
        help="Verify SSL certificates",
    )
    parser.add_argument(
        "--no-verify-ssl",
        action="store_true",
        help="Disable SSL certificate verification",
    )

    args = parser.parse_args()

    # Setup logging
    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)

    try:
        # Load configuration
        if args.config:
            config = NodeConfigLoader.load_from_file(args.config)
        else:
            config = NodeConfigLoader.load_from_env()

        # Override with command line arguments
        if args.server_url:
            config.server_url = args.server_url
        if args.auth_token:
            config.auth_token = args.auth_token
        if args.node_id:
            config.node_id = args.node_id
        if args.no_verify_ssl:
            config.verify_ssl = False

        # Validate required settings
        if not config.server_url:
            logger.error("Server URL is required")
            sys.exit(1)

        if not config.auth_token:
            logger.error("Authentication token is required")
            sys.exit(1)

        if not config.node_id:
            # Auto-generate if not provided
            default_config = NodeConfigLoader.create_default_config()
            config.node_id = default_config.node_id
            logger.info(f"Generated node ID: {config.node_id}")

        # Create and start node client
        logger.info(f"Starting residential proxy node client")
        logger.info(f"Node ID: {config.node_id}")
        logger.info(f"Server URL: {config.server_url}")
        logger.info(f"Max concurrent requests: {config.max_concurrent_requests}")

        client = ResidentialNodeClient(config)

        # Run the client
        asyncio.run(client.start())

    except KeyboardInterrupt:
        logger.info("Interrupted by user")
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()



