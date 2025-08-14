#!/usr/bin/env python3
"""
Node Manager Service - The brain that manages residential node states and health
Handles node registration, health monitoring, load balancing, and state management
"""

import asyncio
import json
import logging
import time
import uuid
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass, asdict
from enum import Enum

import pika
import asyncpg
from pydantic import BaseModel, Field
from redis import Redis
from redis import asyncio as aioredis

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class NodeStatus(Enum):
    ONLINE = "online"
    OFFLINE = "offline"
    BUSY = "busy"
    MAINTENANCE = "maintenance"
    UNHEALTHY = "unhealthy"


class NodeCapability(Enum):
    HTTP_PROXY = "http_proxy"
    HTTPS_TUNNEL = "https_tunnel"
    SOCKS5 = "socks5"


@dataclass
class NodeMetrics:
    cpu_usage: float
    memory_usage: float
    active_connections: int
    requests_per_minute: float
    error_rate: float
    response_time_avg: float
    last_updated: float


@dataclass
class NodeInfo:
    node_id: str
    ip_address: str
    port: int
    status: NodeStatus
    capabilities: List[NodeCapability]
    metrics: NodeMetrics
    last_ping: float
    registered_at: float
    total_requests: int = 0
    successful_requests: int = 0
    failed_requests: int = 0

    def health_score(self) -> float:
        """Calculate node health score (0-100)"""
        if self.status == NodeStatus.OFFLINE:
            return 0.0

        # Base score
        score = 100.0

        # Penalize high CPU usage
        if self.metrics.cpu_usage > 80:
            score -= (self.metrics.cpu_usage - 80) * 2

        # Penalize high memory usage
        if self.metrics.memory_usage > 80:
            score -= (self.metrics.memory_usage - 80) * 2

        # Penalize high error rate
        if self.metrics.error_rate > 0.05:  # 5%
            score -= self.metrics.error_rate * 100

        # Penalize slow response times
        if self.metrics.response_time_avg > 2000:  # 2 seconds
            score -= (self.metrics.response_time_avg - 2000) / 100

        # Ping freshness factor
        ping_age = time.time() - self.last_ping
        if ping_age > 30:  # 30 seconds
            score -= ping_age * 2

        return max(0.0, min(100.0, score))


class NodeManagerService:
    def __init__(self, config: Dict):
        self.config = config
        self.nodes: Dict[str, NodeInfo] = {}
        self.db_pool: Optional[asyncpg.Pool] = None
        self.redis: Optional[aioredis.Redis] = None
        self.rabbitmq_connection: Optional[pika.BlockingConnection] = None
        self.channel: Optional[pika.channel.Channel] = None
        self.running = False

        # Health check settings
        self.health_check_interval = config.get("health_check_interval", 30)
        self.node_timeout = config.get("node_timeout", 120)
        self.unhealthy_threshold = config.get("unhealthy_threshold", 30.0)

    async def initialize(self):
        """Initialize all connections and start background tasks"""
        logger.info("Initializing Node Manager Service...")

        try:
            # Initialize PostgreSQL connection pool
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

            # Load existing nodes from database
            await self._load_nodes_from_db()

            logger.info("Node Manager Service initialized successfully")

        except Exception as e:
            logger.error(f"Failed to initialize Node Manager Service: {e}")
            raise

    async def _setup_rabbitmq(self):
        """Setup RabbitMQ connection and exchanges/queues"""
        try:
            # Connection parameters
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
            self.channel.exchange_declare(
                exchange="node_events", exchange_type="topic", durable=True
            )

            self.channel.exchange_declare(
                exchange="node_commands", exchange_type="direct", durable=True
            )

            # Declare queues
            queues = [
                "node.register",
                "node.heartbeat",
                "node.metrics",
                "node.status_change",
                "node.health_check",
            ]

            for queue_name in queues:
                self.channel.queue_declare(queue=queue_name, durable=True)

            # Bind queues to exchanges
            bindings = [
                ("node.register", "node_events", "node.registered"),
                ("node.heartbeat", "node_events", "node.ping"),
                ("node.metrics", "node_events", "node.metrics"),
                ("node.status_change", "node_events", "node.status.*"),
                ("node.health_check", "node_commands", "health_check"),
            ]

            for queue, exchange, routing_key in bindings:
                self.channel.queue_bind(
                    exchange=exchange, queue=queue, routing_key=routing_key
                )

            logger.info("RabbitMQ setup completed")

        except Exception as e:
            logger.error(f"RabbitMQ setup failed: {e}")
            raise

    async def _load_nodes_from_db(self):
        """Load existing nodes from PostgreSQL database"""
        try:
            async with self.db_pool.acquire() as conn:
                # Create nodes table if not exists
                await conn.execute(
                    """
                    CREATE TABLE IF NOT EXISTS nodes (
                        node_id VARCHAR(255) PRIMARY KEY,
                        ip_address INET NOT NULL,
                        port INTEGER NOT NULL,
                        status VARCHAR(50) NOT NULL,
                        capabilities JSONB NOT NULL,
                        last_ping TIMESTAMP NOT NULL DEFAULT NOW(),
                        registered_at TIMESTAMP NOT NULL DEFAULT NOW(),
                        total_requests BIGINT DEFAULT 0,
                        successful_requests BIGINT DEFAULT 0,
                        failed_requests BIGINT DEFAULT 0,
                        is_active BOOLEAN DEFAULT true
                    )
                """
                )

                # Load active nodes
                rows = await conn.fetch(
                    """
                    SELECT node_id, ip_address, port, status, capabilities, 
                           EXTRACT(EPOCH FROM last_ping) as last_ping,
                           EXTRACT(EPOCH FROM registered_at) as registered_at,
                           total_requests, successful_requests, failed_requests
                    FROM nodes WHERE is_active = true
                """
                )

                for row in rows:
                    # Initialize with default metrics
                    metrics = NodeMetrics(
                        cpu_usage=0.0,
                        memory_usage=0.0,
                        active_connections=0,
                        requests_per_minute=0.0,
                        error_rate=0.0,
                        response_time_avg=0.0,
                        last_updated=time.time(),
                    )

                    node_info = NodeInfo(
                        node_id=row["node_id"],
                        ip_address=str(row["ip_address"]),
                        port=row["port"],
                        status=NodeStatus(row["status"]),
                        capabilities=[
                            NodeCapability(cap) for cap in row["capabilities"]
                        ],
                        metrics=metrics,
                        last_ping=row["last_ping"],
                        registered_at=row["registered_at"],
                        total_requests=row["total_requests"],
                        successful_requests=row["successful_requests"],
                        failed_requests=row["failed_requests"],
                    )

                    self.nodes[row["node_id"]] = node_info

                    # Cache in Redis
                    await self.redis.hset(
                        f"node:{row['node_id']}",
                        mapping={
                            "status": node_info.status.value,
                            "last_ping": node_info.last_ping,
                            "health_score": node_info.health_score(),
                        },
                    )

                logger.info(f"Loaded {len(self.nodes)} nodes from database")

        except Exception as e:
            logger.error(f"Failed to load nodes from database: {e}")
            raise

    async def register_node(self, node_data: Dict) -> bool:
        """Register a new node"""
        try:
            node_id = node_data.get("node_id")
            if not node_id:
                logger.error("Node registration failed: missing node_id")
                return False

            # Create node info
            metrics = NodeMetrics(
                cpu_usage=node_data.get("cpu_usage", 0.0),
                memory_usage=node_data.get("memory_usage", 0.0),
                active_connections=0,
                requests_per_minute=0.0,
                error_rate=0.0,
                response_time_avg=0.0,
                last_updated=time.time(),
            )

            capabilities = [
                NodeCapability(cap)
                for cap in node_data.get("capabilities", ["http_proxy"])
            ]

            node_info = NodeInfo(
                node_id=node_id,
                ip_address=node_data.get("ip_address", "unknown"),
                port=node_data.get("port", 0),
                status=NodeStatus.ONLINE,
                capabilities=capabilities,
                metrics=metrics,
                last_ping=time.time(),
                registered_at=time.time(),
            )

            # Store in memory
            self.nodes[node_id] = node_info

            # Persist to database
            async with self.db_pool.acquire() as conn:
                await conn.execute(
                    """
                    INSERT INTO nodes (node_id, ip_address, port, status, capabilities, last_ping, registered_at)
                    VALUES ($1, $2, $3, $4, $5, NOW(), NOW())
                    ON CONFLICT (node_id) DO UPDATE SET
                        ip_address = EXCLUDED.ip_address,
                        port = EXCLUDED.port,
                        status = EXCLUDED.status,
                        capabilities = EXCLUDED.capabilities,
                        last_ping = NOW(),
                        is_active = true
                """,
                    node_id,
                    node_info.ip_address,
                    node_info.port,
                    node_info.status.value,
                    [cap.value for cap in capabilities],
                )

            # Cache in Redis
            await self.redis.hset(
                f"node:{node_id}",
                mapping={
                    "status": node_info.status.value,
                    "last_ping": node_info.last_ping,
                    "health_score": node_info.health_score(),
                    "ip_address": node_info.ip_address,
                    "port": node_info.port,
                },
            )

            # Publish registration event
            await self._publish_event(
                "node.registered",
                {
                    "node_id": node_id,
                    "status": node_info.status.value,
                    "capabilities": [cap.value for cap in capabilities],
                    "timestamp": time.time(),
                },
            )

            logger.info(f"Node {node_id} registered successfully")
            return True

        except Exception as e:
            logger.error(f"Node registration failed: {e}")
            return False

    async def update_node_heartbeat(self, node_id: str, metrics_data: Dict = None):
        """Update node heartbeat and metrics"""
        try:
            if node_id not in self.nodes:
                logger.warning(f"Heartbeat received from unknown node: {node_id}")
                return False

            node = self.nodes[node_id]
            node.last_ping = time.time()

            # Update metrics if provided
            if metrics_data:
                node.metrics.cpu_usage = metrics_data.get(
                    "cpu_usage", node.metrics.cpu_usage
                )
                node.metrics.memory_usage = metrics_data.get(
                    "memory_usage", node.metrics.memory_usage
                )
                node.metrics.active_connections = metrics_data.get(
                    "active_connections", node.metrics.active_connections
                )
                node.metrics.requests_per_minute = metrics_data.get(
                    "requests_per_minute", node.metrics.requests_per_minute
                )
                node.metrics.error_rate = metrics_data.get(
                    "error_rate", node.metrics.error_rate
                )
                node.metrics.response_time_avg = metrics_data.get(
                    "response_time_avg", node.metrics.response_time_avg
                )
                node.metrics.last_updated = time.time()

            # Update status if currently offline
            if node.status == NodeStatus.OFFLINE:
                await self._update_node_status(node_id, NodeStatus.ONLINE)

            # Update Redis cache
            await self.redis.hset(
                f"node:{node_id}",
                mapping={
                    "last_ping": node.last_ping,
                    "health_score": node.health_score(),
                    "status": node.status.value,
                },
            )

            return True

        except Exception as e:
            logger.error(f"Failed to update heartbeat for node {node_id}: {e}")
            return False

    async def _update_node_status(self, node_id: str, new_status: NodeStatus):
        """Update node status and notify subscribers"""
        try:
            if node_id not in self.nodes:
                return False

            old_status = self.nodes[node_id].status
            self.nodes[node_id].status = new_status

            # Update database
            async with self.db_pool.acquire() as conn:
                await conn.execute(
                    "UPDATE nodes SET status = $1 WHERE node_id = $2",
                    new_status.value,
                    node_id,
                )

            # Update Redis
            await self.redis.hset(f"node:{node_id}", "status", new_status.value)

            # Publish status change event
            await self._publish_event(
                f"node.status.{new_status.value}",
                {
                    "node_id": node_id,
                    "old_status": old_status.value,
                    "new_status": new_status.value,
                    "timestamp": time.time(),
                },
            )

            logger.info(
                f"Node {node_id} status changed from {old_status.value} to {new_status.value}"
            )
            return True

        except Exception as e:
            logger.error(f"Failed to update status for node {node_id}: {e}")
            return False

    async def get_available_nodes(
        self, capability: NodeCapability = None, min_health_score: float = 50.0
    ) -> List[Tuple[str, NodeInfo]]:
        """Get list of available nodes for task assignment"""
        available = []

        for node_id, node in self.nodes.items():
            # Check if node is online and healthy
            if node.status not in [NodeStatus.ONLINE, NodeStatus.BUSY]:
                continue

            # Check capability requirement
            if capability and capability not in node.capabilities:
                continue

            # Check health score
            health_score = node.health_score()
            if health_score < min_health_score:
                continue

            available.append((node_id, node))

        # Sort by health score (descending) and load (ascending)
        available.sort(
            key=lambda x: (-x[1].health_score(), x[1].metrics.active_connections)
        )

        return available

    async def get_best_node(
        self, capability: NodeCapability = None
    ) -> Optional[Tuple[str, NodeInfo]]:
        """Get the best available node for a task"""
        available = await self.get_available_nodes(capability)
        return available[0] if available else None

    async def _health_check_loop(self):
        """Background task to monitor node health"""
        while self.running:
            try:
                current_time = time.time()
                nodes_to_check = list(self.nodes.items())

                for node_id, node in nodes_to_check:
                    # Check if node has timed out
                    if current_time - node.last_ping > self.node_timeout:
                        if node.status != NodeStatus.OFFLINE:
                            logger.warning(
                                f"Node {node_id} timed out, marking as offline"
                            )
                            await self._update_node_status(node_id, NodeStatus.OFFLINE)

                    # Check health score
                    elif node.health_score() < self.unhealthy_threshold:
                        if node.status != NodeStatus.UNHEALTHY:
                            logger.warning(
                                f"Node {node_id} health degraded, marking as unhealthy"
                            )
                            await self._update_node_status(
                                node_id, NodeStatus.UNHEALTHY
                            )

                    # Auto-recovery for healthy nodes
                    elif (
                        node.status == NodeStatus.UNHEALTHY
                        and node.health_score() > self.unhealthy_threshold + 10
                    ):
                        logger.info(
                            f"Node {node_id} health recovered, marking as online"
                        )
                        await self._update_node_status(node_id, NodeStatus.ONLINE)

                await asyncio.sleep(self.health_check_interval)

            except Exception as e:
                logger.error(f"Health check loop error: {e}")
                await asyncio.sleep(5)

    async def _publish_event(self, routing_key: str, data: Dict):
        """Publish event to RabbitMQ"""
        try:
            message = json.dumps(data)
            self.channel.basic_publish(
                exchange="node_events",
                routing_key=routing_key,
                body=message,
                properties=pika.BasicProperties(
                    delivery_mode=2,  # Make message persistent
                    timestamp=int(time.time()),
                ),
            )
        except Exception as e:
            logger.error(f"Failed to publish event {routing_key}: {e}")

    async def get_node_stats(self) -> Dict:
        """Get overall node statistics"""
        total_nodes = len(self.nodes)
        online_nodes = sum(
            1 for n in self.nodes.values() if n.status == NodeStatus.ONLINE
        )
        busy_nodes = sum(1 for n in self.nodes.values() if n.status == NodeStatus.BUSY)
        offline_nodes = sum(
            1 for n in self.nodes.values() if n.status == NodeStatus.OFFLINE
        )
        unhealthy_nodes = sum(
            1 for n in self.nodes.values() if n.status == NodeStatus.UNHEALTHY
        )

        return {
            "total_nodes": total_nodes,
            "online_nodes": online_nodes,
            "busy_nodes": busy_nodes,
            "offline_nodes": offline_nodes,
            "unhealthy_nodes": unhealthy_nodes,
            "avg_health_score": sum(n.health_score() for n in self.nodes.values())
            / max(total_nodes, 1),
            "total_requests": sum(n.total_requests for n in self.nodes.values()),
            "successful_requests": sum(
                n.successful_requests for n in self.nodes.values()
            ),
            "failed_requests": sum(n.failed_requests for n in self.nodes.values()),
        }

    async def start(self):
        """Start the Node Manager Service"""
        self.running = True
        logger.info("Starting Node Manager Service...")

        # Start background tasks
        tasks = [
            asyncio.create_task(self._health_check_loop()),
        ]

        try:
            await asyncio.gather(*tasks)
        except KeyboardInterrupt:
            logger.info("Shutting down Node Manager Service...")
        finally:
            await self.shutdown()

    async def shutdown(self):
        """Gracefully shutdown the service"""
        self.running = False

        # Close connections
        if self.rabbitmq_connection and not self.rabbitmq_connection.is_closed:
            self.rabbitmq_connection.close()

        if self.redis:
            await self.redis.aclose()

        if self.db_pool:
            await self.db_pool.close()

        logger.info("Node Manager Service shutdown completed")

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
    "health_check_interval": 30,
    "node_timeout": 120,
    "unhealthy_threshold": 30.0,
}


async def main():
    """Main entry point"""
    service = NodeManagerService(CONFIG)

    try:
        await service.initialize()
        await service.start()
    except Exception as e:
        logger.error(f"Service failed: {e}")
        await service.shutdown()


if __name__ == "__main__":
    asyncio.run(main())
