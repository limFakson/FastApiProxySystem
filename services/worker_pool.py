#!/usr/bin/env python3
"""
Worker Pool / Queue Processor Service - The engine that processes proxy requests
Pulls tasks from RabbitMQ, coordinates with Node Manager, and executes proxy operations
"""

import asyncio
import json
import logging
import time
import uuid
import threading
from concurrent.futures import ThreadPoolExecutor
from typing import Dict, List, Optional, Any, Callable
from dataclasses import dataclass, asdict
from enum import Enum

import pika
import asyncpg
from redis import asyncio as aioredis
from pydantic import BaseModel, Field

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class TaskType(Enum):
    HTTP_REQUEST = "http_request"
    HTTPS_TUNNEL = "https_tunnel"
    SOCKS5_PROXY = "socks5_proxy"


class TaskStatus(Enum):
    QUEUED = "queued"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"
    TIMEOUT = "timeout"
    RETRYING = "retrying"


class TaskPriority(Enum):
    LOW = 1
    NORMAL = 2
    HIGH = 3
    CRITICAL = 4


@dataclass
class ProxyTask:
    task_id: str
    task_type: TaskType
    priority: TaskPriority
    client_id: str
    request_data: Dict[str, Any]
    timeout: int = 30
    max_retries: int = 3
    retry_count: int = 0
    created_at: float = None
    started_at: Optional[float] = None
    completed_at: Optional[float] = None
    assigned_node: Optional[str] = None
    status: TaskStatus = TaskStatus.QUEUED
    error_message: Optional[str] = None
    response_data: Optional[Dict[str, Any]] = None

    def __post_init__(self):
        if self.created_at is None:
            self.created_at = time.time()

    def is_expired(self) -> bool:
        """Check if task has exceeded its timeout"""
        if self.started_at is None:
            return False
        return time.time() - self.started_at > self.timeout

    def can_retry(self) -> bool:
        """Check if task can be retried"""
        return self.retry_count < self.max_retries and self.status in [
            TaskStatus.FAILED,
            TaskStatus.TIMEOUT,
        ]


@dataclass
class WorkerMetrics:
    tasks_processed: int = 0
    tasks_succeeded: int = 0
    tasks_failed: int = 0
    tasks_timeout: int = 0
    avg_processing_time: float = 0.0
    last_activity: float = 0.0
    worker_id: str = ""


class ProxyWorker:
    """Individual worker that processes proxy tasks"""

    def __init__(self, worker_id: str, worker_pool: "WorkerPoolService"):
        self.worker_id = worker_id
        self.pool = worker_pool
        self.metrics = WorkerMetrics(worker_id=worker_id)
        self.current_task: Optional[ProxyTask] = None
        self.running = False
        self._processing_times = []

    async def process_task(self, task: ProxyTask) -> bool:
        """Process a single proxy task"""
        try:
            self.current_task = task
            task.started_at = time.time()
            task.status = TaskStatus.PROCESSING
            self.metrics.last_activity = time.time()

            logger.info(
                f"Worker {self.worker_id} processing task {task.task_id} ({task.task_type.value})"
            )

            # Update task status in Redis
            await self._update_task_status(task)

            # Process based on task type
            success = False
            if task.task_type == TaskType.HTTP_REQUEST:
                success = await self._process_http_request(task)
            elif task.task_type == TaskType.HTTPS_TUNNEL:
                success = await self._process_https_tunnel(task)
            elif task.task_type == TaskType.SOCKS5_PROXY:
                success = await self._process_socks5_proxy(task)
            else:
                task.error_message = f"Unknown task type: {task.task_type.value}"
                success = False

            # Update task completion
            task.completed_at = time.time()
            processing_time = task.completed_at - task.started_at

            if success:
                task.status = TaskStatus.COMPLETED
                self.metrics.tasks_succeeded += 1
                logger.info(
                    f"Task {task.task_id} completed successfully in {processing_time:.2f}s"
                )
            else:
                task.status = TaskStatus.FAILED
                self.metrics.tasks_failed += 1
                logger.error(f"Task {task.task_id} failed: {task.error_message}")

            # Update metrics
            self.metrics.tasks_processed += 1
            self._processing_times.append(processing_time)
            if len(self._processing_times) > 100:  # Keep last 100 measurements
                self._processing_times.pop(0)
            self.metrics.avg_processing_time = sum(self._processing_times) / len(
                self._processing_times
            )

            # Update final task status
            await self._update_task_status(task)

            # Publish completion event
            await self._publish_task_event(
                "task.completed" if success else "task.failed", task
            )

            return success

        except Exception as e:
            logger.error(
                f"Worker {self.worker_id} error processing task {task.task_id}: {e}"
            )
            task.status = TaskStatus.FAILED
            task.error_message = str(e)
            task.completed_at = time.time()
            await self._update_task_status(task)
            await self._publish_task_event("task.failed", task)
            return False
        finally:
            self.current_task = None

    async def _process_http_request(self, task: ProxyTask) -> bool:
        """Process HTTP proxy request"""
        try:
            request_data = task.request_data
            method = request_data.get("method", "GET")
            url = request_data.get("url")
            headers = request_data.get("headers", {})
            body = request_data.get("body", "")

            if not url:
                task.error_message = "Missing URL in request data"
                return False

            # Get best available node for HTTP proxy
            node_result = await self._get_node_for_capability("http_proxy")
            if not node_result:
                task.error_message = "No available nodes for HTTP proxy"
                return False

            node_id, node_info = node_result
            task.assigned_node = node_id

            # Mark node as busy
            await self._update_node_status(node_id, "busy")

            try:
                # Send HTTP request to node via RabbitMQ
                command = {
                    "type": "http_request",
                    "task_id": task.task_id,
                    "request_id": str(uuid.uuid4()),
                    "method": method,
                    "url": url,
                    "headers": headers,
                    "body": body,
                    "timeout": task.timeout,
                }

                # Send command to node
                await self._send_node_command(node_id, command)

                # Wait for response with timeout
                response = await self._wait_for_response(task.task_id, task.timeout)

                if response:
                    task.response_data = response
                    return True
                else:
                    task.error_message = "Timeout waiting for node response"
                    self.metrics.tasks_timeout += 1
                    return False

            finally:
                # Mark node as free
                await self._update_node_status(node_id, "online")

        except Exception as e:
            task.error_message = f"HTTP request processing error: {str(e)}"
            return False

    async def _process_https_tunnel(self, task: ProxyTask) -> bool:
        """Process HTTPS tunnel request"""
        try:
            request_data = task.request_data
            target_host = request_data.get("target_host")
            target_port = request_data.get("target_port")
            client_socket_id = request_data.get("client_socket_id")

            if not all([target_host, target_port, client_socket_id]):
                task.error_message = "Missing required tunnel parameters"
                return False

            # Get best available node for HTTPS tunneling
            node_result = await self._get_node_for_capability("https_tunnel")
            if not node_result:
                task.error_message = "No available nodes for HTTPS tunnel"
                return False

            node_id, node_info = node_result
            task.assigned_node = node_id

            # Mark node as busy
            await self._update_node_status(node_id, "busy")

            try:
                # Create tunnel
                tunnel_id = str(uuid.uuid4())
                command = {
                    "type": "https_tunnel_create",
                    "task_id": task.task_id,
                    "tunnel_id": tunnel_id,
                    "target_host": target_host,
                    "target_port": int(target_port),
                    "client_socket_id": client_socket_id,
                }

                # Send tunnel creation command
                await self._send_node_command(node_id, command)

                # Wait for tunnel establishment
                response = await self._wait_for_response(
                    task.task_id, 10
                )  # 10s timeout for tunnel setup

                if response and response.get("status") == "tunnel_established":
                    task.response_data = {
                        "tunnel_id": tunnel_id,
                        "node_id": node_id,
                        "status": "established",
                    }
                    return True
                else:
                    task.error_message = "Failed to establish tunnel"
                    return False

            finally:
                # Note: For tunnels, we keep the node busy until tunnel closes
                pass

        except Exception as e:
            task.error_message = f"HTTPS tunnel processing error: {str(e)}"
            return False

    async def _process_socks5_proxy(self, task: ProxyTask) -> bool:
        """Process SOCKS5 proxy request"""
        try:
            # Similar pattern to HTTP/HTTPS but for SOCKS5
            request_data = task.request_data
            target_host = request_data.get("target_host")
            target_port = request_data.get("target_port")
            client_socket_id = request_data.get("client_socket_id")

            # Get node for SOCKS5
            node_result = await self._get_node_for_capability("socks5")
            if not node_result:
                task.error_message = "No available nodes for SOCKS5 proxy"
                return False

            node_id, node_info = node_result
            task.assigned_node = node_id

            # Implementation similar to HTTPS tunnel
            # ... (implementation details)

            return True

        except Exception as e:
            task.error_message = f"SOCKS5 proxy processing error: {str(e)}"
            return False

    async def _get_node_for_capability(self, capability: str) -> Optional[tuple]:
        """Request best node from Node Manager Service"""
        try:
            # Send request to Node Manager via RabbitMQ
            request = {
                "type": "get_best_node",
                "capability": capability,
                "request_id": str(uuid.uuid4()),
            }

            # Publish to node manager queue
            message = json.dumps(request)
            self.pool.channel.basic_publish(
                exchange="node_commands",
                routing_key="node_manager",
                body=message,
                properties=pika.BasicProperties(
                    reply_to="worker_responses", correlation_id=request["request_id"]
                ),
            )

            # Wait for response (simplified - in production use proper correlation)
            # For now, return a mock response
            return ("node_001", {"status": "online", "health_score": 95.0})

        except Exception as e:
            logger.error(f"Error getting node for capability {capability}: {e}")
            return None

    async def _send_node_command(self, node_id: str, command: Dict):
        """Send command to specific node"""
        try:
            message = json.dumps(command)
            self.pool.channel.basic_publish(
                exchange="node_commands",
                routing_key=f"node.{node_id}",
                body=message,
                properties=pika.BasicProperties(delivery_mode=2),
            )
        except Exception as e:
            logger.error(f"Error sending command to node {node_id}: {e}")

    async def _wait_for_response(self, task_id: str, timeout: int) -> Optional[Dict]:
        """Wait for task response from node"""
        try:
            # Check Redis for response (nodes publish responses there)
            response_key = f"task_response:{task_id}"

            for _ in range(timeout * 10):  # Check every 100ms
                response_data = await self.pool.redis.get(response_key)
                if response_data:
                    await self.pool.redis.delete(response_key)  # Cleanup
                    return json.loads(response_data)
                await asyncio.sleep(0.1)

            return None

        except Exception as e:
            logger.error(f"Error waiting for response for task {task_id}: {e}")
            return None

    async def _update_node_status(self, node_id: str, status: str):
        """Update node status via Node Manager"""
        try:
            command = {
                "type": "update_node_status",
                "node_id": node_id,
                "status": status,
                "timestamp": time.time(),
            }

            message = json.dumps(command)
            self.pool.channel.basic_publish(
                exchange="node_commands", routing_key="node_manager", body=message
            )
        except Exception as e:
            logger.error(f"Error updating node status: {e}")

    async def _update_task_status(self, task: ProxyTask):
        """Update task status in Redis"""
        try:
            task_data = {
                "task_id": task.task_id,
                "status": task.status.value,
                "assigned_node": task.assigned_node,
                "started_at": task.started_at,
                "completed_at": task.completed_at,
                "error_message": task.error_message,
                "retry_count": task.retry_count,
            }

            await self.pool.redis.hset(
                f"task:{task.task_id}",
                mapping={
                    k: json.dumps(v) if v is not None else ""
                    for k, v in task_data.items()
                },
            )

            # Set expiration (24 hours)
            await self.pool.redis.expire(f"task:{task.task_id}", 86400)

        except Exception as e:
            logger.error(f"Error updating task status: {e}")

    async def _publish_task_event(self, event_type: str, task: ProxyTask):
        """Publish task event to RabbitMQ"""
        try:
            event_data = {
                "task_id": task.task_id,
                "task_type": task.task_type.value,
                "status": task.status.value,
                "worker_id": self.worker_id,
                "assigned_node": task.assigned_node,
                "processing_time": (
                    (task.completed_at - task.started_at)
                    if task.completed_at and task.started_at
                    else None
                ),
                "timestamp": time.time(),
            }

            message = json.dumps(event_data)
            self.pool.channel.basic_publish(
                exchange="task_events",
                routing_key=event_type,
                body=message,
                properties=pika.BasicProperties(delivery_mode=2),
            )

        except Exception as e:
            logger.error(f"Error publishing task event: {e}")


class WorkerPoolService:
    """Main worker pool service that manages multiple workers"""

    def __init__(self, config: Dict):
        self.config = config
        self.workers: List[ProxyWorker] = []
        self.task_queue: asyncio.Queue = asyncio.Queue()
        self.active_tasks: Dict[str, ProxyTask] = {}
        self.failed_tasks: List[ProxyTask] = []

        # Connections
        self.db_pool: Optional[asyncpg.Pool] = None
        self.redis: Optional[aioredis.Redis] = None
        self.rabbitmq_connection: Optional[pika.BlockingConnection] = None
        self.channel: Optional[pika.channel.Channel] = None

        # Threading for RabbitMQ consumer
        self.consumer_thread: Optional[threading.Thread] = None
        self.running = False

        # Configuration
        self.max_workers = config.get("max_workers", 10)
        self.task_timeout_check_interval = config.get("task_timeout_check_interval", 30)
        self.retry_delay = config.get("retry_delay", 5)

    async def initialize(self):
        """Initialize all connections and workers"""
        logger.info("Initializing Worker Pool Service...")

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

            # Create worker database table
            await self._setup_database()

            # Initialize workers
            for i in range(self.max_workers):
                worker = ProxyWorker(f"worker_{i:03d}", self)
                self.workers.append(worker)

            logger.info(
                f"Worker Pool Service initialized with {self.max_workers} workers"
            )

        except Exception as e:
            logger.error(f"Failed to initialize Worker Pool Service: {e}")
            raise

    async def _setup_rabbitmq(self):
        """Setup RabbitMQ connections and queues"""
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
                ("task_queue", "direct"),
                ("task_events", "topic"),
                ("node_commands", "direct"),
                ("worker_events", "topic"),
            ]

            for exchange, exchange_type in exchanges:
                self.channel.exchange_declare(
                    exchange=exchange, exchange_type=exchange_type, durable=True
                )

            # Declare task queues with priorities
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

            # Response and retry queues
            other_queues = ["worker_responses", "failed_tasks", "retry_tasks"]

            for queue_name in other_queues:
                self.channel.queue_declare(queue=queue_name, durable=True)

            logger.info("RabbitMQ setup completed")

        except Exception as e:
            logger.error(f"RabbitMQ setup failed: {e}")
            raise

    async def _setup_database(self):
        """Setup database tables for task tracking"""
        try:
            async with self.db_pool.acquire() as conn:
                await conn.execute(
                    """
                    CREATE TABLE IF NOT EXISTS tasks (
                        task_id VARCHAR(255) PRIMARY KEY,
                        task_type VARCHAR(50) NOT NULL,
                        priority INTEGER NOT NULL,
                        client_id VARCHAR(255) NOT NULL,
                        request_data JSONB NOT NULL,
                        response_data JSONB,
                        status VARCHAR(50) NOT NULL,
                        assigned_node VARCHAR(255),
                        assigned_worker VARCHAR(255),
                        timeout INTEGER NOT NULL DEFAULT 30,
                        max_retries INTEGER NOT NULL DEFAULT 3,
                        retry_count INTEGER NOT NULL DEFAULT 0,
                        error_message TEXT,
                        created_at TIMESTAMP NOT NULL DEFAULT NOW(),
                        started_at TIMESTAMP,
                        completed_at TIMESTAMP,
                        processing_time_ms INTEGER
                    )
                """
                )

                # Create indexes
                await conn.execute(
                    "CREATE INDEX IF NOT EXISTS idx_tasks_status ON tasks(status)"
                )
                await conn.execute(
                    "CREATE INDEX IF NOT EXISTS idx_tasks_created_at ON tasks(created_at)"
                )
                await conn.execute(
                    "CREATE INDEX IF NOT EXISTS idx_tasks_client_id ON tasks(client_id)"
                )

                logger.info("Database tables created/verified")

        except Exception as e:
            logger.error(f"Database setup failed: {e}")
            raise

    def _start_rabbitmq_consumer(self):
        """Start RabbitMQ consumer in separate thread"""

        def consume():
            try:
                # Setup consumer callbacks
                queues_to_consume = [
                    "tasks.critical",
                    "tasks.high",
                    "tasks.normal",
                    "tasks.low",
                ]

                for queue in queues_to_consume:
                    self.channel.basic_consume(
                        queue=queue,
                        on_message_callback=self._handle_task_message,
                        auto_ack=False,
                    )

                self.channel.basic_qos(
                    prefetch_count=self.max_workers * 2
                )  # Prefetch control

                logger.info("Starting RabbitMQ consumer...")
                self.channel.start_consuming()

            except Exception as e:
                logger.error(f"RabbitMQ consumer error: {e}")

        self.consumer_thread = threading.Thread(target=consume, daemon=True)
        self.consumer_thread.start()

    def _handle_task_message(self, channel, method, properties, body):
        """Handle incoming task message from RabbitMQ"""
        try:
            task_data = json.loads(body)

            # Create ProxyTask object
            task = ProxyTask(
                task_id=task_data.get("task_id", str(uuid.uuid4())),
                task_type=TaskType(task_data["task_type"]),
                priority=TaskPriority(task_data.get("priority", 2)),
                client_id=task_data["client_id"],
                request_data=task_data["request_data"],
                timeout=task_data.get("timeout", 30),
                max_retries=task_data.get("max_retries", 3),
            )

            # Add to async queue
            asyncio.run_coroutine_threadsafe(
                self.task_queue.put(task), asyncio.get_event_loop()
            )

            # Acknowledge message
            channel.basic_ack(delivery_tag=method.delivery_tag)

            logger.debug(f"Queued task {task.task_id} from RabbitMQ")

        except Exception as e:
            logger.error(f"Error handling task message: {e}")
            # Reject and requeue message
            channel.basic_nack(delivery_tag=method.delivery_tag, requeue=True)

    async def _worker_loop(self):
        """Main worker loop that distributes tasks to workers"""
        available_workers = list(self.workers)

        while self.running:
            try:
                if not available_workers:
                    await asyncio.sleep(0.1)
                    continue

                # Get next task from queue
                try:
                    task = await asyncio.wait_for(self.task_queue.get(), timeout=1.0)
                except asyncio.TimeoutError:
                    continue

                # Get available worker
                worker = available_workers.pop(0)

                # Track active task
                self.active_tasks[task.task_id] = task

                # Process task in background
                async def process_and_cleanup():
                    try:
                        await worker.process_task(task)
                    finally:
                        # Cleanup and return worker to pool
                        self.active_tasks.pop(task.task_id, None)
                        available_workers.append(worker)

                asyncio.create_task(process_and_cleanup())

            except Exception as e:
                logger.error(f"Worker loop error: {e}")
                await asyncio.sleep(1)

    async def _timeout_checker_loop(self):
        """Background task to check for timed out tasks"""
        while self.running:
            try:
                current_time = time.time()
                timed_out_tasks = []

                for task_id, task in self.active_tasks.items():
                    if task.is_expired():
                        timed_out_tasks.append(task)

                for task in timed_out_tasks:
                    logger.warning(f"Task {task.task_id} timed out")
                    task.status = TaskStatus.TIMEOUT

                    # Try to retry if possible
                    if task.can_retry():
                        task.retry_count += 1
                        task.status = TaskStatus.RETRYING
                        await self.task_queue.put(task)
                        logger.info(
                            f"Retrying task {task.task_id} (attempt {task.retry_count + 1})"
                        )
                    else:
                        task.status = TaskStatus.FAILED
                        task.error_message = "Task timeout exceeded"
                        self.failed_tasks.append(task)

                await asyncio.sleep(self.task_timeout_check_interval)

            except Exception as e:
                logger.error(f"Timeout checker error: {e}")
                await asyncio.sleep(5)

    async def get_pool_stats(self) -> Dict:
        """Get worker pool statistics"""
        total_processed = sum(w.metrics.tasks_processed for w in self.workers)
        total_succeeded = sum(w.metrics.tasks_succeeded for w in self.workers)
        total_failed = sum(w.metrics.tasks_failed for w in self.workers)
        total_timeout = sum(w.metrics.tasks_timeout for w in self.workers)

        busy_workers = sum(1 for w in self.workers if w.current_task is not None)
        avg_processing_time = sum(
            w.metrics.avg_processing_time for w in self.workers
        ) / len(self.workers)

        return {
            "total_workers": len(self.workers),
            "busy_workers": busy_workers,
            "available_workers": len(self.workers) - busy_workers,
            "queued_tasks": self.task_queue.qsize(),
            "active_tasks": len(self.active_tasks),
            "failed_tasks": len(self.failed_tasks),
            "total_processed": total_processed,
            "total_succeeded": total_succeeded,
            "total_failed": total_failed,
            "total_timeout": total_timeout,
            "success_rate": (total_succeeded / max(total_processed, 1)) * 100,
            "avg_processing_time": avg_processing_time,
        }

    async def start(self):
        """Start the Worker Pool Service"""
        self.running = True
        logger.info("Starting Worker Pool Service...")

        # Start RabbitMQ consumer
        self._start_rabbitmq_consumer()

        # Start background tasks
        tasks = [
            asyncio.create_task(self._worker_loop()),
            asyncio.create_task(self._timeout_checker_loop()),
        ]

        try:
            await asyncio.gather(*tasks)
        except KeyboardInterrupt:
            logger.info("Shutting down Worker Pool Service...")
        finally:
            await self.shutdown()

    async def shutdown(self):
        """Gracefully shutdown the service"""
        self.running = False

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

        logger.info("Worker Pool Service shutdown completed")


# Example configuration
CONFIG = {
    "postgres": {
        "host": "localhost",
        "port": 5432,
        "user": "proxy_user",
        "password": "secure_password",
        "database": "proxy_system",
    },
    "redis": {"host": "localhost", "port": 6379},
    "rabbitmq": {
        "host": "localhost",
        "port": 5672,
        "user": "proxy_user",
        "password": "secure_password",
        "vhost": "proxy_system",
    },
    "max_workers": 20,
    "task_timeout_check_interval": 30,
    "retry_delay": 5,
}


async def main():
    """Main entry point"""
    service = WorkerPoolService(CONFIG)

    try:
        await service.initialize()
        await service.start()
    except Exception as e:
        logger.error(f"Service failed: {e}")
        await service.shutdown()


if __name__ == "__main__":
    asyncio.run(main())
