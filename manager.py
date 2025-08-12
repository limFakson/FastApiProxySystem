import pika
import json
import time


def send_message_to_node(node_id, message):
    data = json.dumps({"node_id": node_id, "message": message})
    max_retries = 5
    retries = 0

    while retries < max_retries:
        try:
            # RabbitMQ connection
            parameters = pika.ConnectionParameters(
                host="localhost",
                port=5672,
                virtual_host="/",
                credentials=pika.PlainCredentials("guest", "guest"),
            )

            # Establish connection
            connection = pika.BlockingConnection(parameters)
            channel = connection.channel()
            channel.queue_declare(queue="websocket_queue")

            channel.basic_publish(exchange="", routing_key="websocket_queue", body=data)
            connection.close()
            break

        except Exception as e:
            print(f"Error in communication with pika attempt-{retries}: {e}")
            retries += 1
            time.sleep(5)
