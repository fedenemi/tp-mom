import pika
from pika.exceptions import AMQPConnectionError
import os
from .middleware import MessageMiddlewareDisconnectedError, MessageMiddlewareQueue, MessageMiddlewareExchange, MessageMiddlewareCloseError, MessageMiddlewareMessageError

# para poder identificarlo después aunque se hayan creado procesos hijos
# Los consumidores son procesos hijos (Evitamos condicion de carrera en que los hijos purguen antes de que el padre mande los mensajes)
_ROOT_PROCESS_ENV = "TP_MOM_ROOT_PID"
if _ROOT_PROCESS_ENV not in os.environ:
    os.environ[_ROOT_PROCESS_ENV] = str(os.getpid())
def _is_root_process():
    return os.environ.get(_ROOT_PROCESS_ENV) == str(os.getpid())


class MessageMiddlewareQueueRabbitMQ(MessageMiddlewareQueue):

    def __init__(self, host, queue_name):
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host))
        self.channel = self.connection.channel()
        self.channel.queue_declare(queue=queue_name)

        # para limpiar mensajes de corridas anteriores
        # Solo el proceso raíz purga, para no afectar a los workers hijos que ya están consumiendo
        if _is_root_process():
            self.channel.queue_purge(queue=queue_name)

        self.queue_name = queue_name
    
    def send(self, message):
        try:
            self.channel.basic_publish(exchange='', routing_key=self.queue_name, body=message)
        except AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError("Failed to connect to RabbitMQ server") from e
        
    def close(self):
        # cierro el canal explícitamente antes de cerrar la conexión, en orden inverso al que fueron abiertos.        
        try:
            if self.channel and self.channel.is_open:
                self.channel.close()
            self.connection.close()
        except Exception as e:
            raise MessageMiddlewareCloseError("Failed to close RabbitMQ connection") from e

    def _adapt_callback(self, on_message_callback):
        def _callback(channel, method, properties, body):
            def ack():
                channel.basic_ack(delivery_tag=method.delivery_tag)

            def nack():
                channel.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

            try:
                on_message_callback(body, ack, nack)
            except Exception as e:
                raise MessageMiddlewareMessageError("Error processing message") from e

        return _callback
    
    def start_consuming(self, on_message_callback):
        try:
            self.channel.basic_consume(
                queue=self.queue_name,
                on_message_callback=self._adapt_callback(on_message_callback),
                auto_ack=False,
            )
            self.channel.start_consuming()
        except AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError("Failed to connect to RabbitMQ server") from e
        except Exception as e:
            raise MessageMiddlewareMessageError("Failed while consuming from RabbitMQ queue") from e
    
    def stop_consuming(self):
        self.channel.stop_consuming()
            
class MessageMiddlewareExchangeRabbitMQ(MessageMiddlewareExchange):
    
    def __init__(self, host, exchange_name, routing_keys):
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host))
        self.channel = self.connection.channel()
        self.channel.exchange_declare(exchange=exchange_name, exchange_type='direct')
        self.exchange_name = exchange_name
        self.routing_keys = routing_keys

    def send(self, message):
            try:
                for routing_key in self.routing_keys:
                    self.channel.basic_publish(exchange=self.exchange_name, routing_key=routing_key, body=message)
            except AMQPConnectionError as e:
                raise MessageMiddlewareDisconnectedError("Failed to connect to RabbitMQ server") from e
            except Exception as e:
                raise MessageMiddlewareMessageError("Failed while publishing to RabbitMQ exchange") from e    
    
    def close(self):
    # cierro el canal explícitamente antes de cerrar la conexión, en orden inverso al que fueron abiertos.        
        try:
            if self.channel and self.channel.is_open:
                self.channel.close()
            self.connection.close()
        except Exception as e:
            raise MessageMiddlewareCloseError("Failed to close RabbitMQ connection") from e

    def _adapt_callback(self, on_message_callback):
        def _callback(channel, method, properties, body):
            def ack():
                channel.basic_ack(delivery_tag=method.delivery_tag)

            def nack():
                channel.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

            try:
                on_message_callback(body, ack, nack)
            except Exception as e:
                raise MessageMiddlewareMessageError("Error processing message") from e

        return _callback
    
    def start_consuming(self, on_message_callback):
        try:
            result = self.channel.queue_declare(queue='', exclusive=True)
            queue_name = result.method.queue

            for routing_key in self.routing_keys:
                self.channel.queue_bind(exchange=self.exchange_name, queue=queue_name, routing_key=routing_key)

            self.channel.basic_consume(
                queue=queue_name,
                on_message_callback=self._adapt_callback(on_message_callback),
                auto_ack=False,
            )
            self.channel.start_consuming()
        except AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError("Failed to connect to RabbitMQ server") from e
        except Exception as e:
            raise MessageMiddlewareMessageError("Failed while consuming from RabbitMQ exchange") from e
    
    def stop_consuming(self):
        self.channel.stop_consuming()