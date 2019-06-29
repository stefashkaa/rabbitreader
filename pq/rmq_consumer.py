from .base_consumer import BaseConsumer
import pika
import logging
import time
import threading

log = logging.getLogger(__name__)


class RMQConsumer(BaseConsumer):

    def heartbeat(self):
        while True:
            time.sleep(40)
            log.info("Rabbit heartbeat process_data_events")
            self.connection.process_data_events()

    def __init__(self, config, task_manager):
        log.info("Start initialization")
        log.debug("Init config %s", config)
        self.host = config['host']
        self.queue = config['queue']
        self.login = config['login']
        self.password = config['password']
        self.task_manager = task_manager
        # init rmq channel
        credentials = pika.PlainCredentials(self.login, self.password)
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=self.host, credentials=credentials))
        self.channel = self.connection.channel()
        self.channel.queue_declare(queue=self.queue, durable=True)
        self.channel.basic_qos(prefetch_count=1)
        self.channel.basic_consume(self.callback, queue=self.queue)
        threading.Thread(target=self.heartbeat).start()
        log.info("Finish initialization")

    def callback(self, ch, method, properties, body):
        task = body  # may be create validation?
        log.info("Start process task %s", task)
        timestamp = int(time.time())
        self.task_manager.execute(task)

        ch.basic_ack(delivery_tag=method.delivery_tag)
        log.info("Finish process task %d s", int(time.time()) - timestamp)  # create send statistic

    def start_consuming(self):
        log.info("Start consuming")
        self.channel.start_consuming()

    def stop_consuming(self):
        self.channel.stop_consuming()
        self.connection.close()
