from task_manager.task_manager import TaskManager
from pq import RMQConsumer, RMQProducer
import logging
import sys


def make_config(host, queue, login, password):
    config = dict()
    config['host'] = host
    config['queue'] = queue
    config['login'] = login
    config['password'] = password
    return config


# LOGGING SETTINGS
logging.basicConfig(stream=sys.stdout, level=logging.INFO)
ch = logging.StreamHandler(sys.stdout)
ch.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
ch.setFormatter(formatter)
log = logging.getLogger()
log.handlers.clear()
log.addHandler(ch)
log.info("Start application")

# RABBIT MQ PRODUCER-CONSUMER SETTINGS
producer_config = make_config("10.90.138.229", "queue2", "guest", "guest")
consumer_config = make_config("10.90.138.176", "queue1", "guest", "guest")
producer = RMQProducer(producer_config)
task_manager = TaskManager(producer)
consumer = RMQConsumer(consumer_config, task_manager)
consumer.start_consuming()
