import simplejson as json
import logging

from task_one.task_one_exec import TaskOne
from task_two.task_two_exec import TaskTwo


log = logging.getLogger(__name__)


class TaskManager(object):

    def __init__(self, producer):
        log.info("Start initialization")

        atomic_processors = {
		'Type_One': TaskOne,
		'Type_Two': TaskTwo,

        }
        self.processors = {**atomic_processors}
        self.producer = producer
        log.info("Finish initialization")

    @staticmethod
    def default_behavior(task: dict, processors, producer) -> None:
        result = processors[task['type']].run(task['body'])

        producer.publish(json.dumps(result))

    def execute(self, task):
        if (type(task) is bytes):
            task = task.decode("utf-8")
        try:
            if type(task) == str:
                task = json.loads(task)
            self.default_behavior(task, self.processors, self.producer)
        except Exception:
            log.info("CANNOT PARSE TASK {0}".format(task))
        log.info("TASK")
        log.info(task)

