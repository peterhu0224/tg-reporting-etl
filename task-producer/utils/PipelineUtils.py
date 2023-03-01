import traceback
from retry.api import retry_call
from abc import ABC
from abc import abstractmethod

from monitor.ErrorHandler import ErrorHandler


class ProcessPipeline:
    def __init__(self, steps, ):
        self.steps = steps

    def run(self, utils):
        data = None
        for step in self.steps:
            try:
                utils['logger'].info(f"{type(step).__name__} start.")
                print(f"{type(step).__name__} start.")

                data = retry_call(step.process, fargs=[data, utils], tries=3, delay=5, logger=utils['logger'])

                utils['logger'].info(f"{type(step).__name__} finished.")
                print(f"{type(step).__name__} finished.")
            except Exception as e:
                error = {
                    'pipeline': f'{type(self).__name__}',
                    'process': f'{type(step).__name__}',
                    'content': f'{repr(e)}',
                    'traceback': f'{traceback.format_exc()}'
                }
                ErrorHandler.raise_error_to_db(error)
                ErrorHandler.send_alert_msg(error)
                utils['logger'].error(f"{type(step).__name__} error")
                print(traceback.format_exc())
                break


class ProcessStep(ABC):
    def __init__(self):
        pass

    @classmethod
    @abstractmethod
    def process(cls, data, utils):
        pass


class StepException(Exception):
    pass
