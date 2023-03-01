import traceback
from abc import ABC
from abc import abstractmethod
from retry.api import retry_call

from monitor.ErrorHandler import ErrorHandler


class ProcessPipeline:
    def __init__(self, steps, logger):
        self.steps = steps
        self.logger = logger

    def run(self, utils):
        data = None
        for step in self.steps:
            try:
                self.logger.info(f"{type(step).__name__} start.")
                print(f"{type(step).__name__} start.")

                # 執行主要任務，並且將Data(TaskList)持續向後傳遞
                data = step.process(data, utils)

                self.logger.info(f"{type(step).__name__} finished.")
                print(f"{type(step).__name__} finished.")
            except Exception as e:
                error = {
                    'pipeline': f'{type(self).__name__}',
                    'process': f'{type(step).__name__}',
                    'content': f'{repr(e)}',
                    'traceback': f'{traceback.format_exc()}'
                }
                ErrorHandler.send_alert_msg(error)
                self.logger.error(f"{type(step).__name__} error")
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
