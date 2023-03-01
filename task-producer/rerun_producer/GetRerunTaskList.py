import pandas as pd
from utils.PipelineUtils import ProcessStep


class GetRerunTaskList(ProcessStep):
    @staticmethod
    def process(data, utils):
        """
        使用task_utils.get_task_list_by_gte_lt(data)來建構task_list
        :param data: 接收已完成分裂的DataFrame
        :param utils: main impl tool
        :return: 完整可發布的task_list
        """
        task_utils = utils['task_utils']
        rerun_task_list = task_utils.get_task_list_by_gte_lt(data)

        return rerun_task_list
