import time

import pandas as pd
from utils.PipelineUtils import ProcessStep


class ProduceRerunTask(ProcessStep):
    @classmethod
    def process(cls, data, utils):
        """
        發布rerun任務，rerun_board_manually_insert

        :param data:來自GetRerunTaskList()已整理好，可發布的task
        :param utils:main impl tool
        :return:None
        """

        if len(data.index) == 0:
            return

        db_utils = utils['db_utils']

        with db_utils.get_task_db_maria_conn() as conn:
            target_table = 'rerun_board'
            data.to_sql(target_table, conn, if_exists='append', index=False)



