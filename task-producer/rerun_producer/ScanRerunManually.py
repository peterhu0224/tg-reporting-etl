import datetime
import pandas as pd

from utils.PipelineUtils import ProcessStep


class ScanRerunManually(ProcessStep):

    @classmethod
    def process(cls, data, utils):
        """

        :param data:
        :param utils:
        :return:
        """
        db_utils = utils['db_utils']

        sql = "SELECT *" \
              " FROM rerun_board_manually_insert WHERE done = 0"

        with db_utils.get_task_db_maria_conn() as conn:
            manually_data = pd.read_sql(sql, conn)

        return manually_data
