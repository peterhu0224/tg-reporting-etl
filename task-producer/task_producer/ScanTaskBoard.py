import pandas as pd

from utils.PipelineUtils import ProcessStep


class ScanTaskBoard(ProcessStep):
    def process(self, data, utils):
        """
        掃描task_log，找到每一類task最後一次執行的的時間，後續會將任務安排到now()，把沒做完的任務全部補上

        :param data: None scan log的第一步，此處還不需要處理任何data
        :param utils: main impl的工具包
        :return:
        """
        db_utils = utils['db_utils']
        table_name = 'task_board'

        sql = f"SELECT create_time, level, platform, site_code, game_code, report_class" \
              f", assignee, freq_type, gte_time, max(lt_time) as 'lt_time', apply_time, complete_time" \
              f", runtime_second, retry, done" \
              f" FROM {table_name} GROUP BY platform, site_code, game_code, assignee"

        try:
            with db_utils.get_task_db_maria_conn() as conn:
                old_task_log = pd.read_sql(sql, conn)
        except Exception as e:
            raise e

        return old_task_log
