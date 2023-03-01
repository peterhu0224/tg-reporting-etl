import pandas as pd
from datetime import datetime

from retry.api import retry_call

from utils.PipelineUtils import ProcessStep
from report_config import trans_summary_1d_config


class trans_summary_1d(ProcessStep):
    @classmethod
    def process(cls, data, utils):

        db_utils = utils['db_utils']
        exec_utils = utils['exec_utils']

        # 取1h task_list裡屬於trans_summary_1d自己該做的任務
        assignee = 'trans_summary_1d'
        sub_data = data[data['assignee'] == assignee]

        task_conn = db_utils.get_task_db_maria_conn()
        report_conn = getattr(db_utils, trans_summary_1d_config['target_conn_report'])()

        # 開始逐筆完成任務
        for i, row in sub_data.iterrows():
            exec_utils.update_task_apply_time(row, task_conn)

            # cls.aggregation(row, report_conn)
            retry_call(cls.aggregation, fargs=[row, report_conn], tries=5, delay=2, logger=utils['logger'])

            # 跨日後才更新done=1，若當前done=0就會一直重跑(實現update效果)
            if datetime.now() > row['lt_time']:
                exec_utils.update_task_exec_time(row, task_conn)

        task_conn.close()
        report_conn.close()

        # 把data交回，讓下一段繼續做
        return data

    @classmethod
    def aggregation(cls, single_task, r_conn):
        target_table = 'trans_summary_1d'

        # aggregation交由DB處理
        report = cls.get_trans_report_1d(single_task, r_conn)
        if len(report.index) == 0:
            return

        # 先刪後寫，支援重跑
        cls.delete_before_insert(single_task, r_conn)
        report.to_sql(target_table, r_conn, if_exists='append', index=False)

    @staticmethod
    def get_trans_report_1d(single_task, r_conn):
        source_table = 'trans_summary_1h'

        # 預設task發布都是跑ALL，非ALL才跑指定的部分
        platform_filter = ""
        site_code_filter = ""
        # game_code_filter = ""
        if single_task['platform'] != 'ALL':
            platform_filter = f"AND platform = '{single_task['platform']}'"
        if single_task['site_code'] != 'ALL':
            site_code_filter = f"AND site_code = '{single_task['site_code']}'"
        # if single_task['game_code'] != 'ALL':
        #     game_code_filter = f"AND game_code = '{single_task['game_code']}'"

        sql = f"""
                    SELECT
                      platform,
                      site_code,
                      player_name,
                      country,
                      SUM(trans_in_amount) AS trans_in_amount,
                      SUM(trans_out_amount) AS trans_out_amount,
                      SUM(trans_in_count) AS trans_in_count,
                      SUM(trans_out_count) AS trans_out_count,
                      SUM(trans_out_lost_amount) AS trans_out_lost_amount 
                    FROM
                      {source_table}
                    WHERE summary_date = '{int(single_task['gte_time'].strftime("%Y%m%d"))}'
                      {platform_filter}
                      {site_code_filter}
                    GROUP BY platform, site_code, player_name, country
               """

        report_1d = pd.read_sql(sql, r_conn)

        # 組織時間格式
        report_1d['summary_date'] = int(single_task['gte_time'].strftime("%Y%m%d"))

        return report_1d

    @staticmethod
    def delete_before_insert(single_task, conn):
        target_table = 'trans_summary_1d'

        # 預設task發布都是跑ALL，非ALL才跑指定的部分
        platform_filter = ""
        site_code_filter = ""
        # game_code_filter = ""
        if single_task['platform'] != 'ALL':
            platform_filter = f"AND platform = '{single_task['platform']}'"
        if single_task['site_code'] != 'ALL':
            site_code_filter = f"AND site_code = '{single_task['site_code']}'"
        # if single_task['game_code'] != 'ALL':
        #     game_code_filter = f"AND game_code = '{single_task['game_code']}'"

        delete_sql = f"""
                        DELETE FROM {target_table}
                        WHERE summary_date = '{int(single_task['gte_time'].strftime("%Y%m%d"))}'
                          {platform_filter}
                          {site_code_filter}
                    """

        conn.execute(delete_sql)
