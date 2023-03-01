from retry.api import retry_call
import pandas as pd

from report_config import trans_summary_5min_config
from utils.PipelineUtils import ProcessStep

class trans_summary_5min(ProcessStep):
    @classmethod
    def process(cls, data, utils):

        db_utils = utils['db_utils']
        exec_utils = utils['exec_utils']
        assignee = 'trans_summary_5min'

        # 取5min task_list裡屬於trans_summary_5min自己該做的任務
        sub_data = data[data['assignee'] == assignee]

        task_conn = db_utils.get_task_db_maria_conn()
        report_conn = getattr(db_utils, trans_summary_5min_config['target_conn_report'])()
        gs_comm = getattr(db_utils, trans_summary_5min_config['source_conn_gs'])()

        # 開始逐筆完成任務
        for i, row in sub_data.iterrows():
            # 更新task接受時間
            exec_utils.update_task_apply_time(row, task_conn)
            # aggregation
            retry_call(cls.aggregation, fargs=[row, report_conn, gs_comm, utils], tries=5, delay=2, logger=utils['logger'])
            # 更新task完成時間
            exec_utils.update_task_exec_time(row, task_conn)

        task_conn.close()
        report_conn.close()
        gs_comm.close()

        # 把data交回，讓下一段繼續做
        return data

    @classmethod
    def aggregation(cls, single_task, r_conn, gs_conn, utils):

        target_table = 'trans_summary_5min'

        # aggregation交由DB處理
        report = cls.get_trans_report_5min(single_task, gs_conn, utils)
        if len(report.index) == 0:
            return
        # 先刪後寫支援重跑
        cls.delete_before_insert(single_task, r_conn)
        report.to_sql(target_table, r_conn, if_exists='append', index=False)

    @staticmethod
    def get_trans_report_5min(single_task, tg_conn, utils):

        source_table = 'player_value_log'

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
                          "platform",
                          "site_code",
                          "player_name",
                          "country",
                          SUM(IF(trade_type='IN', value, 0)) AS trans_in_amount,
                          SUM(IF(trade_type='OUT', value, 0)) AS trans_out_amount,
                          SUM(IF(trade_type='IN', 1, 0)) AS trans_in_count,
                          SUM(IF(trade_type='OUT', 1, 0)) AS trans_out_count,
                          (SUM(IF(trade_type='OUT', before_value, 0))
                          -SUM(IF(trade_type='OUT', after_value, 0))
                          -SUM(IF(trade_type='OUT', value, 0))) AS trans_out_lost_amount
                     FROM
                      "{source_table}"
                    WHERE trade_date = {int(single_task['gte_time'].strftime("%Y%m%d"))}
                      AND trade_status = 'SUCCESS'
                      AND trade_time >= '{single_task['gte_time']}'
                      AND trade_time < '{single_task['lt_time']}'
                      {platform_filter}
                      {site_code_filter}
                    GROUP BY platform, site_code, player_name, country
                """

        # 取得DB資料
        report_5min = retry_call(pd.read_sql, fargs=[sql, tg_conn], tries=10, delay=2,
                                 logger=utils['logger'])

        # 組織時間格式
        report_5min['summary_date'] = int(single_task['gte_time'].strftime("%Y%m%d"))
        report_5min['hours'] = int(single_task['gte_time'].strftime("%H"))
        report_5min['mins'] = int(single_task['gte_time'].strftime("%M"))
        report_5min['start_time'] = single_task['gte_time']

        return report_5min

    @staticmethod
    def delete_before_insert(single_task, conn):

        target_table = 'trans_summary_5min'

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
                          AND hours = '{int(single_task['gte_time'].strftime("%H"))}'
                          AND mins = '{int(single_task['gte_time'].strftime("%M"))}'
                          {platform_filter}
                          {site_code_filter}
                    """

        conn.execute(delete_sql)
