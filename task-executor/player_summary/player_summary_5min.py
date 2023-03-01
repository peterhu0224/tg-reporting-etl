from retry.api import retry_call
import pandas as pd
import numpy as np

from report_config import player_summary_5min_config
from report_config import player_summary_rtp_config
from utils.PipelineUtils import ProcessStep


class player_summary_5min(ProcessStep):
    @classmethod
    def process(cls, data, utils):

        db_utils = utils['db_utils']
        exec_utils = utils['exec_utils']
        assignee = 'player_summary_5min'

        # 取5min task_list裡屬於player_summary_5min自己該做的任務
        sub_data = data[data['assignee'] == assignee]

        task_conn = db_utils.get_task_db_maria_conn()
        report_conn = getattr(db_utils, player_summary_5min_config['target_conn_report'])()
        tg_admin_comm = getattr(db_utils, player_summary_5min_config['source_conn_tg_admin'])()

        # 開始逐筆完成任務
        for i, row in sub_data.iterrows():
            exec_utils.update_task_apply_time(row, task_conn)

            # cls.aggregation(row, report_conn, utils)
            retry_call(cls.aggregation, fargs=[row, report_conn, tg_admin_comm, utils], tries=5, delay=2, logger=utils['logger'])

            exec_utils.update_task_exec_time(row, task_conn)

        task_conn.close()
        report_conn.close()
        tg_admin_comm.close()

        # 把data交回，讓下一段繼續做
        return data

    @classmethod
    def aggregation(cls, single_task, r_conn, tg_conn, utils):
        target_table = 'player_summary_5min'

        # aggregation交由DB處理
        report = cls.get_profit_report_5min(single_task, r_conn, tg_conn, utils)
        if len(report.index) == 0:
            return

        # risky check
        report = cls.check_if_risky(report)
        # 先刪後寫支援重跑
        cls.delete_before_insert(single_task, r_conn)
        report.to_sql(target_table, r_conn, if_exists='append', index=False)

    @staticmethod
    def get_profit_report_5min(single_task, r_conn, tg_conn, utils):
        source_table = 'player_profit_log'

        # 預設task發布都是跑ALL，非ALL才跑指定的部分
        platform_filter = ""
        site_code_filter = ""
        game_code_filter = ""
        if single_task['platform'] != 'ALL':
            platform_filter = f"AND platform = '{single_task['platform']}'"
        if single_task['site_code'] != 'ALL':
            site_code_filter = f"AND site_code = '{single_task['site_code']}'"
        if single_task['game_code'] != 'ALL':
            game_code_filter = f"AND game_code = '{single_task['game_code']}'"

        sql = f"""SELECT
                  `platform`,
                  `site_code`,
                  `game_code`,
                  `player_name`,
                  `country`,
                  COUNT(`bet`) as b_count,
                  SUM(`bet`) as b_amount,
                  SUM(`win`) as w_amount,
                  SUM(`fee`) as fee_amount,
                  SUM(`profit`) as profit_amount,
                  SUM(`refund`) as refund_amount,
                  SUM(`normal_value`) as normal_amount,
                  SUM(`bonus_value`) as bonus_amount,
                  SUM(IF(free_value>=0, free_value, 0)) as free_amount,
                  SUM(IF(jp_value>=0, jp_value, 0)) as jp_amount,
                  SUM(`valid_value`) as valid_amount,
                  SUM(`cancel_value`) as cancel_amount,
                  SUM(`profit`) / SUM(`bet`) as rtp
                  FROM  `{source_table}`
                  WHERE 1=1
                    AND round_time >= '{single_task['gte_time']}'
                    AND round_time < '{single_task['lt_time']}'
                    AND is_robot = 0
                    {platform_filter}
                    {site_code_filter}
                    {game_code_filter}
                  GROUP BY `platform`, `site_code`, `game_code`, `player_name`, `country`
               """

        # 取得DB資料
        report_5min = retry_call(pd.read_sql, fargs=[sql, r_conn], tries=10, delay=2,
                                 logger=utils['logger'])

        # rtp或wl可能發生除以0的問題
        report_5min = report_5min.replace([np.inf, -np.inf], np.nan).fillna(0)

        # 取得tg_admin的ratio資料
        source_table_gs = 'game_sites'
        game_site_sql = f"SELECT * FROM {source_table_gs}"
        game_site_list = retry_call(pd.read_sql, fargs=[game_site_sql, tg_conn], tries=10, delay=2,
                                    logger=utils['logger'])
        game_site_list = game_site_list[['platform', 'code', 'ratio']]

        # 組合ratio (從gs_admin.game_sites來的)
        result = report_5min.merge(game_site_list, how='left', left_on=['platform', 'site_code'],
                                   right_on=['platform', 'code'], suffixes=('', '_y'))
        result.drop(columns=['code'], inplace=True)
        result['ratio'].fillna(0, inplace=True)

        result['p_before_amount'] = result['profit_amount']
        result['p_after_amount'] = result['profit_amount'] * (1 - result['ratio'])
        result['tg_after_amount'] = result['profit_amount'] * result['ratio']

        # 組織時間格式
        result['summary_date'] = int(single_task['gte_time'].strftime("%Y%m%d"))
        result['hours'] = int(single_task['gte_time'].strftime("%H"))
        result['mins'] = int(single_task['gte_time'].strftime("%M"))
        result['start_time'] = single_task['gte_time']

        return result

    @staticmethod
    def check_if_risky(final_report):
        # is_risky 初始值為0
        final_report['is_risky'] = 0
        # 基本篩選條件
        final_report.loc[(final_report['profit_amount'] >= player_summary_rtp_config['profit_threshold'])
               & (final_report['b_count'] >= player_summary_rtp_config['bet_count_threshold'])
               & (final_report['rtp'] >= player_summary_rtp_config['rtp_threshold']), 'is_risky'] = 1
        # 贏太多
        final_report.loc[(final_report['profit_amount'] >= player_summary_rtp_config['profit_unconditional']), 'is_risky'] = 1

        return final_report

    @staticmethod
    def delete_before_insert(single_task, conn):
        target_table = 'player_summary_5min'
        # 預設task發布都是跑ALL，非ALL才跑指定的部分
        platform_filter = ""
        site_code_filter = ""
        game_code_filter = ""
        if single_task['platform'] != 'ALL':
            platform_filter = f"AND platform = '{single_task['platform']}'"
        if single_task['site_code'] != 'ALL':
            site_code_filter = f"AND site_code = '{single_task['site_code']}'"
        if single_task['game_code'] != 'ALL':
            game_code_filter = f"AND game_code = '{single_task['game_code']}'"

        delete_sql = f"""
                        DELETE FROM {target_table}
                        WHERE summary_date = '{int(single_task['gte_time'].strftime("%Y%m%d"))}'
                          AND hours = '{int(single_task['gte_time'].strftime("%H"))}'
                          AND mins = '{int(single_task['gte_time'].strftime("%M"))}'
                          {platform_filter}
                          {site_code_filter}
                          {game_code_filter}
                    """

        conn.execute(delete_sql)
