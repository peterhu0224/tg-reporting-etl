import numpy as np
import pandas as pd
from datetime import datetime
from retry.api import retry_call

from utils.PipelineUtils import ProcessStep
from report_config import player_summary_1h_config
from report_config import player_summary_rtp_config


class player_summary_1h(ProcessStep):
    @classmethod
    def process(cls, data, utils):

        db_utils = utils['db_utils']
        exec_utils = utils['exec_utils']

        # 取1h task_list裡屬於player_summary_1h自己該做的任務
        assignee = 'player_summary_1h'
        sub_data = data[data['assignee'] == assignee]

        task_conn = db_utils.get_task_db_maria_conn()
        report_conn = getattr(db_utils, player_summary_1h_config['target_conn'])()

        # 開始逐筆完成任務
        for i, row in sub_data.iterrows():
            exec_utils.update_task_apply_time(row, task_conn)
            # aggregation
            retry_call(cls.aggregation, fargs=[row, report_conn, utils], tries=5, delay=2, logger=utils['logger'])
            # 跨小時後才更新done=1，若當前done=0就會一直重跑(實現update效果)
            if datetime.now() > row['lt_time']:
                exec_utils.update_task_exec_time(row, task_conn)

        task_conn.close()
        report_conn.close()

        # 把data交回，讓下一段繼續做
        return data

    @classmethod
    def aggregation(cls, single_task, r_conn, utils):
        target_table = 'player_summary_1h'

        # aggregation交由DB處理
        report = cls.get_player_profit_report_1h(single_task, r_conn, utils)
        if len(report.index) == 0:
            return

        # risky check
        report = cls.check_if_risky(report)

        # 先刪後寫，支援重跑
        cls.delete_before_insert(single_task, r_conn)
        report.to_sql(target_table, r_conn, if_exists='append', index=False)

    @staticmethod
    def get_player_profit_report_1h(single_task, r_conn, utils):
        source_table = 'player_summary_5min'

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

        sql = f"""
                    SELECT `platform`,
                          `site_code`,
                          `game_code`,
                          `player_name`,
                          `country`,
                          SUM(`b_count`) as b_count,
                          SUM(`b_amount`) as b_amount,
                          SUM(`w_amount`) as w_amount,
                          SUM(`fee_amount`) as fee_amount,
                          SUM(`profit_amount`) as profit_amount,
                          SUM(`refund_amount`) as refund_amount,
                          SUM(`normal_amount`) as normal_amount,
                          SUM(`bonus_amount`) as bonus_amount,
                          SUM(`free_amount`) as free_amount,
                          SUM(`jp_amount`) as jp_amount,
                          SUM(`valid_amount`) as valid_amount,
                          SUM(`cancel_amount`) as cancel_amount,
                          ratio,
                          SUM(`p_before_amount`) as p_before_amount,
                          SUM(`p_after_amount`) as p_after_amount,
                          SUM(`profit_amount`) / SUM(`b_amount`) as rtp
                      FROM `{source_table}`
                      WHERE summary_date = '{int(single_task['gte_time'].strftime("%Y%m%d"))}' 
                        AND hours = '{int(single_task['gte_time'].strftime("%H"))}'
                        {platform_filter}
                        {site_code_filter}
                        {game_code_filter}
                      GROUP BY `platform`, `site_code`, `game_code`, `player_name`, `country`
               """

        # report_1h = pd.read_sql(sql, r_conn)
        report_1h = retry_call(pd.read_sql, fargs=[sql, r_conn], tries=10, delay=2,
                               logger=utils['logger'])

        # rtp或wl可能發生除以0的問題
        report_1h = report_1h.replace([np.inf, -np.inf], np.nan).fillna(0)

        # 組織時間格式
        report_1h['summary_date'] = int(single_task['gte_time'].strftime("%Y%m%d"))
        report_1h['hours'] = int(single_task['gte_time'].strftime("%H"))
        report_1h['start_time'] = single_task['gte_time']

        return report_1h

    @staticmethod
    def check_if_risky(final_report):
        # is_risky 初始值為0
        final_report['is_risky'] = 0
        # 基本篩選條件
        final_report.loc[(final_report['profit_amount'] >= player_summary_rtp_config['profit_threshold'])
                         & (final_report['b_count'] >= player_summary_rtp_config['bet_count_threshold'])
                         & (final_report['rtp'] >= player_summary_rtp_config['rtp_threshold']), 'is_risky'] = 1
        # 贏太多
        final_report.loc[
            (final_report['profit_amount'] >= player_summary_rtp_config['profit_unconditional']), 'is_risky'] = 1

        return final_report

    @staticmethod
    def delete_before_insert(single_task, conn):
        target_table = 'player_summary_1h'

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
                          {platform_filter}
                          {site_code_filter}
                          {game_code_filter}
                     """

        conn.execute(delete_sql)
