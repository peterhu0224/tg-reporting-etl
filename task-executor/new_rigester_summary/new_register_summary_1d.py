from datetime import datetime
import pandas as pd

from utils.PipelineUtils import ProcessStep
from report_config import new_register_summary_1d_config


class new_register_summary_1d(ProcessStep):
    @classmethod
    def process(cls, data, utils):
        """
        此報表需要join gs.player 與 report_db.player_summary_5min,
        前者只取，當天新會員人數，後者取這些新會員的當天數據
        此報表屬於REALTIME類型，每次執行都update一次(更新到當下，跨日才update done=1)
        :param data:
        :param utils:
        :return:
        """
        db_utils = utils['db_utils']
        exec_utils = utils['exec_utils']

        # 取1d task_list裡屬於player_summary_1d自己該做的任務
        assignee = 'new_register_summary_1d'
        sub_data = data[data['assignee'] == assignee]

        # 建立連接
        task_conn = db_utils.get_task_db_maria_conn()
        report_conn = getattr(db_utils, new_register_summary_1d_config['target_conn'])()
        gs_conn = getattr(db_utils, new_register_summary_1d_config['source_conn'])()

        # 開始逐筆完成任務
        for i, row in sub_data.iterrows():
            exec_utils.update_task_apply_time(row, task_conn)
            cls.aggregation(row, report_conn, gs_conn)
            # 跨日後才更新done=1，若當前done=0就會一直重跑(實現update效果)
            if datetime.now() > row['lt_time']:
                exec_utils.update_task_exec_time(row, task_conn)

        # 關閉連接
        task_conn.close()
        report_conn.close()
        gs_conn.close()

        # 把data交回，讓下一段繼續做
        return data

    @classmethod
    def aggregation(cls, single_task, r_conn, g_conn):
        target_table = 'new_register_summary_1d'

        # 取得新會員清單，若本次無新會員，直接返回
        new_reg_list = cls.get_new_reg_players_by_date(single_task, g_conn)
        if len(new_reg_list.index) == 0:
            return
        # 取得報表 ，若新會員均未進行投注，直接返回
        report = cls.get_new_register_summary_1d(single_task, new_reg_list, r_conn)
        if len(report.index) == 0:
            return

        # 先刪後寫，支援重跑
        cls.delete_before_insert(single_task, r_conn)
        report.to_sql(target_table, r_conn, if_exists='append', index=False)

    @staticmethod
    def get_new_reg_players_by_date(single_task, gs_conn):
        source_table = 'player'
        sql = f"""SELECT  player_name,
                          platform,
                          site_code
                     FROM {source_table}
                    WHERE reg_time >= '{single_task['gte_time']}' AND reg_time < '{single_task['lt_time']}'
                    AND type = 'NORMAL'
               """
        new_reg_list = pd.read_sql(sql, gs_conn)
        return new_reg_list

    @staticmethod
    def get_new_register_summary_1d(single_task, new_reg_list, r_conn):
        source_table = 'player_summary_5min'

        # 若task中的game_code = ALL則跑全部，若有指定則跑指定
        if single_task['game_code'] == 'ALL':
            game_code_filter = ""
        else:
            game_code_filter = f"AND game_code = '{single_task['game_code']}'"

        sql = f"""SELECT 
                      `platform`,
                      `site_code`,
                      `game_code`,
                      `player_name`,
                      `country`,
                      SUM(`b_count`) as b_count,
                      SUM(`b_amount`) as b_amount,
                      SUM(`wl_amount`) as wl_amount,
                      SUM(`fee_amount`) as fee_amount,
                      SUM(`profit_amount`) as profit_amount,
                      SUM(`refund_amount`) as refund_amount
                  FROM `{source_table}`
                  WHERE summary_date = '{int(single_task['gte_time'].strftime("%Y%m%d"))}'
                    AND platform = '{single_task['platform']}'
                    AND site_code = '{single_task['site_code']}'
                    {game_code_filter}
                  GROUP BY `platform`, `site_code`, `game_code`, `player_name`, `country`
               """

        ps_5min = pd.read_sql(sql, r_conn)

        """
        先取reg與bet的交集，獲得新會員的投注紀錄，之後再group by算當天的新會員數與投注總額
        """
        # 取5min與reg交集，如果交集為空，代表新會員沒人投注，直接返回
        reg_data = ps_5min.merge(new_reg_list, how='inner', left_on='player_name', right_on='player_name',
                                 suffixes=('', '_y'))
        if len(reg_data.index) == 0:
            return reg_data

        reg_data.drop(reg_data.filter(regex='_y$').columns, axis=1, inplace=True)
        # 算投注加總
        bet_sum = reg_data.groupby(['platform', 'site_code', 'game_code', 'country']).sum(numeric_only=True)
        # 算新會員數
        reg_count = reg_data.groupby(['platform', 'site_code', 'game_code', 'country']).count()[['player_name']]
        reg_count.rename(columns=({'player_name': 'reg_count'}), inplace=True)
        # 重新聚合成report
        reg_summary = bet_sum.merge(reg_count, left_on=['platform', 'site_code', 'game_code', 'country'],
                                    right_on=['platform', 'site_code', 'game_code', 'country']).reset_index()

        # 組織時間格式
        reg_summary['summary_date'] = int(single_task['gte_time'].strftime("%Y%m%d"))

        return reg_summary

    @staticmethod
    def delete_before_insert(single_task, conn):
        target_table = 'new_register_summary_1d'

        # 若task中的game_code = ALL則跑全部，若有指定則跑指定
        if single_task['game_code'] == 'ALL':
            game_code_filter = ""
        else:
            game_code_filter = f"AND game_code = '{single_task['game_code']}'"

        delete_sql = f"""
                        DELETE FROM {target_table}
                         WHERE summary_date = '{int(single_task['gte_time'].strftime("%Y%m%d"))}'
                           AND platform = '{single_task['platform']}'
                           AND site_code = '{single_task['site_code']}'
                           {game_code_filter}
                     """

        conn.execute(delete_sql)
