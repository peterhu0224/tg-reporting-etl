import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from retry.api import retry_call

from utils.PipelineUtils import ProcessStep
from report_config import risk_ctrl_1d_config


class risk_ctrl_rtp_1d(ProcessStep):
    @classmethod
    def process(cls, data, utils):

        db_utils = utils['db_utils']
        exec_utils = utils['exec_utils']

        # 取1d task_list裡屬於risk_ctrl_rpt_1d自己該做的任務
        assignee = 'risk_ctrl_rtp_1d'
        sub_data = data[data['assignee'] == assignee]

        task_conn = db_utils.get_task_db_maria_conn()
        report_conn = getattr(db_utils, risk_ctrl_1d_config['target_conn_report'])()

        # 開始逐筆完成任務
        for i, row in sub_data.iterrows():
            exec_utils.update_task_apply_time(row, task_conn)
            retry_call(cls.aggregation, fargs=[row, report_conn], tries=5, delay=2, logger=utils['logger'])
            if datetime.now() > row['lt_time']:
                exec_utils.update_task_exec_time(row, task_conn)

        task_conn.close()
        report_conn.close()

        # 把data交回，讓下一段繼續做
        return data

    @classmethod
    def aggregation(cls, single_task, r_conn):
        target_table = 'risk_ctrl_rtp_1d'

        # aggregation交由DB處理
        meta_df = cls.get_meta_df(single_task, r_conn)

        if meta_df is None or len(meta_df.index) == 0:
            return
        # 標記risky
        risky_report = cls.check_if_risky(meta_df)
        # 計算新表並更新舊表
        risky_report = cls.update_to_old_df(risky_report, r_conn)
        # 若無舊表則不需進行後續檢查
        if risky_report is None:
            return
        # 檢查警報時間
        risky_report = cls.check_if_time_to_alert(risky_report)
        # 檢查警報次數
        risky_report = cls.check_if_alert_limited(risky_report)

        # 先刪後寫，支援重跑
        cls.delete_before_insert(single_task, r_conn)
        # 寫回DB
        risky_report.to_sql(target_table, r_conn, if_exists='append', index=False)

    @staticmethod
    def get_meta_df(single_task, r_conn):
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
                    SELECT platform, site_code, player_name, game_code, country,
                           COUNT(`b_amount`) AS b_count,
                           SUM(`b_amount`) AS b_cur,
                           SUM(`profit_amount`) AS p_cur,
                           SUM(`profit_amount`) / SUM(`b_amount`)  AS rtp_cur
                      FROM player_summary_1h
                     WHERE 1=1 
                       AND summary_date = '{int(single_task['gte_time'].strftime('%Y%m%d'))}'
                           {platform_filter}
                           {site_code_filter}
                           {game_code_filter}
                      GROUP BY `platform`, `site_code`, `player_name`, `game_code`, `country`
             """

        # 取得並檢查metadata是否為空
        meta_df = pd.read_sql(sql, r_conn)
        if meta_df is None or len(meta_df) == 0:
            return

        # 植入報表時間
        meta_df['summary_date'] = int(single_task['gte_time'].strftime("%Y%m%d"))
        # 處理空值及除以0的問題
        meta_df = meta_df.replace([np.inf, -np.inf], np.nan).fillna(0)

        return meta_df

    @staticmethod
    def check_if_risky(meta_df):
        # 初始化
        meta_df['is_risky'] = 0
        meta_df['send_alert'] = 0
        # RPT、投注、輸贏同時滿足警示條件
        meta_df.loc[(meta_df['p_cur'] >= risk_ctrl_1d_config['rtp_cfg_player']['profit_threshold'])
                    & (meta_df['b_count'] >= risk_ctrl_1d_config['rtp_cfg_player']['bet_count_threshold'])
                    & (meta_df['rtp_cur'] >= risk_ctrl_1d_config['rtp_cfg_player']['rtp_threshold']), [
                        'is_risky',
                        'send_alert']] = 1
        # 贏太多，無條件警示
        meta_df.loc[
            (meta_df['p_cur'] >= risk_ctrl_1d_config['rtp_cfg_player']['profit_unconditional']), [
                'is_risky', 'send_alert']] = 1

        return meta_df

    @staticmethod
    def update_to_old_df(risky_df, r_conn):
        # 取基準日期
        summary_date = risky_df['summary_date'][0]
        # 取出舊資料
        sql = f"SELECT * FROM risk_ctrl_rtp_1d WHERE summary_date = '{summary_date}'"
        old_df = pd.read_sql(sql, r_conn)
        # 若無舊資料(代表本次為第一次)，將risky_report資料補齊後寫入DB，並且直接return
        if len(old_df.index) == 0 or old_df is None:
            risky_df['alert_count'] = 0
            risky_df['last_alert_time'] = pd.to_datetime('1970-01-01')
            # 直接寫入DB
            risky_df.to_sql('risk_ctrl_rtp_1d', r_conn, if_exists='append', index=False)
            return

        # 若有舊資料，則更新舊資料

        # 組合新舊資料並更新(old_df只增不減，所以用left_join)
        new_df = old_df.merge(risky_df, how='left',
                              left_on=['platform', 'site_code', 'player_name', 'game_code', 'country'],
                              right_on=['platform', 'site_code', 'player_name', 'game_code', 'country'],
                              suffixes=('', '_new'))
        # 將新資料複寫至所有舊資料
        new_df['b_count'] = new_df['b_count_new']
        new_df['b_cur'] = new_df['b_cur_new']
        new_df['p_cur'] = new_df['p_cur_new']
        new_df['rtp_cur'] = new_df['rtp_cur_new']
        new_df['is_risky'] = new_df['is_risky_new']
        new_df['send_alert'] = new_df['send_alert_new']

        # reset所有is_risky=0的資料
        new_df.loc[new_df['is_risky'] == 0, 'send_alert'] = 0
        new_df.loc[new_df['is_risky'] == 0, 'alert_count'] = 0
        new_df.loc[new_df['is_risky'] == 0, 'last_alert_time'] = pd.to_datetime('1970-01-01')

        # 將old中沒有的全新資料(即新投注會員)取出後加回old
        dup_subset = ['platform', 'site_code', 'player_name', 'game_code', 'country']
        all_new_df = pd.concat([old_df, risky_df]).drop_duplicates(subset=dup_subset, keep=False)
        all_new_df.to_excel('all_new_df.xlsx')

        # 補齊初始化資料
        all_new_df['alert_count'] = 0
        all_new_df['last_alert_time'] = pd.to_datetime('1970-01-01')
        # 加回old
        new_df = pd.concat([new_df, all_new_df]).reset_index(drop=True)
        # 更新時間old時間
        new_df['update_time'] = datetime.now()

        return new_df

    @staticmethod
    def check_if_time_to_alert(new_df):
        # 將時間差異先寫下來
        new_df['time_check'] = new_df['last_alert_time'].apply(lambda x: (datetime.now() - x).total_seconds() / 60)
        # 取參數
        time_attr = risk_ctrl_1d_config['rtp']['time_to_alert']
        # 時間未到的row，send_alert改為0
        new_df.loc[new_df['time_check'] < time_attr, 'send_alert'] = 0
        # 清空無效欄位
        new_df.drop(columns=['time_check'], inplace=True)

        return new_df

    @staticmethod
    def check_if_alert_limited(new_df):
        # 參數
        count_attr = risk_ctrl_1d_config['rtp']['alert_limit']
        # 已到上限的不再警報
        new_df.loc[(new_df['send_alert'] == 1) & (new_df['alert_count'] >= count_attr), 'send_alert'] = 0
        # 未到上限的次數+1，
        new_df.loc[(new_df['send_alert'] == 1), 'alert_count'] = new_df['alert_count'] + 1
        # 更新最後警報時間
        new_df.loc[(new_df['send_alert'] == 1), 'last_alert_time'] = datetime.now()
        # 篩選最後的欄位
        columns = ['summary_date', 'platform', 'site_code', 'player_name', 'game_code', 'country', 'b_count', 'b_cur',
                   'p_cur', 'rtp_cur', 'is_risky', 'send_alert', 'alert_count', 'last_alert_time', 'update_time',
                   'create_time']

        new_df = new_df[columns]

        return new_df

    def send_alert_msg_to_java(self):
        pass

    @staticmethod
    def delete_before_insert(single_task, conn):
        target_table = 'risk_ctrl_rtp_1d'

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
                                    {platform_filter}
                                    {site_code_filter}
                                    {game_code_filter}
                     """

        conn.execute(delete_sql)
