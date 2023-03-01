import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from retry.api import retry_call

from utils.PipelineUtils import ProcessStep
from report_config import risk_ctrl_1d_config


class risk_ctrl_player_1d(ProcessStep):
    @classmethod
    def process(cls, data, utils):
        db_utils = utils['db_utils']
        exec_utils = utils['exec_utils']

        # 取1d task_list裡屬於risk_control_player_1d自己該做的任務
        assignee = 'risk_ctrl_player_1d'
        sub_data = data[data['assignee'] == assignee]

        task_conn = db_utils.get_task_db_maria_conn()
        report_conn = getattr(db_utils, risk_ctrl_1d_config['target_conn_report'])()

        # 開始逐筆完成任務
        for i, row in sub_data.iterrows():
            exec_utils.update_task_apply_time(row, task_conn)

            # cls.aggregation(row, report_conn, utils)
            retry_call(cls.aggregation, fargs=[row, report_conn], tries=5, delay=2, logger=utils['logger'])

            if datetime.now() > row['lt_time']:
                exec_utils.update_task_exec_time(row, task_conn)

        task_conn.close()
        report_conn.close()

        # 把data交回，讓下一段繼續做
        return data

    @classmethod
    def aggregation(cls, single_task, r_conn):
        target_table = 'risk_ctrl_player_1d'

        # aggregation交由DB處理
        meta_df = cls.get_metadata(single_task, r_conn)

        if meta_df is None or len(meta_df.index) == 0:
            return

        sigma_df = cls.get_sigma_data(meta_df)
        risk_report = meta_df.merge(sigma_df, how='left', left_on=['platform', 'site_code', 'player_name', 'country'],
                                    right_on=['platform', 'site_code', 'player_name', 'country'])

        # 先刪後寫，支援重跑
        cls.delete_before_insert(single_task, r_conn)
        risk_report.to_sql(target_table, r_conn, if_exists='append', index=False)

    @staticmethod
    def get_metadata(single_task, r_conn):
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

        current_sum_sql = f"""
                                SELECT platform, site_code, player_name, country,
                                       SUM(`b_amount`) AS b_cur,
                                       SUM(`profit_amount`) AS p_cur,
                                       SUM(`profit_amount`) / SUM(`b_amount`)  AS rtp_cur
                                  FROM player_summary_1h
                                 WHERE 1=1 
                                   AND summary_date = '{int(single_task['gte_time'].strftime('%Y%m%d'))}'
                                       {platform_filter}
                                       {site_code_filter}
                                       {game_code_filter}
                                  GROUP BY `platform`, `site_code`, `player_name`, `country`
                           """

        current_trans_sql = f"""
                                SELECT platform, site_code, player_name, country,
                                       SUM(trans_out_amount-trans_in_amount) AS net_trans_cur
                                  FROM trans_summary_1h
                                 WHERE 1=1 
                                   AND summary_date = '{int(single_task['gte_time'].strftime('%Y%m%d'))}'
                                       {platform_filter}
                                       {site_code_filter}
                                       {game_code_filter}
                                  GROUP BY `platform`, `site_code`, `player_name`, `country`
                            """

        p_wma_sql = f"""
                            SELECT platform, site_code, player_name, country,
                                   SUM(b_amount)/7 AS b_wma,
                                   SUM(p_amount)/7 AS p_wma,
                                   STDDEV(b_amount) AS b_wmasd,
                                   STDDEV(p_amount) AS p_wmasd
                             FROM(
                            SELECT summary_date, platform, site_code, player_name, country, 
                                   SUM(b_amount) AS b_amount,
                                   SUM(profit_amount) AS p_amount
                              FROM player_summary_1d
                             WHERE 1=1 
                               AND summary_date >= '{int((single_task['gte_time'] - timedelta(days=8)).strftime('%Y%m%d'))}' 
                               AND summary_date < '{int((single_task['gte_time'] - timedelta(days=0)).strftime('%Y%m%d'))}' 
                             GROUP BY summary_date, `platform`, `site_code`, `player_name`, `country`
                              ) AS a
                             GROUP BY `platform`, `site_code`, `player_name`, `country`;
                       """

        trans_wma_sql = f"""
                            SELECT platform, site_code, player_name, country,
                                   SUM(net_trans_amount)/7 AS net_trans_wma,
                                   STDDEV(net_trans_amount) AS net_trans_wmasd
                             FROM(
                            SELECT summary_date, platform, site_code, player_name, country, 
                                   (trans_out_amount-trans_in_amount) AS net_trans_amount
                              FROM trans_summary_1d
                             WHERE 1=1 
                               AND summary_date >= '{int((single_task['gte_time'] - timedelta(days=8)).strftime('%Y%m%d'))}' 
                               AND summary_date < '{int((single_task['gte_time'] - timedelta(days=0)).strftime('%Y%m%d'))}' 
                             GROUP BY summary_date, `platform`, `site_code`, `player_name`, `country`
                              ) AS a
                             GROUP BY `platform`, `site_code`, `player_name`, `country`;
                       """

        # 取得並檢查metadata
        current_df = pd.read_sql(current_sum_sql, r_conn)
        if current_df is None or len(current_df) == 0:
            return

        # 取得統計數據
        current_trans = pd.read_sql(current_trans_sql, r_conn)
        p_wma_df = pd.read_sql(p_wma_sql, r_conn)
        trans_wma_df = pd.read_sql(trans_wma_sql, r_conn)

        # 組合報表
        tdf1 = current_df.merge(current_trans, how='left', left_on=['platform', 'site_code', 'player_name', 'country'],
                                right_on=['platform', 'site_code', 'player_name', 'country'])
        tdf2 = tdf1.merge(p_wma_df, how='left', left_on=['platform', 'site_code', 'player_name', 'country'],
                          right_on=['platform', 'site_code', 'player_name', 'country'])
        meta_df = tdf2.merge(trans_wma_df, how='left', left_on=['platform', 'site_code', 'player_name', 'country'],
                             right_on=['platform', 'site_code', 'player_name', 'country'])
        # 植入報表時間
        meta_df['summary_date'] = int(single_task['gte_time'].strftime("%Y%m%d"))
        # 處理空值及除以0的問題
        meta_df = meta_df.replace([np.inf, -np.inf], np.nan).fillna(0)

        return meta_df

    @staticmethod
    def get_sigma_data(meta_df):
        """
        模擬布林通道，只對數據上界部分做警示
        計算sigma與risk_score

        公式:
        b_sigma = abs(b_cur - b_wma) / b_wmasd
        說明：
        b_sigma = 當前數值與7日均值的差，除以7日標準差，得sigma
        條件：
        若:b_wma或b_wmasd數值低於預設值，則帶入預設值 (避免數值過小，會不斷觸碰到上界產生過敏問題)
        且:因投注少或輸錢會員無風險，因此只判斷上界，若數值小於預設，或為負數(wl)，則直接給sigma為0

        :param meta_df:
        :return:sigma_df
        """
        # 占存避免影響原始資料
        tdf = meta_df.copy()
        # 名稱太長，在內部可以簡短一點
        cfg = risk_ctrl_1d_config
        # 寫入幣值轉換值
        tdf['country_rate'] = tdf['country'].map(cfg['country_rate'])

        """
        投注值轉換 (若某項wma數據比預設值低，則視為新用戶並以預設值取代，降低警示靈敏度)
        """
        b_wma_floor = cfg['default_value_player']['def_b_wma']
        b_wmasd_floor = cfg['default_value_player']['def_b_wmasd']
        tdf.loc[tdf['b_wma'] < b_wma_floor, 'b_wma'] = b_wma_floor * tdf['country_rate']
        tdf.loc[tdf['b_wmasd'] < b_wmasd_floor, 'b_wmasd'] = b_wmasd_floor * tdf['country_rate']

        """
        輸贏值轉換 (值小於預設無論正負直接取代)
        """
        p_wma_floor = cfg['default_value_player']['def_p_wma']
        p_wmasd_floor = cfg['default_value_player']['def_p_wmasd']
        tdf.loc[tdf['p_wma'] < b_wma_floor, 'p_wma'] = p_wma_floor * tdf['country_rate']
        tdf.loc[tdf['p_wmasd'] < b_wmasd_floor, 'p_wmasd'] = p_wmasd_floor * tdf['country_rate']

        """
        轉出入值轉換
        """
        trans_wma_floor = cfg['default_value_player']['def_p_wma']
        trans_wmasd_floor = cfg['default_value_player']['def_p_wmasd']
        tdf.loc[tdf['net_trans_wma'] < b_wma_floor, 'net_trans_wma'] = trans_wma_floor * tdf['country_rate']
        tdf.loc[tdf['net_trans_wmasd'] < b_wmasd_floor, 'net_trans_wmasd'] = trans_wmasd_floor * tdf['country_rate']

        """
        開始sigma計算 (取當前值與7日均值的距離，除以SD得sigma)
        """
        # bet sigma
        tdf.loc[tdf['b_cur'] >= tdf['b_wma'], 'b_sigma'] = abs(tdf['b_cur'] - tdf['b_wma']) / tdf['b_wmasd']
        # wl sigma
        tdf.loc[tdf['p_cur'] >= tdf['p_wma'], 'p_sigma'] = abs(tdf['p_cur'] - tdf['p_wma']) / tdf['p_wmasd']
        tdf.loc[tdf['p_cur'] <= 0, 'p_sigma'] = 0  # 輸贏為負或零的，sigma以0計
        # trans sigma
        tdf.loc[tdf['net_trans_cur'] >= tdf['net_trans_wma'], 'trans_sigma'] = abs(
            tdf['net_trans_cur'] - tdf['net_trans_wma']) / tdf['net_trans_wmasd']
        tdf.loc[tdf['net_trans_cur'] <= 0, 'trans_sigma'] = 0  # 淨轉出負或零的，sigma以0計
        # 淨轉出與輸贏比例
        tdf.loc[tdf['net_trans_cur'] > 0, 'p_trans_multi'] = tdf['net_trans_cur'] / tdf['p_cur']
        # 補上0
        tdf.fillna(0, inplace=True)

        # 計算權重並加總risk_score
        tdf['risk_score'] = tdf['b_sigma'] * cfg['risk_score_multi']['b'] \
                            + tdf['p_sigma'] * cfg['risk_score_multi']['wl'] \
                            + tdf['trans_sigma'] * cfg['risk_score_multi']['trans'] \
                            + tdf['p_trans_multi'] * cfg['risk_score_multi']['p_trans_multi']
        # 只取要的欄位
        sigma_df = tdf[
            ['platform', 'site_code', 'player_name', 'country', 'b_sigma', 'p_sigma', 'trans_sigma',
             'p_trans_multi', 'risk_score']].copy()

        # 處理空值及除以0的問題
        sigma_df = sigma_df.replace([np.inf, -np.inf], np.nan).fillna(0)

        return sigma_df

    @staticmethod
    def check_if_risky(final_report):
        # todo : 判斷是否為風險會員，不只看RTP
        # # is_risky 初始值為0
        # final_report['is_risky'] = 0
        # # 基本篩選條件
        # final_report.loc[(final_report['profit_amount'] >= risk_ctrl_1d_config['rtp_cfg_player']['profit_threshold'])
        #                  & (final_report['b_count'] >= risk_ctrl_1d_config['rtp_cfg_player']['bet_count_threshold'])
        #                  & (final_report['rtp'] >= risk_ctrl_1d_config['rtp_cfg_player']['rtp_threshold']), 'is_risky'] = 1
        # # 贏太多
        # final_report.loc[(final_report['profit_amount'] >= risk_ctrl_1d_config['rtp_cfg_player']['profit_unconditional']), 'is_risky'] = 1

        return final_report

    @staticmethod
    def delete_before_insert(single_task, conn):
        target_table = 'risk_ctrl_player_1d'

        # 預設task發布都是跑ALL，非ALL才跑指定的部分
        platform_filter = ""
        site_code_filter = ""
        game_code_filter = ""
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
