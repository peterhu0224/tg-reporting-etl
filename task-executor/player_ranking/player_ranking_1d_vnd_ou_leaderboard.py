from retry.api import retry_call
import pandas as pd
from datetime import datetime, timedelta
import random

from player_ranking.player_ranking_1d_config import VndOuConfig
from utils.DBUtils import DBUtils
from utils.ExecUtils import ExecUtils
from utils.PipelineUtils import ProcessStep

# 處理pandas小數點
pd.options.display.float_format = '{:.10f}'.format

"""
功能：
越式大小排行榜，每2分鐘update一次，並且需要確保贏分最高會員不得進入排行榜。

實作方式：
1. 模擬20個機器人，隨機投注10萬且標準差為50萬的注碼共3注並結束該回合，若有會員進入20名以內，則繼續投注直到會員不在20名以內。
2. 正常情況下(會員未進前20)，機器人有51%勝率，因此排行榜會因為輸贏而有起落
3. 若會員進入前20，會啟動boost模式，勝率100%且投注額乘5倍，以更有效率的方式追趕，減少運算次數

重跑方式：
從player_ranking_1d_config中設定VND_OU_RERUN_START_DATE(起始日期)與VND_OU_RERUN_END_DATE(結束日期)，
並且直接執行player_ranking_1d_vnd_ou_leaderboard.py這個腳本即可
"""


class player_ranking_1d_vnd_ou_leaderboard(ProcessStep):
    @classmethod
    def process(cls, data, utils):
        # 取出config
        for origin_list in VndOuConfig.VND_OU_LEADERBOARD_LIST:
            # 將維護清單展開
            site_info_list = pd.json_normalize(origin_list).explode(['site_code']).explode(
                ['game_code']).explode(['room_type']).explode(['country']).to_dict(
                orient='records')
            # 開始逐筆維護排行榜
            for site_info in site_info_list:
                # 植入utils
                site_info['utils'] = utils
                # 設定執行時間
                site_info['state_date'] = datetime.now()
                r_conn = utils['db_utils'].get_report_db_cr_report_db_conn()
                # get report
                ranked_report = cls.get_ranked_report(site_info, r_conn)

                # 若無法取得report，代表該站點無實作(可能為config組合問題)
                if ranked_report is None or len(ranked_report) == 0:
                    continue

                # update ranking
                cls.update_ranking(ranked_report, site_info, r_conn)
                # update is_bonus
                cls.update_bonus_get(site_info, r_conn)
                r_conn.close()

    @classmethod
    def get_history_report(cls, site_info, r_conn, method='ALL'):
        # 取出預設日期
        today = site_info['state_date'].strftime("%Y-%m-%d")
        # 取得當前報表
        # history_report預設取全部
        sql = ""
        if method == 'ALL':
            sql = f""" SELECT * 
                         FROM player_ranking_1d
                        WHERE 1=1 
                          AND state_date = '{today}'
                          AND platform = '{site_info['platform']}'
                          AND site_code = '{site_info['site_code']}'
                          AND game_code = '{site_info['game_code']}'
                          AND room_type = '{site_info['room_type']}'
                          AND country = '{site_info['country']}'
                   """
        # 用來輔助upsert的功能
        if method == 'upsert':
            sql = f"""SELECT 1 
                        FROM player_ranking_1d
                       WHERE 1=1 
                         AND state_date = '{today}'
                         AND platform = '{site_info['platform']}'
                         AND site_code = '{site_info['site_code']}'
                         AND game_code = '{site_info['game_code']}'
                         AND room_type = '{site_info['room_type']}'
                         AND country = '{site_info['country']}'
                       LIMIT 1
                   """

        # 取得DB資料
        history_report = retry_call(pd.read_sql, fargs=[sql, r_conn], tries=10, delay=2,
                                    logger=site_info['utils']['logger'])

        return history_report

    @classmethod
    def get_base_report(cls, site_info, r_conn):
        # 取出三日已出現過的機器人名單
        three_days_ago = (site_info['state_date'] - timedelta(days=4)).strftime("%Y-%m-%d")
        yesterday = (site_info['state_date'] - timedelta(days=1)).strftime("%Y-%m-%d")
        sql_3days = f"""SELECT player_name 
                        FROM player_ranking_1d 
                        WHERE state_date >= '{three_days_ago}' 
                        AND state_date <= '{yesterday}'
                        AND game_code = '{site_info['game_code']}'
                        AND room_type = '{site_info['room_type']}'
                        AND country = '{site_info['country']}'
                    """

        # 取得DB資料
        showed_in3days = retry_call(pd.read_sql, fargs=[sql_3days, r_conn], tries=10, delay=2,
                                    logger=site_info['utils']['logger'])

        # 取得當前報表
        history_report = cls.get_history_report(site_info, r_conn)

        # 若報表當前無資料，初始化報表
        if len(history_report.index) == 0:
            gs_conn = site_info['utils']['db_utils'].get_source_db_cr_gs_conn()
            # 取出當前站點全部機器人名單
            all_robot_sql = f"""
                                  SELECT player_name 
                                    FROM player 
                                   WHERE type = 'ROBOT' 
                                     AND status = 'ACTIVATE'
                                     AND platform = '{site_info['platform']}'
                                     AND site_code = '{site_info['site_code']}'
                             """
            robot_list = retry_call(pd.read_sql, fargs=[all_robot_sql, gs_conn], tries=10, delay=2,
                                    logger=site_info['utils']['logger'])

            # 若當前站點無機器人，代表該站點無實作 (可能是config組合問題)
            # 直接返回不執行
            if robot_list is None or len(robot_list) == 0:
                site_info['utils']['logger'].info(
                    f"player_ranking_1d_vnd_ou_leaderboard.get_base_report(), platform:{site_info['platform']}, site_code:{site_info['site_code']}, game_code:{site_info['game_code']}, room_type:{site_info['room_type']}, country:{site_info['country']}, can't get ROBOT list, will skip VND_OU_LEADERBOARD process)"
                )
                return

            # 排除三日內已出現過的名單
            robot_list = robot_list[~robot_list['player_name'].isin(showed_in3days['player_name'])]
            # 隨機抽取20個機器人名單
            history_report = robot_list.sample(n=20)
            # 初始化數據
            history_report['state_date'] = site_info['state_date'].strftime("%Y-%m-%d")
            history_report['platform'] = site_info['platform']
            history_report['site_code'] = site_info['site_code']
            history_report['game_code'] = site_info['game_code']
            history_report['room_type'] = site_info['room_type']
            history_report['country'] = site_info['country']
            history_report['bet'] = 0
            history_report['win'] = 0
            history_report['rank_no'] = history_report['win'].rank(method='first', ascending=False).astype(int)
            history_report['is_bonus'] = False
            history_report['is_robot'] = True
            gs_conn.close()

        return history_report

    @classmethod
    def rolling_bet(cls, row, win_boost):
        """
        開始模擬投注
        DB數字全部要乘以1000倍，後端取數時會在除以1000，因此數字要先乘1000
        :param row:
        :param win_boost:
        :return:
        """
        win_amount = row['win']
        bet_amount = row['bet']
        # 高離散正態分布，隨機三注
        gauss = abs(int(random.gauss(mu=100, sigma=500)))
        for betting in [gauss * 100 for _ in range(3)]:
            # 追趕機制，勝率100%，倍率5倍，做完直接跳出
            if win_boost:
                win_amount += (betting * 0.98) * 5 * 1000
                bet_amount += betting * 5 * 1000
                continue
            # 預設勝率51%
            if random.randint(1, abs(int(random.gauss(mu=115, sigma=20)))) > 50:
                # 贏單(2%抽水)
                win_amount += (betting * 0.98) * 1000
            # 寫入投注額總計
            bet_amount += betting * 1000
        return {'bet_amount': bet_amount, 'win_amount': win_amount}

    # 開始運算
    @classmethod
    def ranking(cls, base_report, max_value, win_boost=False):

        # 取出5位幸運兒開始一般下注1回合
        sample = base_report.sample(n=5)
        for i, row in sample.iterrows():
            result = cls.rolling_bet(row, win_boost)
            # 更新report
            base_report.at[i, 'bet'] = result['bet_amount']
            base_report.at[i, 'win'] = result['win_amount']

        # 若玩家進入20名(比贏最少的機器人高)，則不斷投注直到玩家被擠20名
        while base_report['win'].min() <= max_value:
            # 開啟追趕機制(100%勝率模式)
            win_boost = True
            # 取出5位幸運兒開始下注，直到贏最少(min)的高於真實玩家的max_value
            sample = base_report.sample(n=5)
            # 開始投注
            for i, row in sample.iterrows():
                result = cls.rolling_bet(row, win_boost)
                # 更新report
                base_report.at[i, 'bet'] = result['bet_amount']
                base_report.at[i, 'win'] = result['win_amount']

        # rolling後更新排名
        base_report.sort_values(by='win', inplace=True, ascending=False)
        base_report['rank_no'] = base_report['win'].rank(method='first', ascending=False).astype(int)

        return base_report

    @classmethod
    def get_max_value(cls, site_info, r_conn):
        today = site_info['state_date'].strftime("%Y-%m-%d")
        source_table = 'player_daily_ranking'
        sql = f"""
                    SELECT country, MAX(win) as max_value
                      FROM {source_table}
                     WHERE 1=1
                       AND state_date = '{today}'
                       AND is_robot = false
                       AND platform = '{site_info['platform']}'
                       AND site_code = '{site_info['site_code']}'
                       AND game_code = '{site_info['game_code']}'
                       AND room_type = '{site_info['room_type']}'
                       AND country = '{site_info['country']}'
                       GROUP BY platform, site_code, game_code, room_type, country
              """

        # 取得DB資料
        max_value_df = retry_call(pd.read_sql, fargs=[sql, r_conn], tries=10, delay=2,
                                  logger=site_info['utils']['logger'])

        # max_value預設為0
        max_value = 0
        # 若有max_value則以DB資料為準
        if len(max_value_df.index) > 0:
            max_value = max_value_df['max_value'].values[0]
        # 若max_value為負數則以0計
        if max_value <= 0:
            # VndOuConfig.DEFAULT_MAX_VALUE預設0
            max_value = VndOuConfig.VND_OU_DEFAULT_MAX_VALUE

        return max_value

    @classmethod
    def get_ranked_report(cls, site_info, r_conn):
        # 取得未更新的報表
        base_report = cls.get_base_report(site_info, r_conn)

        # 若無法取得base_report，代表該站點無實作(可能為config組合問題)
        if base_report is None or len(base_report) == 0:
            return

        # 取得最高贏分的會員贏分數字
        max_value = cls.get_max_value(site_info, r_conn)
        # 帶入最高分數字，取得即時新排行榜
        ranked_report = cls.ranking(base_report, max_value)

        return ranked_report

    @classmethod
    def update_ranking(cls, ranked_report, site_info, r_conn):
        # 檢查當天是否有資料，做upsert
        tdf = cls.get_history_report(site_info, r_conn, 'upsert')
        # 若有資料則更新資料
        if len(tdf.index) > 0:
            for i, row in ranked_report.iterrows():
                sql = f""" UPDATE player_ranking_1d
                              SET bet = {row['bet']}, win = {row['win']}, rank_no = {row['rank_no']}
                            WHERE state_date = '{row['state_date']}' 
                              AND platform = '{row['platform']}' 
                              AND site_code = '{row['site_code']}'
                              AND game_code = '{row['game_code']}'
                              AND room_type = '{site_info['room_type']}'
                              AND country = '{site_info['country']}'
                              AND player_name = '{row['player_name']}'
                        """

                retry_call(r_conn.execute, fargs=[sql], fkwargs={"multi": True}, tries=10, delay=2,
                           logger=site_info['utils']['logger'])

        # 若無資料則插入資料
        else:
            target_table = 'player_ranking_1d'
            retry_call(ranked_report.to_sql, fargs=[target_table, r_conn],
                       fkwargs={'if_exists': 'append', 'index': False}, tries=10, delay=2,
                       logger=site_info['utils']['logger'])

    @classmethod
    def update_bonus_get(cls, site_info, r_conn):
        """
        單日排行榜結束，隨機派獎3~8名
        :param r_conn:
        :param site_info:
        :return:
        """
        yesterday = site_info['state_date'] - timedelta(days=1)
        bonus_count_sql = f"""
                SELECT country, SUM(if(is_bonus, 1, 0)) as bonus_count
                  FROM player_ranking_1d
                 WHERE state_date = '{yesterday}'
                   AND platform = '{site_info['platform']}' 
                   AND site_code = '{site_info['site_code']}'
                   AND game_code = '{site_info['game_code']}'
                   AND room_type = '{site_info['room_type']}'
                   AND country = '{site_info['country']}'
                   GROUP BY platform, site_code, game_code, room_type, country
                """
        history_sql = f"""
                        SELECT *
                          FROM player_ranking_1d
                         WHERE state_date = '{yesterday}'
                           AND platform = '{site_info['platform']}' 
                           AND site_code = '{site_info['site_code']}'
                           AND game_code = '{site_info['game_code']}'
                           AND room_type = '{site_info['room_type']}'
                           AND country = '{site_info['country']}'
                       """

        bonus_count = retry_call(pd.read_sql, fargs=[bonus_count_sql, r_conn], tries=10, delay=2,
                                 logger=site_info['utils']['logger'])

        # 若昨日資料為空，代表今天為第一天，，返回
        if len(bonus_count.index) == 0:
            return
        # 若昨日有資料，且bonus_count>0代表已派發過，返回
        if bonus_count['bonus_count'].values[0] > 0:
            return

        # 若上面代碼都通過，代表昨日有資料但尚未派獎
        # 派獎前先重骰一次，確保會員沒進20名
        sub_info = site_info.copy()
        sub_info['state_date'] = site_info['state_date'] - timedelta(days=1)
        ranked_report = cls.get_ranked_report(sub_info, r_conn)
        cls.update_ranking(ranked_report, sub_info, r_conn)
        # 取得昨日排行榜
        history = retry_call(pd.read_sql, fargs=[history_sql, r_conn], tries=10, delay=2,
                             logger=site_info['utils']['logger'])

        # 取出前20名，隨機抽3~8個人中獎
        bonus_sample = history.sample(
            n=random.randint(VndOuConfig.VND_OU_BONUS_RELEASE_FLOOR, VndOuConfig.VND_OU_BONUS_RELEASE_CEIL))
        bonus_sample['is_bonus'] = True
        for i, row in bonus_sample.iterrows():
            sql = f""" UPDATE player_ranking_1d
                          SET is_bonus = {row['is_bonus']}
                        WHERE state_date = '{row['state_date']}' 
                          AND platform = '{row['platform']}' 
                          AND site_code = '{row['site_code']}'
                          AND game_code = '{row['game_code']}'
                          AND room_type = '{site_info['room_type']}'
                          AND country = '{site_info['country']}'
                          AND player_name = '{row['player_name']}'
                    """
            # r_conn.execute(sql, multi=True)
            retry_call(r_conn.execute, fargs=[sql], fkwargs={'multi': 'True'}, tries=10, delay=2,
                       logger=site_info['utils']['logger'])

    @classmethod
    def rerun(cls, utils, rerun_start_date, rerun_end_date):
        # 取得重跑日期區間，並開始逐日重跑
        for exec_date in pd.date_range(rerun_start_date, rerun_end_date, freq='1D'):
            # 取出config
            for origin_list in VndOuConfig.VND_OU_LEADERBOARD_LIST:
                # 將維護清單展開
                site_info_list = pd.json_normalize(origin_list).explode(['site_code']).explode(
                    ['game_code']).explode(['room_type']).explode(['country']).to_dict(
                    orient='records')
                # 開始逐筆維護排行榜
                for site_info in site_info_list:
                    # 植入utils
                    site_info['utils'] = utils
                    # 設定執行時間
                    site_info['state_date'] = exec_date
                    r_conn = utils['db_utils'].get_report_db_cr_report_db_conn()
                    # get report
                    ranked_report = cls.get_ranked_report(site_info, r_conn)

                    # 若無法取得report，代表該站點無實作(可能為config組合問題)
                    if ranked_report is None or len(ranked_report) == 0:
                        continue

                    # update ranking
                    cls.update_ranking(ranked_report, site_info, r_conn)
                    # update is_bonus
                    cls.update_bonus_get(site_info, r_conn)
                    r_conn.close()


if __name__ == '__main__':
    # 設定logger
    logger = ExecUtils.get_report_logger()
    logger.propagate = False

    utils = {
        'db_utils': DBUtils(),
        'exec_utils': ExecUtils(),
        'logger': logger
    }
    # 開始重跑
    player_ranking_1d_vnd_ou_leaderboard.rerun(utils, VndOuConfig.VND_OU_RERUN_START_DATE,
                                               VndOuConfig.VND_OU_RERUN_END_DATE)
