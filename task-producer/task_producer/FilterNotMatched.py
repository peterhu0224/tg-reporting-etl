from utils.PipelineUtils import ProcessStep
import pandas as pd


class FilterNotMatched(ProcessStep):

    @classmethod
    def process(cls, data, utils):

        """
        將dependency沒完成的任務篩掉，不發布任務，並且更新dep_not_matched_log table

        :param data: 來源於GetTaskDepCount的output
        :param utils: main impl的tool
        :return: DataFrame (回傳篩選後，可發布任務的結果)
        """
        db_utils = utils['db_utils']

        # 空值直接拋出去
        if len(data.index) == 0:
            return data

        """
        with中的全部動作均為update dep_not_matched_log
        """
        with db_utils.get_task_db_maria_conn() as conn:
            # select -> update -> delete -> insert
            # select
            old_not_matched = cls.scan_dep_not_matched_log(conn)
            # update_locally
            new_not_matched = cls.update_dep_not_matched_log_locally(old_not_matched, data)
            # delete
            cls.delete_old_not_matched_log(old_not_matched, conn)
            # Insert
            cls.produce_not_matched_log(new_not_matched, conn)

        # 回傳matched為1的結果
        matched_df = data[data['matched'] == 1]

        return matched_df

    @staticmethod
    def scan_dep_not_matched_log(conn):
        """
        SELECT Table 取得舊資料，準備進行後續更新
        :param conn: dbconn
        :return: DataFrame (old_log)
        """
        sql = "SELECT `level`, `platform`, `site_code`, `game_code`, `report_class`" \
              ", `assignee`, `freq_type`, `gte_time`, `lt_time`, `dep_count`, `matched`" \
              " FROM `dep_not_matched_log`" \
              " WHERE matched = 0"
        df = pd.read_sql(sql, conn)
        return df

    @staticmethod
    def update_dep_not_matched_log_locally(old_log, new_log):
        """
        取得舊資料後，與本回合資料進行比對，將not_matched_log更新後寫回去
        :param old_log: 來自DB的舊資料
        :param new_log: 本回合的新資料
        :return: DataFrame (可寫回的dep_not_matched_log)
        """

        # 若db裡沒有初始值，第一次以傳入的new_log作為old_log即可
        if len(old_log.index) == 0:
            old_log = new_log[new_log['matched'] == 0]

        """
        更新步驟 : SELECT Table 取得舊資料 -> 與本回合的資料做比對 -> 將重複出現的data進行update (之前已經有在log裡的data)
                  ->  將新的data加入log裡 -> 刪除本次SELECT出來的log紀錄 -> 將log紀錄寫回table -> 完成更新
        """
        # 與本回合的資料做比對，left_join old_log，配上的代表有變動，就進行後續更新
        updated_df = old_log.merge(new_log, how='left'
                                   , left_on=['platform', 'site_code', 'game_code', 'level', 'report_class', 'assignee',
                                              'freq_type', 'gte_time', 'lt_time']
                                   ,
                                   right_on=['platform', 'site_code', 'game_code', 'level', 'report_class', 'assignee',
                                             'freq_type', 'gte_time', 'lt_time']
                                   , suffixes=('', '_new'))

        # 將重複出現的data進行update (代表dependency的count數字有變動)
        updated_df['dep_count'] = updated_df['dep_count_new']
        updated_df['matched'] = updated_df['matched_new']
        updated_df.drop(columns=['dep_count_new', 'matched_new'], inplace=True)

        # 新的matched為0的log
        new_not_matched = new_log[new_log['matched'] == 0]

        # 比對後找出需要append的資料 (concat後去重，取差集)
        tdf = pd.concat([updated_df, new_not_matched]) \
            .drop_duplicates(keep=False, subset=['platform', 'site_code', 'game_code', 'assignee', 'gte_time'])
        need_to_append_new_not_matched = tdf[tdf['matched'] == 0]

        # 將新log append上去
        updated_not_matched = pd.concat([updated_df, need_to_append_new_not_matched])

        return updated_not_matched

    @staticmethod
    def delete_old_not_matched_log(old_df, conn):
        """
        SELECT那些資料就DELETE那些資料，更新完再插入

        兩個為一組，處理dep問題
        GetTaskDepCount()
        FilterNotMatched()
        
        :param old_df: SELECT出的舊資料
        :param conn: dbconn
        :return: None
        """
        table_name = 'dep_not_matched_log'
        for i, row in old_df.iterrows():
            sql = f"DELETE FROM {table_name}" \
                  f" WHERE platform = '{row['platform']}' AND site_code = '{row['site_code']}' AND game_code = '{row['game_code']}'" \
                  f" AND report_class = '{row['report_class']}' AND assignee = '{row['assignee']}' AND freq_type = '{row['freq_type']}'" \
                  f" AND gte_time = '{row['gte_time']}' AND lt_time = '{row['lt_time']}'"
            conn.execute(sql)

    @staticmethod
    def produce_not_matched_log(not_matched_df, conn):
        """
        將update後的資料寫回去

        :param updated_not_matched_df: update完的資料
        :param conn: dbconn
        :return: None
        """
        table_name = 'dep_not_matched_log'

        not_matched_df.to_sql(table_name, conn, if_exists='append', index=False)

    # @staticmethod
    # def dep_not_matched_alert(not_matched_df):
    #     for i, row in not_matched_df.iterrows():
    #         msg = f"***** DEP_NOT_MATCHED_ALERT *****\n" \
    #                      f"platform: {row['platform']} | site_code: {row['site_code']} | game_code: {row['game_code']}\n" \
    #                      f"assignee: {row['assignee']}\n" \
    #                      f"{row['gte_time']} to {row['lt_time']}\n"
    #                      f"dep_count: {row['dep_count']}"
    #
    #         # TG發訊
    #         TGMessage.send_msg_to_tg(msg)
    #
    #         time.sleep(0.1)
