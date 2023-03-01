import pandas as pd


class RerunUtils:

    @staticmethod
    def scan_rerun_manually_assign(dbconn):
        """
        每分鐘掃描一次是否有手動插入需要重跑的資料

        一次掃整張表，再丟給create_related_date批次處理

        :param dbconn: dbconn
        :return: manually_assign_df

        """
        sql = "SELECT level, platform, site_code, game_code, report_class," \
              "assignee, freq_type, gte_time, lt_time, status" \
              " FROM rerun_manually_assign WHERE status = 0"
        df = pd.read_sql(sql, dbconn)

        return df

    @staticmethod
    def manual_time_related_split(manually_assign_df):

        """
        目前淘汰不使用
        """

        """
        接收 rerun_manually_assign dbtable的資料，進行分裂，產生相互關聯的日期

        !!!注意：
        input只能是最小單位的10min，這支程式會自動往後找關聯，把後續關連到，需要重跑的日期一起寫上task

        :param manually_assign_df:
        :return:DataFrame (分裂後的rerun task list，需要在向後丟給time_structure做處理)

        rerun_manually_assign 的資料範例：
           level  platform site_code  ...       gte_time              lt_time              status
        0   1000  platform01  sitecode01  ...   2022-10-20 23:30:00   2022-10-21 00:10:00    0
        1   1000  platform01  sitecode01  ...   2022-10-21 11:00:00   2022-10-21 11:30:00    0

        return 範例： (已把gte_time==lt_time的部分(不需要做的)拿掉)
          level  platform    site_code  ...  gte_time             lt_time                status
        0  1000  platform01  sitecode01  ... 2022-10-20 23:30:00  2022-10-21 00:10:00      0
        0  1000  platform01  sitecode01  ... 2022-10-20 23:00:00  2022-10-21 00:00:00      0
        0  1000  platform01  sitecode01  ... 2022-10-20 00:00:00  2022-10-21 00:00:00      0
        1  1000  platform01  sitecode01  ... 2022-10-21 11:00:00  2022-10-21 11:30:00      0

        """
        # 批次處理，全部整理到一個outer_temp中，再一起return
        outer_temp = pd.DataFrame()

        # iterrows()會把df解成series，方便取值
        for i, min_row in manually_assign_df.iterrows():
            # 內部需要df.T轉置，要先存一個df，才能跟外面的outer_temp做concat
            temp = pd.DataFrame()

            # manually_assign_df進來的row應為5min為單位的Data
            # 需要向後分解5min, 1H, 1D, 1M(共複製四份)
            H_row = min_row.copy()
            D_row = min_row.copy()
            M_row = min_row.copy()

            # 複製後改data
            H_row.update(pd.Series(['1H', H_row['report_class'] + '_1H'
                                        , H_row['gte_time'].floor("H")
                                        , H_row['lt_time'].floor("H")]
                                        , index=['freq_type', 'assignee', 'gte_time', 'lt_time']))
            D_row.update(pd.Series(['1D', D_row['report_class'] + '_1D'
                                        , D_row['gte_time'].floor("D")
                                        , D_row['lt_time'].floor("D")]
                                        , index=['freq_type', 'assignee', 'gte_time', 'lt_time']))
            # M沒有floor()可用，因此使用轉自串再轉回的方式取到當月(如2022/10/20只取2022/10，再轉回date變成2022/10/01)
            M_row.update(pd.Series(['1M', M_row['report_class'] + '_1M'
                                        , pd.to_datetime(str(M_row['gte_time']).split(" ")[0][0:7])
                                        , pd.to_datetime(str(M_row['lt_time']).split(" ")[0][0:7])]
                                        , index=['freq_type', 'assignee', 'gte_time', 'lt_time']))

            # series concat後columns會在indeex，需要df.T轉置
            temp = pd.concat([temp, min_row, H_row, D_row, M_row], axis=1).T
            # 把結果加到outer_temp
            outer_temp = pd.concat([outer_temp, temp])

        # 若gte==lt，則忽略(發生在rerun未跨時間單位的情況，就是rerun的部分時間跨度太短，後面大單位的時間不需要重跑)
        splited_manually_task = outer_temp[outer_temp['gte_time'] != outer_temp['lt_time']]

        return splited_manually_task
