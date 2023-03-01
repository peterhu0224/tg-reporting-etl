import pandas as pd
from pandas.tseries.offsets import MonthEnd

from utils.PipelineUtils import ProcessStep
import task_config


class GetTaskDepCount(ProcessStep):
    @classmethod
    def process(cls, data, utils):
        """
        接收GetNewTaskList傳進來的data(new_task_list)，進行dependency的檢查

        兩個為一組，處理dep問題
        GetTaskDepCount()
        FilterNotMatched()

        :param data: DataFrame
        :param utils: main impl的工具包
        :return:
        """
        with utils['db_utils'].get_task_db_maria_conn() as conn:
            counted_df = cls.get_dep_count(data, conn)

        return counted_df

    @staticmethod
    def get_dep_count(data, conn):

        # 5min為最基本單位，沒有dep所以全部不檢查
        tenmin = data[data['freq_type'] == '5min'].copy()
        # 不需要檢查的task
        pass_dep = data[data['assignee'].isin(task_config.PASS_DEP_CHECK_LIST)].copy()
        # 其他的進迴圈
        check_dep = pd.concat([data, tenmin, pass_dep]).drop_duplicates(keep=False)

        # loop final_task_list，把dependency的count加上，最後再篩選出可放行與需要警報的
        counted_df = pd.DataFrame()
        for index, row in check_dep.iterrows():

            dependency_freq_type = None
            # 1H的看前一小時的5min是否全部完成，count = 12 為完成
            if row['freq_type'] == '1H':
                dependency_freq_type = '5min'
            # 1D的看昨日的1H是否全部完成，count = 24 為完成
            if row['freq_type'] == '1D':
                dependency_freq_type = '1H'
            # 1M的看上月的1D是否全部完成，count = 30為完成 (要當月有幾天)
            if row['freq_type'] == '1M':
                dependency_freq_type = '1D'

            # dependency source table
            table_name = 'task_board'
            # get dependency count
            sql = f"SELECT COUNT(*)" \
                  f" FROM {table_name}" \
                  f" WHERE done = 1 AND platform = '{row['platform']}' AND site_code = '{row['site_code']}' AND game_code = '{row['game_code']}'" \
                  f" AND report_class = '{row['report_class']}' AND freq_type = '{dependency_freq_type}'" \
                  f" AND gte_time >= '{row['gte_time']}' AND lt_time <= '{row['lt_time']}'"

            # 寫入dependency count
            dep_count = pd.read_sql(sql, conn).iloc[0].values[0]
            row['dep_count'] = dep_count
            # 本次循環結束，將row加回上
            counted_df = pd.concat([counted_df, row], axis=1)

        counted_df = counted_df.T

        # 將直接pass的task進行組合
        pass_dep_task = pd.concat([pass_dep, tenmin])
        pass_dep_task['matched'] = 1
        pass_dep_task['dep_count'] = 0

        # 若counted_df為空就直接將可pass的資料拋出
        if len(counted_df.index) == 0:
            return pass_dep_task

        # 若有資料就組合checked_df
        counted_df['matched'] = 0
        counted_df = pd.concat([counted_df, pass_dep_task])

        # 最後開始標記已完成的task
        # 1H
        counted_df.loc[(counted_df['dep_count'] == 12) & (counted_df['freq_type'] == '1H'), 'matched'] = 1
        # 1D
        counted_df.loc[(counted_df['dep_count'] == 24) & (counted_df['freq_type'] == '1D'), 'matched'] = 1
        # 1M (將gte_time取出，轉換成當月最後一天，再跟dependency_count配對，如果相等就給1)
        counted_df.loc[(counted_df['dep_count']
                        == counted_df['gte_time'].apply(lambda x: pd.to_datetime(str(x)[0:7]) + MonthEnd(0)).dt.day)
                       & (counted_df['freq_type'] == '1M'), 'matched'] = 1

        return counted_df
