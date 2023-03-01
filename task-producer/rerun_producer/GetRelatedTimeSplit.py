from datetime import timedelta, datetime

import pandas as pd
from utils.PipelineUtils import ProcessStep
from pandas.tseries.offsets import MonthEnd


class GetRelatedTimeSplit(ProcessStep):

    @classmethod
    def process(cls, data, utils):

        db_utils = utils['db_utils']

        with db_utils.get_task_db_maria_conn() as conn:
            related_task_list = pd.DataFrame()

            for i, row in data.iterrows():
                cls.update_rerun_apply_time(row, conn)
                related_task = cls.get_related_task(row)
                related_task_list = pd.concat([related_task_list, related_task])

            return related_task_list

    @staticmethod
    def get_related_task(single_task):

        if len(single_task.index) == 0:
            return pd.DataFrame()

        task_series = None

        if single_task['5min'] == 1:
            temp = single_task.copy()
            temp['assignee'] = temp['report_class'] + '_5min'
            temp['freq_type'] = '5min'
            temp['level'] = 100
            task_series = pd.concat([temp, task_series], axis=1)

        if single_task['1h'] == 1:
            temp = single_task.copy()
            temp['assignee'] = temp['report_class'] + '_1h'
            temp['freq_type'] = '1H'
            temp['level'] = 200
            temp['gte_time'] = temp['gte_time'].floor("H")
            temp['lt_time'] = temp['lt_time'].ceil("H")
            task_series = pd.concat([temp, task_series], axis=1)

        if single_task['1d'] == 1:
            temp = single_task.copy()
            temp['assignee'] = temp['report_class'] + '_1d'
            temp['freq_type'] = '1D'
            temp['level'] = 300
            temp['gte_time'] = temp['gte_time'].floor("D")
            temp['lt_time'] = temp['lt_time'].ceil("D")
            task_series = pd.concat([temp, task_series], axis=1)

        if single_task['1m'] == 1:
            temp = single_task.copy()
            temp['assignee'] = temp['report_class'] + '_1m'
            temp['freq_type'] = '1M'
            temp['level'] = 400
            temp['gte_time'] = pd.to_datetime(str(temp['gte_time']).split(" ")[0][0:7])

            # 跨月整點，不影響次月數據，月份不進位 (例如10/29 23:00:00 ~ 11/01 00:00:00，M報表只需跑GTE:10/01~LT:11/01
            date_str = str(temp['lt_time']).split(" ")[0][8:10]
            time_str = str(temp['lt_time']).split(" ")[1]
            if date_str + time_str == '0100:00:00':
                temp['lt_time'] = pd.to_datetime(str(temp['lt_time']).split(" ")[0][0:7])
            else:
                # 跨越非整點，月份要進位 (例如~11/01 01:30:00，11當月就需要重跑，因此要進位，跑到LT:12/01)
                temp['lt_time'] = pd.to_datetime(
                    str(temp['lt_time']).split(" ")[0][0:7]) + MonthEnd(0) + timedelta(days=1)

            task_series = pd.concat([temp, task_series], axis=1)

        task_list = pd.DataFrame(task_series).T

        task_list.drop(columns=['5min', '1h', '1d', '1m'], inplace=True)

        # 若gte==lt，則忽略(發生在rerun未跨時間單位的情況，就是rerun的部分時間跨度太短，後面大單位的時間不需要重跑)
        split_task = task_list[task_list['gte_time'] != task_list['lt_time']]

        return split_task

    @staticmethod
    def update_rerun_apply_time(single_task, conn):
        update_time = datetime.now()

        sql = f"UPDATE rerun_board_manually_insert" \
              f" SET apply_time = '{update_time}', done = 1" \
              f" WHERE platform = '{single_task['platform']}' AND site_code = '{single_task['site_code']}' AND game_code = '{single_task['game_code']}'" \
              f" AND report_class = '{single_task['report_class']}' AND gte_time = '{single_task['gte_time']}' AND lt_time = '{single_task['lt_time']}'" \
              f" AND 5min = '{single_task['5min']}' AND 1h = '{single_task['1h']}' AND 1d = '{single_task['1d']}' AND 1m = '{single_task['1m']}'"

        conn.execute(sql)
