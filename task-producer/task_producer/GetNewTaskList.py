import pandas as pd
from datetime import datetime, timedelta
from pandas.tseries.offsets import MonthEnd

from utils.PipelineUtils import ProcessStep


class GetNewTaskList(ProcessStep):
    @classmethod
    def process(cls, data, utils):
        """
        接收NewTaskMeta()回傳的data，交給task_utils處理，
        產出以時間範圍圈定的，分解後的任務，例如：
        10/10 00:00:00~10/11 00:00:00，
        5min將會產出288筆，1H將會產出24筆等等

        :param data: new_task_meta
        :param utils:
        :return:
        """

        formatted = cls.old_task_formatted(data)
        new_task_list = cls.get_new_task_list(formatted, utils)

        return new_task_list

    @staticmethod
    def get_new_task_list(formatted_old_task, utils):
        task_utils = utils['task_utils']
        new_task_list = task_utils.get_task_list_by_gte_lt(formatted_old_task)
        return new_task_list

    @staticmethod
    def old_task_formatted(data):
        """
                接收ScanTaskLog()回傳的data (old_task_log)，轉換時間後拋出
                :param data: DataFrame
                :return:
        """

        # 最後執行時間若是更久之前的，會自動補齊
        # old_log的lt_time為new_task的gte_time
        data['gte_time'] = data['lt_time']
        # new_task的lt_time直接帶new()，時間切片交給TaskUtils處理
        data['lt_time'] = datetime.now()

        """
        即時更新類task，lt_time要算到當個時間粒度的ceil
        例如 當前時間2022/11/08 10:05:00 (datetime.now())
        5min 暫時不需要
        1H    lt_time = 2022/11/08 11:00:00
        1D    lt_time = 2022/11/09 00:00:00
        1M    lt_time = 2022/12/01 00:00:00
        
        2022/12/12 全部報表類均改為real time，因此也不需要REALTIME_FILTER了
        """

        # 1H 取ceil
        h_mask = data['freq_type'] == '1H'
        data.loc[h_mask, 'lt_time'] = data.loc[h_mask, 'lt_time'].apply(lambda x: x.ceil(freq='H'))

        # 1D 取ceil
        d_mask = data['freq_type'] == '1D'
        data.loc[d_mask, 'lt_time'] = data.loc[d_mask, 'lt_time'].apply(lambda x: x.ceil(freq='D'))

        # 1M取ceil
        m_mask = data['freq_type'] == '1M'
        data.loc[m_mask, 'lt_time'] = pd.to_datetime(
            (datetime.now()).strftime("%Y-%m-%d").split(" ")[0][0:7]) + MonthEnd(0) + timedelta(days=1)

        return data
