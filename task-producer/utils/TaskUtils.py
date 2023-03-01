import pandas as pd
import time
from functools import wraps
from datetime import datetime, timedelta
import logging
from logging.handlers import TimedRotatingFileHandler
import re


class TaskUtils:

    @staticmethod
    def get_task_list_by_gte_lt(meta_task_list):
        """
        meta_rerun_task進來可以直接做 (因為rerun已指定開始與結束時間)

        meta_last_task進來之前要先整理成有時間差的row (基本要與rerun相同)

        接收gte與lt有時間差的row，兩者相減後會把該時間段全部需要produce的task_row建出來，
        產出的資料可以直接produce

        備註：具備追趕進度的功能，例如init的data是2022/09/01 00:00:00，當前時間是2022/10/21 15:30:15則：
            gte = 2022/09/01 00:00:00, lt = 2022/10/21 15:30:15
            兩個時間內的所有資料都會以5min, 1H, 1D, 1M為單位進行切片，

        !!! 注意 : H、D、M只接受以10分鐘為最小單位的整點時間，如08:00:00、16:10:00
            原因 : 小時(含)以上的時間單位，若不符合10分鐘為最小單位的規範，
                    結束時間粒度會跑掉，會影響到從raw data取資料的時間篩選。
                    假設gte_time輸入2022/10/21 08:15:00，則lt_time為：
                    5min = 2022/10/21 08:20:00 (10分鐘統計應為每10分鐘一個區間，每小時12筆)
                    1H = 2022/10/21 09:15:00 (小時統計應為上小時0點到本小時0點)
                    1D = 2022/10/22 08:15:00 (日統計應為昨天0點到今天0點的時間)
                    1M = 2022/11/01 08:15:00 (月統計應為上月0點到本月0點的時間)
                    (補充：gte = grater than equal(大於等於), lt = less than(小於))
        :param meta_task_list: 一份完整的task_meta，與task_board欄位完全一樣
                                ，只是起始時間與結束時間會是一個range，不會符合5min為最小單位的時間設定，
                                因為meta_task，lt_time日期欄位帶的是now()
        :return: DataFrame (final date_format ，可以produce的task_list)

        2022/12/06 將10min改成5min

        """

        # 每次計算區間，必須把最後一次的時間移除，不然會太早算
        # 如前一次最後時間2022/10/20 17:00:00 現在時間 17:21:00
        # 5min 最後一筆時間會變成gte_time 17:20，lt_time 17:30，這樣會因為時候未到就先算而少算
        # 因此要減去一個時間單位 (如5min, 1h ...etc)

        # 批次處理，外部先準備空的df
        cmp_task_list = pd.DataFrame()
        date_format = None

        for index, row in meta_task_list.iterrows():
            # '5min', '1H', '1D'第三方包有支援，M沒支援，因此要分開做
            td = None
            if row['freq_type'] in ['5min', '1H', '1D']:
                # 取得時間單位，後續做時間偏移用
                if row['freq_type'] == '5min':
                    td = timedelta(minutes=5)
                if row['freq_type'] == '1H':
                    td = timedelta(hours=1)
                if row['freq_type'] == '1D':
                    td = timedelta(days=1)

                # 開始產生gte_time與lt_time之間的時間切片
                gte_datetime_series = pd.date_range(row['gte_time'], (row['lt_time'] - td), freq=row['freq_type'])
                date_format = pd.DataFrame(gte_datetime_series)
                date_format.rename(columns={0: "gte_time"}, inplace=True)
                # 取出的切片為開始時間gte_datetime
                # 把開始時間加1個時間單位(視freq_type而定)，作為結束時間lt_datetime
                date_format['lt_time'] = date_format['gte_time'] + td

            # freq_type為1M時，會拿到當月最後一天，如09/30
            # 將2022/09/30+1天(10/01)，作為lt_time
            # 再將20022/09/30取2022/09的字串，再轉date，可得2022/09/01，作為gte_time
            if row['freq_type'] == '1M':
                td = timedelta(days=1)
                # 注意：row['lt_time']-td，是因為10/01~10/31，freq=1M時，date_range會有output，但10/31為now，表示今天還沒結束，還不能計算當月
                gte_datetime_series = pd.date_range(row['gte_time'], row['lt_time'] - td, freq=row['freq_type'])
                date_format = pd.DataFrame(gte_datetime_series)
                date_format.rename(columns={0: "gte_time"}, inplace=True)
                # 最後一天+1天 (09/30 -> 10/01)，作為結束時間
                date_format['lt_time'] = date_format['gte_time'] + td
                # 最後一天取到當月 (09/30 -> 09/01)，作為開始時間
                date_format['gte_time'] = date_format["gte_time"].apply(
                    lambda x: pd.to_datetime(str(x).split(" ")[0][0:7]))

            # 這段必須再loop內，不要拔到外面去，因為每一個row的info不盡相同
            # 把info還原回去，讓df資料完整
            date_format['platform'] = row['platform']
            date_format['site_code'] = row['site_code']
            date_format['game_code'] = row['game_code']
            date_format['report_class'] = row['report_class']
            date_format['assignee'] = row['assignee']
            date_format['freq_type'] = row['freq_type']
            date_format['level'] = row['level']
            # date_format['create_time'] = datetime.now()

            # 這段也在loop內，每做完一個row就concat一次
            cmp_task_list = pd.concat([cmp_task_list, date_format])

        return cmp_task_list

    # @staticmethod
    # def timeit(func):
    #     @wraps(func)
    #     def timeit_wrapper(*args, **kwargs):
    #         start_time = time.perf_counter()
    #         result = func(*args, **kwargs)
    #         end_time = time.perf_counter()
    #         total_time = end_time - start_time
    #         print(f'Function {func.__name__} Took {total_time:.4f} seconds')
    #         return result
    #
    #     return timeit_wrapper

    @staticmethod
    def get_report_logger():
        logger = logging.getLogger('report_app')
        logger.setLevel(logging.INFO)
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(funcName)s - %(message)s')

        log_handler = TimedRotatingFileHandler('TaskProducer_normal.log', when='midnight', interval=1)
        log_handler.setLevel(logging.INFO)
        log_handler.setFormatter(formatter)

        errorlog_handler = TimedRotatingFileHandler('TaskProducer_error.log', when='midnight', interval=1)
        errorlog_handler.setLevel(logging.ERROR)
        errorlog_handler.setFormatter(formatter)

        log_handler.suffix = "%Y%m%d"
        errorlog_handler.suffix = "%Y%m%d"
        log_handler.extMatch = re.compile(r"^\d{8}$")
        errorlog_handler.extMatch = re.compile(r"^\d{8}$")

        logger.addHandler(log_handler)
        logger.addHandler(errorlog_handler)

        return logger
