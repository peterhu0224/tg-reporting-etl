import pandas as pd

from utils.DBUtils import DBUtils
from utils.TaskUtils import TaskUtils
from initialize import init_config


def get_date_structure(date_str):
    """
    py Script，可直接執行，僅供初始化使用，

    :param date_str:初始化時間
    :param without_freq_type:不包含的清單 (有些任務可能只有H一種freq)
    :return:DataFrame
    """
    date = pd.to_datetime(date_str)
    fivemin_endtime = date + pd.Timedelta(5, "min")
    hour_endtime = date + pd.Timedelta(1, "H")
    day_endtime = date + pd.Timedelta(1, "D")

    # 因為後續的邏輯都是取最後一次的task執行時間算到今天
    # 所以月份初始化要寫入上個月日期，因為會沒數據算出數字會是0，完成初始化
    curr_month_str = date_str.split(" ")[0][0:7]
    curr_month = pd.to_datetime(curr_month_str)
    # 取輸入日期取到當月，如10/25取到10/01，減一天後得09/30 00:00:00，轉成str取09/30，在取當月得09/01
    last_month = pd.to_datetime(str(pd.to_datetime(curr_month_str) - pd.Timedelta(1, "D")).split(" ")[0][0:7])

    # 組合數據
    freq_type_s = pd.Series(["5min", "1H", "1D", "1M"])
    gte_time_s = pd.Series([date, date, date, last_month])
    lt_time_s = pd.Series([fivemin_endtime, hour_endtime, day_endtime, curr_month])
    date_structure = pd.DataFrame({"freq_type": freq_type_s, "gte_time": gte_time_s, "lt_time": lt_time_s})

    """ 當前數據格式
              freq_type  gte_time    lt_time
        0      5min      2022-10-15  2022-10-15 00:05:00
        1        1H      2022-10-15  2022-10-15 01:00:00
        2        1D      2022-10-15  2022-10-16 00:00:00
        3        1M      2022-09-01  2022-10-01 00:00:00
    """

    return date_structure


def get_init_task_list(date_structure, report_info, platform_info):
    report_df = pd.DataFrame()

    # 從report_info拆解init task，直接組成一個df
    for k, v in report_info.items():
        tdf = pd.json_normalize(report_info[k])
        tdf['report_class'] = k
        report_df = pd.concat([report_df, tdf])

    init_task_list = report_df.merge(date_structure, how='left', left_on='freq_type', right_on='freq_type')

    # 填充欄位
    init_task_list["platform"] = platform_info['platform']
    init_task_list["site_code"] = platform_info['site_code']
    init_task_list["game_code"] = platform_info['game_code']
    init_task_list["done"] = 0

    """ 當前欄位:
        ['freq_type', 'assignee', 'gte_time', 'lt_time', 'platform', 'site_code', 'game_code', 'report_class', 'level']
    """
    return init_task_list


def monthly_dep_init(init_task_list):
    """
    解決初始化日期可能導致M無法正常通過dpe檢查的問題
    例如初始化為10/25，但11/01要產出10月份M報表時，dep_count只會有6 (10/25~10/31)
    因此先把前面的D全部補齊，並且done=1，不實際執行

    :param init_task_list: 初始化任務
    :return: init_task_list
    # TODO:2022/12/12改為即時報表，因此dependency暫時不需要了，未來明確定案後再移除
    """
    # 檢查是否有M的任務，這裡預預設每個task有M就有D
    monthly_task = init_task_list[init_task_list['freq_type'] == '1M']
    if len(monthly_task.index) == 0:
        return
    # self join 在把D找出來，把M的結束日期作為D的起始日期，D的起始日期為D的結束日期，完成補全
    tdf = init_task_list.merge(init_task_list, how='left', left_on='report_class', right_on='report_class',
                               suffixes=['', '_M'])
    dep_task = tdf[(tdf['freq_type'] == '1D') & (tdf['freq_type_M'] == '1M')]
    dep_task['lt_time'] = dep_task['gte_time']
    dep_task['gte_time'] = dep_task['lt_time_M']

    dep_task = dep_task[[
        'freq_type', 'assignee', 'gte_time', 'lt_time', 'platform', 'site_code', 'game_code', 'report_class', 'level']]

    dep_task = dep_task[dep_task['gte_time'] != dep_task['lt_time']]
    # 使用TaskUtils擴展task
    dep_task_list = TaskUtils.get_task_list_by_gte_lt(dep_task)
    dep_task_list['done'] = 1

    return dep_task_list


def main():
    report_info = init_config.current_init
    platform_info = init_config.platform_init

    # 產出init_task_data
    date_structure = get_date_structure(platform_info["init_start_date"])
    init_task_list = get_init_task_list(date_structure, report_info, platform_info)

    # TODO:2022/12/12改為即時報表，因此dependency暫時不需要了，未來明確定案後再移除
    # init_task_list = pd.concat([init_task_list, monthly_dep_init(init_task_list)])

    # 要初始化的target table
    init_table = 'task_board'

    # 寫入init_task_data
    try:
        with DBUtils.get_task_db_maria_conn() as conn:
            init_task_list.to_sql(init_table, conn, if_exists='append', index=False)

    except Exception as e:
        print(e)
        raise


if __name__ == '__main__':
    main()
