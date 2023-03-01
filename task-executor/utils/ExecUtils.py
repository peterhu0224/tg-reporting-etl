import datetime
import pandas as pd
import re
import logging
from logging.handlers import TimedRotatingFileHandler


class ExecUtils:

    @staticmethod
    def scan_task_board(report_class, conn):
        task_sql = f"SELECT level, platform, site_code, game_code, report_class" \
                   f", assignee, freq_type, gte_time, lt_time, apply_time, complete_time" \
                   f", runtime_second, retry, done" \
                   f" FROM task_board WHERE report_class = '{report_class}' AND done = 0"

        rerun_sql = f"SELECT level, platform, site_code, game_code, report_class" \
                    f", assignee, freq_type, gte_time, lt_time, apply_time, complete_time" \
                    f", runtime_second, retry, done" \
                    f" FROM rerun_board WHERE report_class = '{report_class}' AND done = 0"

        # 一次性把task跟rerun select出來一起做
        task_list = pd.read_sql(task_sql, conn)
        rerun_task_list = pd.read_sql(rerun_sql, conn)
        # 加上標記，以利後續的table status
        task_list['is_rerun'] = 0
        rerun_task_list['is_rerun'] = 1

        task_list = pd.concat([task_list, rerun_task_list])

        return task_list

    @staticmethod
    def update_task_apply_time(single_task, conn):
        """
        更新task的接任務時間，作為後續執行時間統計的判斷基準
        :param single_task:
        :param conn:
        :return:
        """
        # 如果是來自rerun的任務，則更新到rerun_board
        table_name = 'task_board'
        if single_task['is_rerun'] == 1:
            table_name = 'rerun_board'

        update_time = datetime.datetime.now()

        sql = f"UPDATE {table_name}" \
              f" SET apply_time = '{update_time}'" \
              f" WHERE platform = '{single_task['platform']}' AND site_code = '{single_task['site_code']}' AND game_code = '{single_task['game_code']}'" \
              f" AND report_class = '{single_task['report_class']}' AND assignee = '{single_task['assignee']}' AND freq_type = '{single_task['freq_type']}'" \
              f" AND gte_time = '{single_task['gte_time']}' AND lt_time = '{single_task['lt_time']}'"

        conn.execute(sql)

    @staticmethod
    def update_task_exec_time(single_task, conn):
        """
        更新task的執行時間
        :param single_task:
        :param conn:
        :return:
        """
        # 如果是來自rerun的任務，則更新到rerun_board
        table_name = 'task_board'
        if single_task['is_rerun'] == 1:
            table_name = 'rerun_board'

        update_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        update_cmp_time_sql = f"UPDATE {table_name}" \
                              f" SET done = 1, complete_time = '{update_time}'" \
                              f" WHERE platform = '{single_task['platform']}' AND site_code = '{single_task['site_code']}' AND game_code = '{single_task['game_code']}'" \
                              f" AND report_class = '{single_task['report_class']}' AND assignee = '{single_task['assignee']}' AND freq_type = '{single_task['freq_type']}'" \
                              f" AND gte_time = '{single_task['gte_time']}' AND lt_time = '{single_task['lt_time']}'"

        update_diff_time_sql = f"UPDATE {table_name}" \
                               f" SET runtime_second = TIMESTAMPDIFF(SECOND, apply_time, '{update_time}')" \
                               f" WHERE platform = '{single_task['platform']}' AND site_code = '{single_task['site_code']}' AND game_code = '{single_task['game_code']}'" \
                               f" AND report_class = '{single_task['report_class']}' AND assignee = '{single_task['assignee']}' AND freq_type = '{single_task['freq_type']}'" \
                               f" AND gte_time = '{single_task['gte_time']}' AND lt_time = '{single_task['lt_time']}'"

        conn.execute(update_cmp_time_sql)
        conn.execute(update_diff_time_sql)

    @staticmethod
    def get_report_logger():
        logger = logging.getLogger('report_app')
        logger.setLevel(logging.INFO)
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(funcName)s - %(message)s')

        log_handler = TimedRotatingFileHandler('TaskExecutor_normal.log', when='midnight', interval=1)
        log_handler.setLevel(logging.INFO)
        log_handler.setFormatter(formatter)

        errorlog_handler = TimedRotatingFileHandler('TaskExecutor_error.log', when='midnight', interval=1)
        errorlog_handler.setLevel(logging.ERROR)
        errorlog_handler.setFormatter(formatter)

        log_handler.suffix = "%Y%m%d"
        errorlog_handler.suffix = "%Y%m%d"
        log_handler.extMatch = re.compile(r"^\d{8}$")
        errorlog_handler.extMatch = re.compile(r"^\d{8}$")

        logger.addHandler(log_handler)
        logger.addHandler(errorlog_handler)

        return logger

    # @staticmethod
    # def customize_retry(func, title, tries, logger):
    #     result = None
    #     for i in range(1, tries + 1):
    #         try:
    #             result = func
    #         except Exception as e:
    #             logger.error(f"{title} error, retrying: {i}")
    #             time.sleep(2)
    #             continue
    #         break
    #     return result
