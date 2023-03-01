import time
import schedule
from datetime import datetime
import os
from functools import wraps

from utils.DBUtils import DBUtils
from utils.TaskUtils import TaskUtils
from utils.PipelineUtils import ProcessPipeline

from task_producer.ScanTaskBoard import ScanTaskBoard
from task_producer.GetNewTaskList import GetNewTaskList
from task_producer.FilterDisabled import FilterDisabled
from task_producer.ProduceNewTask import ProduceNewTask

from rerun_producer.ScanRerunManually import ScanRerunManually
from rerun_producer.GetRelatedTimeSplit import GetRelatedTimeSplit
from rerun_producer.GetRerunTaskList import GetRerunTaskList
from rerun_producer.ProduceRerunTask import ProduceRerunTask

from monitor.ErrorHandler import ErrorHandler

from task_config import run_type, tg_message_title, utc_timezone_switch

# 設定系統時間
if utc_timezone_switch:
    os.environ["TZ"] = "UTC"
    time.tzset()

# 設定logger
logger = TaskUtils.get_report_logger()
logger.propagate = False

# 初始化工具包
utils = {
    'db_utils': DBUtils(),
    'task_utils': TaskUtils(),
    'logger': logger
}


def log(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        try:
            start_time = time.perf_counter()
            result = func(*args, **kwargs)
            end_time = time.perf_counter()
            total_time = end_time - start_time
            message = f'{datetime.now()} : Function {func.__name__}{args} Took {total_time:.4f} seconds'
            print(message)
            logger.info(message)
            return result
        except Exception as e:
            logger.exception(f"Exception raised in {func.__name__}. exception: {str(e)}")
            raise e

    return wrapper


@log
def task_producer_start():
    task_producer_steps = [
        ScanTaskBoard(),
        GetNewTaskList(),
        FilterDisabled(),
        ProduceNewTask()
    ]

    task_pipline = ProcessPipeline(task_producer_steps)
    task_pipline.run(utils)


@log
def rerun_start():
    rerun_steps = [
        ScanRerunManually(),
        GetRelatedTimeSplit(),
        GetRerunTaskList(),
        ProduceRerunTask()
    ]

    rerun_pipline = ProcessPipeline(rerun_steps)
    rerun_pipline.run(utils)


@log
def monitor_start():
    monitor_steps = [
    ]

    monitor_pipeline = ProcessPipeline(monitor_steps)
    monitor_pipeline.run(utils)


def heartbeat():
    ErrorHandler.send_customize_msg(
        f"{tg_message_title} TaskProducer heartbeat {datetime.now().strftime('%Y/%m/%d %H:%M:%S')}")


def job_setting():
    schedule.every().hour.at(":00").do(task_producer_start)
    schedule.every().hour.at(":05").do(task_producer_start)
    schedule.every().hour.at(":10").do(task_producer_start)
    schedule.every().hour.at(":15").do(task_producer_start)
    schedule.every().hour.at(":20").do(task_producer_start)
    schedule.every().hour.at(":25").do(task_producer_start)
    schedule.every().hour.at(":30").do(task_producer_start)
    schedule.every().hour.at(":35").do(task_producer_start)
    schedule.every().hour.at(":40").do(task_producer_start)
    schedule.every().hour.at(":45").do(task_producer_start)
    schedule.every().hour.at(":50").do(task_producer_start)
    schedule.every().hour.at(":55").do(task_producer_start)

    schedule.every(1).minutes.do(rerun_start)

    # 5min 監控
    # TODO: 可能要監控task運行時間是否超過域值，但暫時還不需要
    # schedule.every(5).minute.do(monitor_start)

    # heartbeat
    schedule.every().hour.at(":00").do(heartbeat)


if __name__ == '__main__':
    if run_type == 'run_once':
        task_producer_start()
        rerun_start()

    if run_type == 'run_schedule':
        job_setting()
        while True:
            schedule.run_pending()
            time.sleep(5)
