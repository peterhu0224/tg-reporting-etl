"""
env setting
"""
# run_type = 'run_schedule' 按照schedule排程執行
# run_type = 'run_once' 全部跑一次並結束
run_type = 'run_once'

# Telegram message title
tg_message_title = 'OA'

# timezone True = UTC+0
utc_timezone_switch = False


"""
可直接放行的assignee(不需檢查dependency)
"""
PASS_DEP_CHECK_LIST = ['another_1d', 'new_register_summary_1d']

"""
即時更新的assignee，在old_task_formatted取到datetime.now().ceil(freq=時間單位)

說明：
在GetNewTaskList中產生新task時，lt_time是取到datetime.now()，因此時候未到的task不會發布，
例如前一次1H的任務區間為gte 09:00 lt 10:00，若now() 10:05，則gte 10:00 lt 11:00的任務不能發布，
因為當小時只過了5分鐘，尚有55分鐘。
但有部分task採update的方式，只要時間踩上去就必須開始算，因此以前一個例子來說，
10:05就必須發布gte=10:00 lt=11:00的任務了
"""
REALTIME_TASK_LIST = ['new_register_summary_1d']


