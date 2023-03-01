import pandas as pd
from monitor.TGMessage import TGMessage
from utils.DBUtils import DBUtils
from report_config import tg_message_title


class ErrorHandler:

    def __init__(self):
        pass

    # @staticmethod
    # def raise_error_to_db(error_dict):
    #     error_df = pd.DataFrame(error_dict.items()).T
    #     error_df.columns = error_df.loc[0]
    #     error_df.drop([0], inplace=True)
    #
    #     with DBUtils().get_task_db_maria_conn() as conn:
    #         target_table = 'error_log'
    #         error_df.to_sql(target_table, conn, if_exists='append', index=False)

    @staticmethod
    def send_alert_msg(error_dict):
        msg = f"***** {tg_message_title} TaskExecutor Error *****\n" \
              f"pipeline: {error_dict['pipeline']}\n" \
              f"process: {error_dict['process']}\n" \
              f"content: {error_dict['content']}\n\n" \
              f"***** TRACEBACK ***** \n" \
              f"{error_dict['traceback']}"

        TGMessage.send_msg_to_tg(msg)

    @staticmethod
    def send_customize_msg(text):
        TGMessage.send_msg_to_tg(text)
