from utils.PipelineUtils import ProcessStep


class ScanNewRegisterSummaryTask(ProcessStep):
    @classmethod
    def process(cls, data, utils):
        exec_utils = utils['exec_utils']
        db_utils = utils['db_utils']
        report_class = 'new_register_summary'

        with db_utils.get_task_db_maria_conn() as conn:
            new_register_summary_task = exec_utils.scan_task_board(report_class, conn)

        new_register_summary_task.sort_values(by=['level', 'gte_time'], ascending=True, inplace=True)
        return new_register_summary_task
