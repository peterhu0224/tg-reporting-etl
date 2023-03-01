from utils.PipelineUtils import ProcessStep


class ScanRiskCtrlTask(ProcessStep):
    @classmethod
    def process(cls, data, utils):
        exec_utils = utils['exec_utils']
        db_utils = utils['db_utils']
        report_class = 'risk_ctrl'

        with db_utils.get_task_db_maria_conn() as conn:
            risk_ctrl_task = exec_utils.scan_task_board(report_class, conn)

        risk_ctrl_task.sort_values(by=['level', 'gte_time'], ascending=True, inplace=True)
        return risk_ctrl_task
