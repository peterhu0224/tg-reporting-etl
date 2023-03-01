from utils.PipelineUtils import ProcessStep


class ScanPlaySummaryTask(ProcessStep):
    @classmethod
    def process(cls, data, utils):
        exec_utils = utils['exec_utils']
        db_utils = utils['db_utils']
        report_class = 'player_summary'

        with db_utils.get_task_db_maria_conn() as conn:
            player_summary_task = exec_utils.scan_task_board(report_class, conn)

        player_summary_task.sort_values(by=['level', 'gte_time'], ascending=True, inplace=True)
        return player_summary_task
