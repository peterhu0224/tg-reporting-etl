from utils.PipelineUtils import ProcessStep


class ProduceNewTask(ProcessStep):
    @staticmethod
    def process(data, utils):
        db_utils = utils['db_utils']

        columns_mapping = ['level', 'platform', 'site_code', 'game_code', 'report_class',
                           'assignee', 'freq_type', 'gte_time', 'lt_time'
                           ]

        data = data[columns_mapping]

        try:
            with db_utils.get_task_db_maria_conn() as conn:
                target_table = 'task_board'
                data.to_sql(target_table, conn, if_exists='append', index=False)
        except Exception as e:
            print(e)
            raise
