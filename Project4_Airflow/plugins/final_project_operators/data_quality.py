from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class DataQualityOperator(BaseOperator):
    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id,
                 dq_checks: list,
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.dq_checks = dq_checks

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        if not len(self.dq_checks):
            self.log.info("Don't have any quality checks")
            return

        for check in self.dq_checks:
            test_sql = check['test_sql']
            expected_result = check['expected_result']

            records = redshift.get_records(test_sql)
            if not expected_result == records[0][0]:
                raise ValueError(f"Data quality check #{i} failed.")

        self.log.info(f"Data quality checks already done")
