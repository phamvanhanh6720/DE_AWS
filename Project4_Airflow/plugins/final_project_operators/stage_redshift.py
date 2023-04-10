from airflow.contrib.hooks.aws_hook import AwsHook

from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'

    copy_sql = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        json 'auto'
        region '{}';
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id,
                 aws_conn_id,
                 table,
                 s3_bucket,
                 s3_key,
                 region='us-east-1',
                 *args, **kwargs):
        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)

        self.redshift_conn_id = redshift_conn_id
        self.aws_conn_id = aws_conn_id
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.region = region

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        aws_hook = AwsHook(aws_conn_id=self.aws_conn_id)
        credentials = aws_hook.get_credentials()

        self.log.info('Copy data from S3 to Redshift')
        s3_path = f's3://{self.s3_bucket}/{self.s3_key}'

        redshift.run("DELETE FROM {}".format(self.table))

        formatted_sql = StageToRedshiftOperator.copy_sql.format(
            self.table,
            s3_path,
            credentials.access_key,
            credentials.secret_key,
            self.region
        )

        redshift.run(formatted_sql)

        self.log.info(f"Success: Copy {self.table} from S3 to Redshift")




