from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'
    

    

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 test=[],
                 result=[],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        
        self.redshift_conn_id = redshift_conn_id
        self.test = test
        self.result= result

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        count_test = len(self.test)
        for i in range(count_test):
            count_result = redshift.get_records(self.test[i])[0][0]
            self.log.info(f"count result is {count_result}")
            self.log.info("check if there are null values in primary key")
            if count_result != self.result[i]:
                raise ValueError(f"The test {self.test[i]} failed due to having null values in primary key.")
            else:
                self.log.info(f"The test {self.test[i]} has passed.")
        
        
        
        
        