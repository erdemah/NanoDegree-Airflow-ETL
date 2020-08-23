from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'
    
    load_fact = """
        INSERT INTO {}
        {}
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 create_table="",
                 load_table="",
                 truncate_insert=True,
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.create_table = create_table
        self.load_table = load_table
        self.truncate_insert = truncate_insert

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        if self.truncate_insert:
            self.log.info(f"Dropping table {self.table}")
            redshift.run(f"DROP TABLE IF EXISTS {self.table}")
        
        self.log.info(f"Creating fact table {self.table}")
        redshift.run(self.create_table)
        self.log.info(f"Loading fact table {self.table}")
        redshift.run(LoadFactOperator.load_fact.format(self.table, self.load_table))
        
