from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'
    
    test_pk_songplays = """
    select count(*) from public.songplays 
    where playid is null
    """
    test_pk_songs = """
    select count(*) from public.songs
    where songid is null
    """
    test_pk_artists = """
    select count(*) from public.artists
    where artistid is null
    """
    test_pk_users = """
    select count(*) from public.users
    where userid is null
    """
    test_pk_time = """
    select count(*) from public.time_table
    where start_time is null
    """
    test_list = [test_pk_songplays, test_pk_songs, test_pk_artists, test_pk_users, test_pk_time]

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        
        self.redshift_conn_id = redshift_conn_id

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        for test in DataQualityOperator.test_list:
            count_result = redshift.get_records(test)[0][0]
            self.log.info(f"count result is {count_result}")
#             self.log.info(f"count result is v {count_result[0]}")
#             self.log.info(f"count result is v2 {count_result[0][0]}")
            self.log.info("check if there are null values in primary key")
            if count_result != 0:
                raise ValueError(f"The test {test} failed due to having null values in primary key.")
            else:
                self.log.info(f"The test {test} has passed.")
        
        
        
        
        