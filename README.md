# NanoDegree-Airflow-ETL
<p>This project introduces the use of Airflow for orchestration of common ETL jobs. We have two different data sources residing S3 as a json format: <p>
<p>Log data: s3://udacity-dend/log_data </p>
<p>Song data: s3://udacity-dend/song_data </p>
<p> The data is taken to a Redshift cluster in AWS using star schema modeling. Using airflow, we achieved to process the data partition by partition in the Redshift cluster using Furthermore, using the custom Airflow operators, we have a more granular observation of our ETL task. This helps us detect the problem -if any- on the spot considering some ETL jobs can be quite complex. There is also a data quality check in Airflow which we can run on each partitioned data.
