from datetime import datetime, timedelta
from util import getSqlFiles, getMysqlConnector
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
sqlFiles = getSqlFiles()

# see if anything went wrong with the foreign keys when renaming everything
# select *
# FROM
# INFORMATION_SCHEMA.KEY_COLUMN_USAGE
# WHERE
# REFERENCED_TABLE_SCHEMA = 'tcrdinfinity';

def takeTCRDSnapshot():
    newName = Variable.get('NewTCRDName')
    oldName = 'tcrdinfinity'

    mysqlserver = getMysqlConnector()
    mysqlserver.run(f"""CREATE SCHEMA IF NOT EXISTS `{newName}`;""")
    sqlString = sqlFiles['snapshot.sql'].replace('$1', oldName, 10).replace('$2', newName, 10)
    data = mysqlserver.get_records(sqlString)
    sqlCommand = ''.join(map(lambda x: x[0], data))
    mysqlserver.run(sqlCommand)

with DAG(
        'snapshot-tcrdinfinity',
        default_args={
            'depends_on_past': False,
            'email': ['keith.kelleher@ncats.nih.gov'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 0,
            'retry_delay': timedelta(minutes=5),
        },
        description='Build\'s the TCRD schema and base tables',
        schedule_interval=None,
        start_date=datetime(2022, 4, 1),
        catchup=False,
        tags=['TCRD', 'Pharos', 'IDG'],
) as dag:

    snapshotTCRD = PythonOperator(
        dag=dag,
        task_id='snapshotTCRD',
        python_callable=takeTCRDSnapshot
    )