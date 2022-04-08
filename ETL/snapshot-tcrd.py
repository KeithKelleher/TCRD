from datetime import datetime, timedelta
from sql.loadSqlFiles import getSqlFiles
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.hooks.mysql_hook import MySqlHook

sqlFiles = getSqlFiles()

def takeTCRDSnapshot():
    newName = Variable.get('NewTCRDName')
    oldName = 'tcrdinfinity'

    mysqlserver = MySqlHook('tcrdinfinity')
    sqlString = sqlFiles['snapshot.sql'].replace('$1', oldName, 10).replace('$2', newName, 10)
    data = mysqlserver.get_records(sqlString)
    sqlCommand = ''.join(map(lambda x: x[0], data))
    mysqlserver.run(sqlCommand)


with DAG(
        'snapshot-tcrdinfinity',
        # These args will get passed on to each operator
        # You can override them on a per-task basis during operator initialization
        default_args={
            'depends_on_past': False,
            'email': ['keith.kelleher@ncats.nih.gov'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 0,
            'retry_delay': timedelta(minutes=5),
            # 'queue': 'bash_queue',
            # 'pool': 'backfill',
            # 'priority_weight': 10,
            # 'end_date': datetime(2016, 1, 1),
            # 'wait_for_downstream': False,
            # 'sla': timedelta(hours=2),
            # 'execution_timeout': timedelta(seconds=300),
            # 'on_failure_callback': some_function,
            # 'on_success_callback': some_other_function,
            # 'on_retry_callback': another_function,
            # 'sla_miss_callback': yet_another_function,
            # 'trigger_rule': 'all_success'
        },
        description='Build\'s the TCRD schema and base tables',
        # schedule_interval=timedelta(days=28),
        start_date=datetime(2022, 4, 1),
        catchup=False,
        tags=['TCRD', 'Pharos', 'IDG'],
) as dag:


    # newName = Variable.get('NewTCRDName')
    # sqlString = sqlFiles['snapshot.sql']
    # print(sqlString)
    # sqlString = sqlString.replace('$1', 'tcrdinfinity', 10)
    # print(sqlString)
    # sqlString = sqlString.replace('$2', newName, 10)
    # print(sqlString)
    # snapshotTCRD = MySqlOperator(
    #     dag=dag,
    #     task_id='snapshot-TCRD',
    #     sql=sqlString,
    #     mysql_conn_id=mysqlConnectorID
    # )
    snapshotTCRD = PythonOperator(
        dag=dag,
        task_id='snapshotTCRD',
        python_callable=takeTCRDSnapshot
    )