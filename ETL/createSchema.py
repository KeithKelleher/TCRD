from datetime import datetime, timedelta
from sql.loadSqlFiles import getSqlFiles
# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG
from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.utils.task_group import TaskGroup

sqlFiles = getSqlFiles()
schemaname = 'tcrdinfinity'
mysqlConnectorID = schemaname

def doNothing(**kwargs):
    print(kwargs['task'])
    return 1

def getCreateSchemaDAG():
    return dag

with DAG(
        'create-or-truncate-tcrdinfinity',
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

    createDatabase = MySqlOperator(
        dag=dag,
        task_id='createDatabase',
        sql=sqlFiles['createSchema.sql'],
        mysql_conn_id=mysqlConnectorID
    )

    with TaskGroup("createBaseTables", tooltip="Create Tables for UniProt Data") as createBaseTables:
        createProteinTableTask = MySqlOperator(
            dag=dag,
            task_id='createProteinTable',
            sql='use ' + schemaname + ';' + sqlFiles['protein.sql'],
            mysql_conn_id=mysqlConnectorID
        )

        createDatasetTableTask = MySqlOperator(
            dag=dag,
            task_id='createDatasetTable',
            sql='use ' + schemaname + ';' + sqlFiles['dataset.sql'],
            mysql_conn_id=mysqlConnectorID
        )

        createTargetTableTask = MySqlOperator(
            dag=dag,
            task_id='createTargetTable',
            sql='use ' + schemaname + ';' + sqlFiles['target.sql'],
            mysql_conn_id=mysqlConnectorID
        )

        createT2TCTableTask = MySqlOperator(
            dag=dag,
            task_id='createT2TCTable',
            sql='use ' + schemaname + ';' + sqlFiles['t2tc.sql'],
            mysql_conn_id=mysqlConnectorID
        )

        createAliasTableTask = MySqlOperator(
            dag=dag,
            task_id='createAliasTable',
            sql='use ' + schemaname + ';' + sqlFiles['alias.sql'],
            mysql_conn_id=mysqlConnectorID
        )
        createT2TCTableTask << [createProteinTableTask, createTargetTableTask]
        createAliasTableTask << [createProteinTableTask, createDatasetTableTask]

    createDatabase >> createBaseTables