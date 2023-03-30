import os
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow import DAG
import pendulum

newDB = Variable.get('NewDB')

from FullRebuild.models.common import common
sqlFiles = common.getSqlFiles()
DAG_NAME = 'copy-database'
mysqlConnectorID = 'aws_conn'

mysqlserver = MySqlHook(mysqlConnectorID)

directory = os.path.dirname(__file__) + '/'

def createLoadDBDag(child_task_id, args):
    connection = mysqlserver.get_connection(mysqlConnectorID)
    dag = DAG(
        dag_id=f'{child_task_id}',
        default_args=args,
        start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
        catchup=False,
        schedule_interval=None,
        tags=['TCRD', 'Pharos', 'IDG', 'Copy Database', 'Utility']
    )

    dump_file = directory + 'tcrd-dump.sql'
    bash_load = BashOperator(
        dag=dag,
        task_id='bash-load-database',
        bash_command=f'mysql -h{connection.host} -u{connection.login} '
                     f'-p"{connection.password}" {newDB} < {dump_file}'
    )
    truncateSchema = MySqlOperator(
        dag=dag,
        task_id='start-fresh',
        sql=f"""DROP SCHEMA IF EXISTS `{newDB}`;
                CREATE SCHEMA `{newDB}`;
                GRANT SELECT ON `{newDB}`.* TO 'tcrd'@'%';""",
        mysql_conn_id=mysqlConnectorID
    )

    createTables = MySqlOperator(
        dag=dag,
        task_id='create-tables',
        sql=sqlFiles['input_version.sql'],
        mysql_conn_id=mysqlConnectorID,
        database=newDB
    )

    truncateSchema >> createTables
    truncateSchema >> bash_load
    return dag

dag = createLoadDBDag('2b-load-db-dumpfile', {"retries": 0})