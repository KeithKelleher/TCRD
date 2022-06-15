import os
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from util import doVersionInserts, version_is_same, getMysqlConnector, getBaseDirectory
from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow import DAG
import pendulum


DAG_NAME = 'copy-old-tcrd'
copySchema = Variable.get('CopyTCRD')
schemaname = 'tcrdinfinity'
mysqlConnectorID = schemaname
directory = os.path.dirname(__file__) + '/'

mysqlserver = getMysqlConnector()
connection = mysqlserver.get_connection('tcrdinfinity')

def check_input_versions(**kwargs):
    inputKey = kwargs['input']
    table = kwargs['table']
    doFullRebuild = Variable.get('FullRebuild')
    if (doFullRebuild == 'True'):
        print('doing full rebuild')
        return 'rebuild-' + inputKey + '-tables'

    print('checking versions for ' + inputKey)
    if (version_is_same(inputKey)):
        return 'bash-dump-' + table
    return 'rebuild-' + inputKey + '-tables'


def saveMetadata(**kwargs):
    inputKey = kwargs['input']
    doVersionInserts(inputKey)

def createCopyTCRDDag(parent_dag_name, child_task_id, args):
    dag_subdag = DAG(
        dag_id=f'{parent_dag_name}.{child_task_id}',
        default_args=args,
        start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
        catchup=False,
        schedule_interval=None,
        tags=['TCRD', 'Pharos', 'IDG']
    )

    dump_file = getBaseDirectory() + 'tcrd-dump.sql'
    tables = "protein goa tdl_info xref t2tc target info_type uberon data_type"
    # tables = "protein goa"

    bash_dump = BashOperator(
        dag=dag_subdag,
        task_id='bash-dump-tcrd',
        bash_command=f"""mysqldump --column-statistics=0 --set-gtid-purged=OFF -h{connection.host} \
        -u{connection.login} -p"{connection.password}" --databases {copySchema} --tables {tables} > {dump_file}""",
    )

    bash_load = BashOperator(
        dag=dag_subdag,
        task_id='bash-load-tcrd',
        bash_command=f'mysql -h{connection.host} -u{connection.login} -p"{connection.password}" tcrdinfinity < {dump_file}'
    )

    truncateSchema = MySqlOperator(
        dag=dag_subdag,
        task_id='start-fresh',
        sql=f"""DROP SCHEMA IF EXISTS `tcrdinfinity`;
                CREATE SCHEMA `tcrdinfinity`;""",
        mysql_conn_id=mysqlConnectorID
    )

    truncateSchema >> bash_load
    bash_dump >> bash_load
    return dag_subdag

dag = createCopyTCRDDag('standalone', 'copy-old-tcrd', {"retries": 0})