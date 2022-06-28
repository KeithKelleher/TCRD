import os

from airflow.models import Variable
from airflow.operators.bash import BashOperator

from buildUberonAncestry import createUberonAncestryDAG
from copyOldTCRD import createCopyTCRDDag
from util import getSqlFiles, doVersionInserts, version_is_same, getMysqlConnector, getBaseDirectory
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.operators.subdag import SubDagOperator
from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.utils.trigger_rule import TriggerRule
from buildGTEx import createGTExDAG
from buildExpression import createExpressionDAG
from airflow import DAG
from airflow.utils.dates import days_ago

DAG_NAME = 'rebuild-tcrd3'
copySchema = Variable.get('CopyTCRD')
sqlFiles = getSqlFiles()
schemaname = 'tcrdinfinity'
mysqlConnectorID = schemaname
directory = os.path.dirname(__file__) + '/'


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


dag = DAG(
    dag_id=DAG_NAME,
    default_args={"retries": 0},
    start_date=days_ago(0),
    schedule_interval=None,
    tags=['TCRD', 'Pharos', 'IDG']
)

createTables = MySqlOperator(
    dag=dag,
    task_id='create-new-tables',
    sql=sqlFiles['input_version.sql'] + sqlFiles['ncats_unfiltered_counts.sql'],
    mysql_conn_id=mysqlConnectorID
)

def getDataSourceTasks(dag, key, subdagFunction, table):
    mysqlserver = getMysqlConnector()
    connection = mysqlserver.get_connection(schemaname)
    rebuild_task_id = 'rebuild-' + key + '-tables'
    tasks = {}
    tasks['is_updated'] = BranchPythonOperator(
        task_id='is-' + key + '-updated',
        python_callable=check_input_versions,
        op_kwargs={'input': key, 'table': table},
        dag=dag
    )
    tasks['rebuild'] = SubDagOperator(
        dag=dag,
        task_id=rebuild_task_id,
        subdag=subdagFunction(DAG_NAME, rebuild_task_id, dag.default_args)
    )

    directory = getBaseDirectory()

    tasks['dump'] = BashOperator(
        task_id='bash-dump-' + table,
        bash_command=f"""mysqldump --column-statistics=0 --set-gtid-purged=OFF -h{connection.host} \
        -u{connection.login} -p"{connection.password}" --databases {copySchema} --tables {table} > {directory}dump-{table}.sql""",
    )

    tasks['load'] = BashOperator(
        task_id='bash-load-' + table,
        bash_command=f'mysql -h{connection.host} -u{connection.login} -p"{connection.password}" tcrdinfinity < {directory}dump-{table}.sql'
    )

    tasks['save_metadata'] = PythonOperator(
        dag=dag,
        task_id='save-' + key + '-metadata',
        python_callable=saveMetadata,
        op_kwargs={'input': key},
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )
    tasks['is_updated'] >> tasks['rebuild'] >> tasks['save_metadata']
    tasks['is_updated'] >> tasks['dump'] >> tasks['load'] >> tasks['save_metadata']
    return tasks

pop_ancestry_task_id='populate-ancestry-table'
populateAncestryTable = SubDagOperator(
    dag=dag,
    task_id=pop_ancestry_task_id,
    subdag=createUberonAncestryDAG(DAG_NAME, pop_ancestry_task_id, dag.default_args)
)

copy_task_id = 'copy-old-tcrd'
copyOldTCRD = SubDagOperator(
    dag=dag,
    task_id=copy_task_id,
    subdag=createCopyTCRDDag(DAG_NAME, copy_task_id, dag.default_args)
)

copyOldTCRD >> createTables

gtexTasks = getDataSourceTasks(dag, 'GTEx', createGTExDAG, 'gtex')
createTables >> gtexTasks['is_updated']

expressionTasks = getDataSourceTasks(dag, 'Expression', createExpressionDAG, 'expression')
createTables >> expressionTasks['is_updated']

[gtexTasks['save_metadata'], expressionTasks['save_metadata']] >> populateAncestryTable
