import os,sys

from airflow.operators.subdag import SubDagOperator
from airflow.providers.mysql.operators.mysql import MySqlOperator
from buildGTEx import createGTExDAG
from buildExpression import createExpressionDAG
from airflow import DAG
from airflow.utils.dates import days_ago

sys.path += [ os.path.dirname(__file__) ]
sys.path += [ os.path.dirname(__file__) + '/FullRebuild' ]
from FullRebuild.models.common import common

DAG_NAME = 'build-all-expression'
schemaname = common.getNewDatabaseName()
mysqlConnectorID = common.getGenericConnectionName()

directory = os.path.dirname(__file__) + '/'

sqlFiles = common.getSqlFiles()

dag = DAG(
    dag_id=DAG_NAME,
    default_args={"retries": 0},
    start_date=days_ago(0),
    schedule_interval=None,
    tags=['TCRD', 'Pharos', 'IDG']
)

def getDataSourceTasks(dag, key, subdagFunction):
    rebuild_task_id = 'rebuild-' + key + '-tables'
    tasks = {}
    tasks['rebuild'] = SubDagOperator(
        dag=dag,
        task_id=rebuild_task_id,
        subdag=subdagFunction(DAG_NAME, rebuild_task_id, dag.default_args)
    )
    tasks['save_metadata'] = common.getSaveMetadataTask(key, dag)
    tasks['rebuild'] >> tasks['save_metadata']
    return tasks

createTables = MySqlOperator(
    dag=dag,
    task_id='create-tables',
    sql=sqlFiles['target.sql'] +
        sqlFiles['ncats_ligands.sql'] +
        sqlFiles['tdl_info.sql'] +
        sqlFiles['ncats_dataSource.sql'] +
        sqlFiles['ncats_dataSource_map.sql'],
    mysql_conn_id=mysqlConnectorID,
    database=schemaname
)

gtexTasks = getDataSourceTasks(dag, 'GTEx', createGTExDAG)

expressionTasks = getDataSourceTasks(dag, 'Expression', createExpressionDAG)

createTables >> [gtexTasks['rebuild'], expressionTasks['rebuild']]
