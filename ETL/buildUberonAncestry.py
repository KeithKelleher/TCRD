
import pendulum
from airflow.providers.mysql.operators.mysql import MySqlOperator

from util import getSqlFiles, getMysqlConnector
from airflow import DAG
from airflow.operators.python import PythonOperator

sqlFiles = getSqlFiles()
schemaname = 'tcrdinfinity'
mysqlConnectorID = schemaname

def createUberonAncestryDAG(parent_dag_name, child_task_id, args):
    dag_subdag = DAG(
        dag_id=f'{parent_dag_name}.{child_task_id}',
        default_args=args,
        start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
        catchup=False,
        schedule_interval=None,
    )

    # region Drop Tables
    dropAllTables = MySqlOperator(
        dag=dag_subdag,
        task_id='drop-all-tables',
        sql=f"""DROP TABLE IF EXISTS `uberon_ancestry`;""",
        mysql_conn_id=mysqlConnectorID
    )
    #endregion

    createAncestryTable = MySqlOperator(
        dag=dag_subdag,
        task_id='create-ancestry-table',
        sql=sqlFiles['uberon_ancestry.sql'],
        mysql_conn_id=mysqlConnectorID
    )

    def getParents(uberon, tree, node, list, branches):
        list.append(node)
        parents = [row[1] for row in tree if row[0] == node]
        parentObjects = [row for row in uberon if row[0] in parents]
        for p in parentObjects:
            getParents(uberon, tree, p[0], [r for r in list], branches)
        if len(parentObjects) == 0:
            branches.append(list)

    def populateTable():
        mysqlserver = getMysqlConnector()
        uberon = mysqlserver.get_records("""select * from uberon""")
        tree = mysqlserver.get_records("""select * from uberon_parent""")
        relevantIDs = mysqlserver.get_records("""select uberon_id from expression union select uberon_id from gtex""")

        inserts = []
        for row in relevantIDs:
            branches = []
            getParents(uberon, tree, row[0], [], branches)
            nonDupList = []
            for branch in branches:
                for ancestor in branch:
                    if ancestor not in nonDupList:
                        nonDupList.append(ancestor)
            for ancestor in nonDupList:
                if ancestor != row[0]:
                    inserts.append((row[0], ancestor))

        mysqlserver.insert_many_rows('uberon_ancestry', inserts, target_fields=('uberon_id', 'ancestor_uberon_id'))



    populateAncestryTable = PythonOperator(
        dag=dag_subdag,
        task_id='populate-ancestry-table',
        python_callable=populateTable
    )

    dropAllTables >> createAncestryTable >> populateAncestryTable

    return dag_subdag

dag = createUberonAncestryDAG('standalone', 'populate-ancestry-table', {"retries": 0})