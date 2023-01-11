
import pendulum, sys, os
from airflow.providers.mysql.operators.mysql import MySqlOperator

from airflow import DAG
from airflow.operators.python import PythonOperator

sys.path += [ os.path.dirname(__file__) ]
sys.path += [ os.path.dirname(__file__) + '/FullRebuild' ]
from FullRebuild.models.common import common

sqlFiles = common.getSqlFiles()
schemaname = common.getNewDatabaseName()
mysqlConnectorID = common.getGenericConnectionName()


def createAncestryTablesDAG(parent_dag_name, child_task_id, args):
    dag_subdag = DAG(
        dag_id=f'{parent_dag_name}.{child_task_id}',
        default_args=args,
        start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
        catchup=False,
        schedule_interval=None,
    )

    for ontology in ontologies:
        dropit = MySqlOperator(
            dag=dag_subdag,
            task_id=f'drop-ancestry_{ontology["table"]}-table',
            sql=f"""DROP TABLE IF EXISTS `ancestry_{ontology["table"]}`;""",
            mysql_conn_id=mysqlConnectorID,
            database=schemaname
        )
        sql = sqlFiles['ancestry_all.sql']\
            .replace('{ontology}', ontology["table"])\
            .replace('{ontology_id}', ontology["id"])\
            .replace('{length}', ontology["keylength"])

        createit = MySqlOperator(
                dag=dag_subdag,
                task_id=f"create-ancestry_{ontology['table']}-table",
                sql=sql,
                mysql_conn_id=mysqlConnectorID,
                database=schemaname
            )
        populateit = PythonOperator(
            dag=dag_subdag,
            task_id=f'populate-ancestry_{ontology["table"]}-table',
            python_callable=populateTable,
            op_kwargs={'ontology': ontology},
        )
        dropit >> createit >> populateit

    return dag_subdag

def relevantUberonIDs():
    mysqlserver = common.getMysqlConnector()
    return mysqlserver.get_records(f"""select uberon_id from {schemaname}.expression union select uberon_id from {schemaname}.gtex""")

def relevantMondoIDs():
    mysqlserver = common.getMysqlConnector()
    return mysqlserver.get_records(f"""select mondoid from {schemaname}.ncats_disease where mondoid is not NULL""")

def getDTOparents():
    mysqlserver = common.getMysqlConnector()
    return mysqlserver.get_records(f"""SELECT dtoid, parent_id FROM {schemaname}.dto where parent_id is not NULL""")

def relevantDTOIDs():
    mysqlserver = common.getMysqlConnector()
    return mysqlserver.get_records(f"""SELECT distinct replace(dtoid,"_",":") FROM {schemaname}.protein WHERE replace(dtoid,"_",":") in (select dtoid from {schemaname}.dto);""")

ontologies = [
    {
        'table': 'do',
        'id': 'doid',
        'parent_table': 'do_parent',
        'keylength': '12'
    },
    {
        'table': 'dto',
        'id': 'dtoid',
        'parent_query': getDTOparents,
        'id_function': relevantDTOIDs,
        'keylength': '255'
    },
    {
        'table': 'mondo',
        'id': 'mondoid',
        'parent_table': 'mondo_parent',
        'keylength': '20',
        'id_function': relevantMondoIDs
    },
    {
        'table': 'uberon',
        'id': 'uid',
        'parent_table': 'uberon_parent',
        'keylength': '20',
        'id_function': relevantUberonIDs,
        'boring_ids': ['CARO:0000000', 'CARO:0030000', 'BFO:0000004', 'BFO:0000002']
    }
]

def getParents(uberon, tree, node, list, branches):
    list.append(node)
    parents = [row[1] for row in tree if row[0] == node]
    parentObjects = [row for row in uberon if row[0] in parents]
    for p in parentObjects:
        getParents(uberon, tree, p[0], [r for r in list], branches)
    if len(parentObjects) == 0:
        branches.append(list)

def populateTable(**kwargs):
    ontology = kwargs['ontology']
    mysqlserver = common.getMysqlConnector()

    if 'id_function' in ontology:
        relevantIDs = ontology['id_function']()
    else :
        relevantIDs = mysqlserver.get_records(f"""select {ontology['id']} from {schemaname}.{ontology['table']}""")
    if 'boring_ids' in ontology:
        query = f"""select * from {schemaname}.{ontology['table']} where {ontology['id']} not in ("{'","'.join(ontology['boring_ids'])}")"""
    else:
        query = f"""select * from {schemaname}.{ontology['table']}"""
    ontologyDetails = mysqlserver.get_records(query)
    if 'parent_query' in ontology:
        ontologyTree = ontology['parent_query']()
    else:
        ontologyTree = mysqlserver.get_records(f"""select * from {schemaname}.{ontology['parent_table']}""")

    inserts = []
    count = 0
    total = len(relevantIDs)
    for row in relevantIDs:
        count += 1
        branches = []
        getParents(ontologyDetails, ontologyTree, row[0], [], branches)
        nonDupList = []
        for branch in branches:
            for ancestor in branch:
                if ancestor not in nonDupList:
                    nonDupList.append(ancestor)
        for ancestor in nonDupList:
            inserts.append((row[0], ancestor))

    mysqlserver.insert_many_rows(f'{schemaname}.ancestry_{ontology["table"]}', inserts, target_fields=('oid', 'ancestor_id'))

# dag = createAncestryTablesDAG('standalone', 'create-ancestry-tables', {"retries": 0})