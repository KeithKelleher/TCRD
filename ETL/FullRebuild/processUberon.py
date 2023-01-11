import sys, re, os, obonet

sys.path += [ os.path.dirname(__file__) ]
sys.path += [ os.path.dirname(__file__) + '/models' ]

from datetime import datetime, timedelta
from FullRebuild.models.common import common

from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow import DAG
from airflow.operators.python import PythonOperator

sqlFiles = common.getSqlFiles()
schemaname = common.getNewDatabaseName()
mysqlConnectorID = common.getGenericConnectionName()

testing = False

def processUberonOBO():
    file = common.getConfigFile('Uberon', None, 'data')
    graph = obonet.read_obo(file)
    print(graph)
    count = 0
    # id_to_name = {id: data.get('name') for id, data in graph.nodes(data=True)}
    uberon_inserts = []
    uberon_parent_inserts = []
    uberon_xref_dict = {}
    for id, data in graph.nodes(data=True):
        if id.startswith('http') or data is None:
            continue
        if count > 1000 and testing:
            continue
        count += 1
        if 'xref' in data:
            xrefs = data['xref']
            for xref in xrefs:
                pieces = xref.split(':')
                if len(pieces) > 1:
                    db = pieces[0]
                    if db in validOntologies:
                        value = pieces[1]
                        key = f"{id}-{db}-{value}".upper()
                        if key not in uberon_xref_dict:
                            uberon_xref_dict[key] = (id, db, value)

        if 'is_a' in data:
            parents = data['is_a']
            for parent in parents:
                uberon_parent_inserts.append((id, parent))

        name = data['name'] if 'name' in data else id
        definition = data['def'] if 'def' in data else None
        if definition is not None:
            matches = re.findall('\"(.+)\" \[', definition)
            if len(matches) > 0:
                definition = matches[0]
        comment = data['comment'] if 'comment' in data else None
        uberon_inserts.append((id, name, definition, comment))
    mysqlserver = common.getMysqlConnector()
    mysqlserver.insert_many_rows(f'{schemaname}.uberon', uberon_inserts, target_fields=('uid', 'name', 'def', 'comment'))
    mysqlserver.insert_many_rows(f'{schemaname}.uberon_parent', uberon_parent_inserts, target_fields=('uid', 'parent_id'))
    mysqlserver.insert_many_rows(f'{schemaname}.uberon_xref', [uberon_xref_dict[k] for k in uberon_xref_dict], target_fields=('uid', 'db', 'value'))


with DAG(
        '5-build-uberon',
        default_args={
            'depends_on_past': False,
            'email': ['keith.kelleher@ncats.nih.gov'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 0,
            'retry_delay': timedelta(minutes=5),
        },
        description='Rebuilds the uberon and uberon_parent tables.',
        schedule_interval=None,
        start_date=datetime(2022, 9, 1),
        catchup=False,
        tags=['TCRD', 'Pharos', 'IDG', 'Uberon', 'Build from Scratch'],
) as dag:

    clearOldData = MySqlOperator(
        dag=dag,
        task_id='clear-data',
        sql=f"""
        DROP TABLE IF EXISTS `uberon_xref`;
        DROP TABLE IF EXISTS `uberon_parent`;
        DROP TABLE IF EXISTS `uberon`;
        """,
        mysql_conn_id=mysqlConnectorID,
        database=schemaname
    )

    createTables = MySqlOperator(
        dag=dag,
        task_id='create-tables',
        sql=sqlFiles['uberon.sql'] + sqlFiles['uberon_parent.sql'] + sqlFiles['uberon_xref.sql'],
        mysql_conn_id=mysqlConnectorID,
        database=schemaname
    )

    fillTables = PythonOperator(
        dag=dag,
        python_callable=processUberonOBO,
        task_id='process-uberon-file'
    )

    saveMetadataTask = common.getSaveMetadataTask('Uberon', dag)

    clearOldData >> createTables >> fillTables >> saveMetadataTask

validOntologies = [
    "FMA","EMAPA","MA","UMLS","NCIT","ZFA","BTO","EHDAA2","BAMS","neuronames","VHOG","MESH","BIRNLEX","TAO","EHDAA",
    "GAID","DHBA","CALOHA","EFO","XAO","HBA","OpenCyc","MBA","galen","FBbt","GC_ID","EV","DMBA","BM","MAT",
    "MIAA","NLXANAT","VSAO","AEO","RETIRED_EHDAA2","KUPO","Wikipedia","NLX","ZFS","UBERONTEMP",
    "UBERON","TGMA","TE","TADS","STID","SPD","SCTID","SAO","PHENOSCAPE","PBA","OGES",
    "OGEM","NOID","NIFSTD","MURDOCH","MPATH","MP","MFMO","MAP","ISBN","HAO","GOC","GO",
    "FMAID","FAO","EVM","ENVO","EMAPS","EHDA","CP","CL","CARO","BSA","BILS","BILA","ANISEED","ABA","AAO"]
