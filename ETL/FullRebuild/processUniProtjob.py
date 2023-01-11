import sys
import os

sys.path += [ os.path.dirname(__file__) ]
sys.path += [ os.path.dirname(__file__) + '/models' ]

import json
import gzip

from datetime import datetime, timedelta

from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow import DAG
from airflow.operators.python import PythonOperator

from FullRebuild.models.protein import protein
from FullRebuild.models.alias import alias
from FullRebuild.models.keyword import keyword
from FullRebuild.models.goterm import goterm, go_association
from FullRebuild.models.common import common

sqlFiles = common.getSqlFiles()
schemaname = common.getNewDatabaseName()
mysqlConnectorID = common.getGenericConnectionName()

def processUniProtData():
    gzFileName = common.getConfigFile("UniProt", None, "data")
    proteinObjects = []
    with gzip.open(gzFileName, 'rb') as f:
        file_content = f.read()
        uniprotList = json.loads(file_content.decode('utf-8'))['results']
        proteinObjects = [protein(obj) for obj in uniprotList]
    protein.assignIDs(proteinObjects)
    protein.calculatePreferredSymbols(proteinObjects)
    (go_associations, go_terms) = protein.extractGOterms(proteinObjects)
    keyword_list = protein.extractKeywords(proteinObjects)
    alias_list = protein.extractAliases(proteinObjects)

    mysqlserver = common.getMysqlConnector()
    mysqlserver.insert_many_rows(f'{schemaname}.go',
                                 [go_terms[key].getInsertTuple() for key in go_terms],
                                 target_fields=goterm.getFields())

    mysqlserver.insert_many_rows(f'{schemaname}.protein',
                                 [pro.getInsertTuple() for pro in proteinObjects],
                                 target_fields=protein.getFields())

    mysqlserver.insert_many_rows(f'{schemaname}.goa',
                                 [association.getInsertTuple() for association in go_associations],
                                 target_fields=go_association.getFields())

    mysqlserver.insert_many_rows(f'{schemaname}.xref',
                                 [keywordObj.getInsertTuple() for keywordObj in keyword_list],
                                 target_fields=keyword.getFields())

    mysqlserver.insert_many_rows(f'{schemaname}.alias',
                                 [aliasObj.getInsertTuple() for aliasObj in alias_list],
                                 target_fields=alias.getFields())



with DAG(
        '2a-process-uniprot-job3',
        default_args={
            'depends_on_past': False,
            'email': ['keith.kelleher@ncats.nih.gov'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 0,
            'retry_delay': timedelta(minutes=5),
        },
        description='Fetches new version of UniProt data once per month, on the first of the month.',
        schedule_interval=None,
        start_date=datetime(2022, 9, 1),
        catchup=False,
        tags=['TCRD', 'Pharos', 'IDG', 'UniProt', 'Build from Scratch'],
) as dag:

    dropTables = MySqlOperator(
        dag=dag,
        task_id='start-fresh',
        sql=f"""DROP SCHEMA IF EXISTS `tcrdinfinity`;
                CREATE SCHEMA `tcrdinfinity`;
                GRANT SELECT ON `tcrdinfinity`.* TO 'tcrd'@'%';""",
        mysql_conn_id=mysqlConnectorID
    )
    createTables = MySqlOperator(
        dag=dag,
        task_id='create-tables',
        sql=sqlFiles['input_version.sql'] + sqlFiles['protein.sql'] + sqlFiles['goa.sql'] + sqlFiles['xref.sql'] + sqlFiles['alias.sql'],
        mysql_conn_id=mysqlConnectorID,
        database=schemaname
    )
    processData = PythonOperator(
        dag=dag,
        task_id='process-UniProt',
        python_callable=processUniProtData
    )
    saveMetadata = common.getSaveMetadataTask('UniProt', dag)

    dropTables >> createTables >> processData >> saveMetadata
