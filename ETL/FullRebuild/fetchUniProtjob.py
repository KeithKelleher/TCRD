import sys
import os
from urllib import request, parse

sys.path += [ os.path.dirname(__file__) ]
sys.path += [ os.path.dirname(__file__) + '/models' ]

import json
import gzip

from datetime import datetime, timedelta

from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow import DAG
from airflow.operators.python import PythonOperator

from models.protein import protein
from models.alias import alias
from models.keyword import keyword
from models.goterm import goterm, go_association

from util import getSqlFiles, getMysqlConnector


sqlFiles = getSqlFiles()
schemaname = 'tcrdinfinity'
mysqlConnectorID = schemaname

gzFileName = 'uniprot.json.gz'

baseURL = "https://rest.uniprot.org/uniprotkb/stream?compressed=true&download=true&format=json&query="
fullQuery = "(*) AND (reviewed:true) AND (model_organism:9606)"
sampleQuery = "accession:A0A0C5B5G6 OR accession:A0A1B0GTW7 OR accession:A0JNW5 OR accession:A0JP26 OR " \
              "accession:A0PK11 OR accession:A1A4S6 OR accession:A1A519 OR accession:A1L190 OR accession:A1L3X0 OR " \
              "accession:A1X283 OR accession:A2A2Y4 OR accession:A2RU14 OR accession:A2RUB6 OR accession:A2RUC4 OR " \
              "accession:A4D1B5 OR accession:A4GXA9 OR accession:A5D8V7 OR accession:A5PLL7 OR accession:A6BM72 OR " \
              "accession:A6H8Y1 OR accession:A6NCS4 OR accession:A6NFY7 OR accession:A6NGG8 OR accession:A6NI61 OR " \
              "accession:A6NKB5"

def fetchNewUniProtData():
    # print ('no need to refetch it right now')
    # request.urlretrieve(baseURL + parse.quote(sampleQuery), gzFileName)
    request.urlretrieve(baseURL + parse.quote(fullQuery), gzFileName)

def processUniProtData():
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

    mysqlserver = getMysqlConnector()
    mysqlserver.insert_many_rows('go',
                                 [go_terms[key].getInsertTuple() for key in go_terms],
                                 target_fields=goterm.getFields())

    mysqlserver.insert_many_rows('protein',
                                 [pro.getInsertTuple() for pro in proteinObjects],
                                 target_fields=protein.getFields())

    mysqlserver.insert_many_rows('goa',
                                 [association.getInsertTuple() for association in go_associations],
                                 target_fields=go_association.getFields())

    mysqlserver.insert_many_rows('xref',
                                 [keywordObj.getInsertTuple() for keywordObj in keyword_list],
                                 target_fields=keyword.getFields())

    mysqlserver.insert_many_rows('alias',
                                 [aliasObj.getInsertTuple() for aliasObj in alias_list],
                                 target_fields=alias.getFields())



with DAG(
        'fetch-uniprot-job3',
        default_args={
            'depends_on_past': False,
            'email': ['keith.kelleher@ncats.nih.gov'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 0,
            'retry_delay': timedelta(minutes=5),
        },
        description='Fetches new version of UniProt data once per month, on the first of the month.',
        schedule_interval="@monthly",
        start_date=datetime(2022, 9, 1),
        catchup=False,
        tags=['TCRD', 'Pharos', 'IDG', 'UniProt'],
) as dag:
    dropTables = MySqlOperator(
        dag=dag,
        task_id='drop-tables',
        sql="""
        DROP TABLE IF EXISTS `alias`;
        DROP TABLE IF EXISTS `goa`;
        DROP TABLE IF EXISTS `go`;
        DROP TABLE IF EXISTS `xref`;
        DROP TABLE IF EXISTS `protein`;
        """,
        mysql_conn_id=mysqlConnectorID
    )
    fetchData = PythonOperator(
        dag=dag,
        task_id='fetch-UniProt',
        python_callable=fetchNewUniProtData
    )
    createTables = MySqlOperator(
        dag=dag,
        task_id='create-tables',
        sql=sqlFiles['protein.sql'] + sqlFiles['goa.sql'] + sqlFiles['xref.sql'] + sqlFiles['alias.sql'],
        mysql_conn_id=mysqlConnectorID
    )
    processData = PythonOperator(
        dag=dag,
        task_id='process-UniProt',
        python_callable=processUniProtData
    )
    dropTables >> [fetchData, createTables] >> processData
