import os, sys, re
from math import ceil

from airflow.operators.python import PythonOperator
from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow import DAG
import pendulum

sys.path += [ os.path.dirname(__file__) ]
sys.path += [ os.path.dirname(__file__) + '/FullRebuild' ]

from FullRebuild.models.common import common

sqlFiles = common.getSqlFiles()
testing = False  # just do some rows of the input file, set to False do the full ETL
testCount = 10000
schemaname = common.getNewDatabaseName()
mysqlConnectorID = common.getGenericConnectionName()

def countWords():
    mysqlserver = common.getMysqlConnector()
    if testing:
        pmids = mysqlserver.get_records(f'SELECT distinct pubmed_id FROM {schemaname}.protein2pubmed where source = "NCBI" limit {testCount}')
    else:
        pmids = mysqlserver.get_records(f'SELECT distinct pubmed_id FROM {schemaname}.protein2pubmed where source = "NCBI"')

    wordSums = {}
    pmids = list(map(lambda x: str(x[0]), pmids))
    chunkSize = 10000
    chunks = common.getchunks(pmids, chunkSize)
    count = 1
    abstract_count = 1
    wordPattern = r"\b[A-Za-z]+[A-Za-z0-9\-\./\+]{1,}\b|\b[0-9]+[A-Za-z]+[0-9]+\b"
    for chunk in chunks:
        print(f"processing {count} of {ceil(len(pmids) / chunkSize)}")
        count += 1
        textlist = '","'.join(chunk)
        query = f'SELECT abstract FROM ncats_pubmed.pubmed where id in ("{textlist}") and abstract is not NULL'
        abstracts = mysqlserver.get_records(query)
        for abstract in abstracts:
            abstract_count += 1
            matches = re.findall(wordPattern, abstract[0])
            wordsInAbstract = {word.lower(): 1 for word in matches}
            for k in wordsInAbstract:
                if k in wordSums:
                    wordSums[k] += 1
                else:
                    wordSums[k] = 1

    inserts = [(k, v) for k, v in wordSums.items()]
    inserts.append(('__ABSTRACT_COUNT__', abstract_count))
    mysqlserver.insert_many_rows(f'{schemaname}.word_count', inserts, target_fields=('word', 'count'))

def countAbstractWordsDAG():
    dag_subdag = DAG(
        dag_id='count-abstract-words',
        start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
        catchup=False,
        schedule_interval=None,
        tags=['TCRD', 'Pharos', 'IDG', 'Enrichment', 'PubMed']
    )

    # region Drop Tables
    dropAllTables = MySqlOperator(
        dag=dag_subdag,
        task_id='drop-word-count-table',
        sql=f"""DROP TABLE IF EXISTS `word_count`;""",
        mysql_conn_id=mysqlConnectorID,
        database=schemaname
    )

    # region Subject Table
    createTables = MySqlOperator(
        dag=dag_subdag,
        task_id='create-word-count-table',
        sql=sqlFiles['word_count.sql'],
        mysql_conn_id=mysqlConnectorID,
        database=schemaname
    )

    countWordsTask = PythonOperator(
        dag=dag_subdag,
        task_id='count-abstract-words',
        python_callable=countWords
    )

    dropAllTables >> createTables >> countWordsTask

    return dag_subdag

dag = countAbstractWordsDAG()