import sys
import os
import time

sys.path += [ os.path.dirname(__file__) ]
sys.path += [ os.path.dirname(__file__) + '/models' ]

from FullRebuild.util import getDropFKTask
from FullRebuild.models.ncbiQueue import NcbiQueue
from datetime import datetime, timedelta
from FullRebuild.models.common import common

from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow import DAG
from airflow.operators.python import PythonOperator

sqlFiles = common.getSqlFiles()
schemaname = common.getNewDatabaseName()
mysqlConnectorID = common.getGenericConnectionName()

testing = False
fetch_time = datetime.now()
formatted_fetch_time = fetch_time.strftime('%Y-%m-%d %H:%M:%S')

def fetchNCBIData():
    mysqlserver = common.getMysqlConnector()
    databaseName = common.getNewDatabaseName()
    if testing:
        uniprotIDs = mysqlserver.get_records(
            f"select 'P0DI83' union select uniprot from {databaseName}.protein where id not in (select distinct protein_id from {databaseName}.alias where type = 'NCBI Gene ID') limit 50")
    else:
        uniprotIDs = mysqlserver.get_records(
            f"select uniprot from {databaseName}.protein where id not in (select distinct protein_id from {databaseName}.alias where type = 'NCBI Gene ID')")

    if testing:
        chunk_size = 5
    else:
        chunk_size = 2000
    chunks = [uniprotIDs[i:i + chunk_size] for i in range(0, len(uniprotIDs), chunk_size)]

    for chunk in chunks:
        queue = NcbiQueue()
        for uniprot in chunk:
            queue.addJob("GetIDs", uniprot[0])

        while(queue.runOneJob()):
            time.sleep(0.5)

        geneIDinserts, generifInserts, protein2pubmedInserts, generif2pubmedInserts = queue.getInserts()
        queue.doInserts(generifInserts, protein2pubmedInserts, generif2pubmedInserts, geneIDinserts)



with DAG(
        '3-fetch-and-process-ncbi-job',
        default_args={
            'depends_on_past': False,
            'email': ['keith.kelleher@ncats.nih.gov'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 0,
            'retry_delay': timedelta(minutes=5),
        },
        description='Fetches gene ids, publications, and generifs for all targets.',
        schedule_interval=None,
        start_date=datetime(2022, 9, 1),
        catchup=False,
        tags=['TCRD', 'Pharos', 'IDG', 'UniProt', 'Build from Scratch'],
) as dag:

    clearOldData = MySqlOperator(
        dag=dag,
        task_id='clear-data',
        sql=f"""
        DELETE FROM `alias` WHERE type = 'NCBI Gene ID' and id > 0;
        DROP TABLE IF EXISTS `ncats_generif_pubmed_map`;
        DROP TABLE IF EXISTS `protein2pubmed`;
        DROP TABLE IF EXISTS `generif2pubmed`;
        DROP TABLE IF EXISTS `generif`;
        """,
        mysql_conn_id=mysqlConnectorID,
        database=schemaname
    )

    removeAliasFK = getDropFKTask(dag, schemaname, mysqlConnectorID, 'alias', 'fk_alias_dataset')

    createTables = MySqlOperator(
        dag=dag,
        task_id='create-tables',
        sql=sqlFiles['generif.sql'] + sqlFiles['protein2pubmed.sql'] + sqlFiles['generif2pubmed.sql'],
        mysql_conn_id=mysqlConnectorID,
        database=schemaname
    )

    fetchGeneIDs = PythonOperator(
        dag=dag,
        task_id='fetch-ncbi-data',
        python_callable=fetchNCBIData
    )

    saveGeneIDMetadata = PythonOperator(
        dag=dag,
        task_id='save-gene-id-metadata',
        python_callable=common.saveFetchMetadata,
        op_args=("NCBI", "NCBI", "NCBI Gene IDs",
                 "http://www.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi?db=Gene&retmode=json&term={uniprot}[accession] and 9606[taxid]"
                 , None, None, formatted_fetch_time)
    )
    saveGeneRIFmetadata = PythonOperator(
        dag=dag,
        task_id='save-generif-metadata',
        python_callable=common.saveFetchMetadata,
        op_args=("NCBI", "NCBI", "NCBI GeneRIFs",
                 "http://www.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi?db=Gene&rettype=xml&id={gene_id}",
                 None, None, formatted_fetch_time)
    )
    savePubListMetadata = PythonOperator(
        dag=dag,
        task_id='save-publist-metadata',
        python_callable=common.saveFetchMetadata,
        op_args=("NCBI", "NCBI", "Publication List",
                 "http://www.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi?db=Gene&rettype=xml&id={gene_id}",
                 None, None, formatted_fetch_time)
    )

    clearOldData >> removeAliasFK >> \
    createTables >> fetchGeneIDs >> [saveGeneIDMetadata, saveGeneRIFmetadata, savePubListMetadata]
