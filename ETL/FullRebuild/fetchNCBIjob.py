import sys
import os
import time

sys.path += [ os.path.dirname(__file__) ]
sys.path += [ os.path.dirname(__file__) + '/models' ]

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
            f"select uniprot from {databaseName}.protein where id not in (select distinct protein_id from {databaseName}.alias where type = 'NCBI Gene ID') limit 100")
    else:
        uniprotIDs = mysqlserver.get_records(
            f"select uniprot from {databaseName}.protein where id not in (select distinct protein_id from {databaseName}.alias where type = 'NCBI Gene ID')")

    queue = NcbiQueue()
    for uniprot in uniprotIDs:
        queue.addJob("GetIDs", uniprot[0])

    while(queue.runOneJob()):
        time.sleep(0.5)

    geneIDinserts, generifInserts, protein2pubmedInserts, generif2pubmedInserts = queue.getInserts()

    mysqlserver.insert_many_rows(f'{schemaname}.generif', generifInserts, target_fields=('id', 'protein_id', 'gene_id', 'text', 'date'))
    mysqlserver.insert_many_rows(f'{schemaname}.protein2pubmed', protein2pubmedInserts, target_fields=('protein_id','pubmed_id', 'gene_id', 'source'))
    mysqlserver.insert_many_rows(f'{schemaname}.generif2pubmed', generif2pubmedInserts, target_fields=('generif_id','pubmed_id'))
    mysqlserver.insert_many_rows(f'{schemaname}.alias', geneIDinserts, target_fields=('type','protein_id', 'value'))


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

    removeAliasFK = MySqlOperator(
        dag=dag,
        task_id='remove-alias-dataset-fk', # input_version keeps track of the data source now
        sql=f"""
            -- DROP FOREIGN KEY IF EXISTS
            SELECT
                COUNT(*)
            INTO
                @FOREIGN_KEY_my_foreign_key_ON_TABLE_my_table_EXISTS
            FROM
                `information_schema`.`table_constraints`
            WHERE
                `table_schema` = '{schemaname}'
                AND `table_name` = 'alias'
                AND `constraint_name` = 'fk_alias_dataset'
                AND `constraint_type` = 'FOREIGN KEY'
            ;
            SET @statement := IF(
                @FOREIGN_KEY_my_foreign_key_ON_TABLE_my_table_EXISTS > 0,
                -- 'SELECT "info: foreign key exists."',
                'ALTER TABLE alias DROP FOREIGN KEY fk_alias_dataset',
                'SELECT "info: foreign key does not exist."'
            );
            PREPARE statement FROM @statement;
            EXECUTE statement;
            """,
        mysql_conn_id=mysqlConnectorID,
        database=schemaname
    )

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
