import sys, csv
import os
import time

from airflow.operators.python import PythonOperator
from airflow.providers.mysql.operators.mysql import MySqlOperator

sys.path += [ os.path.dirname(__file__) ]
sys.path += [ os.path.dirname(__file__) + '/models' ]

from datetime import datetime, timedelta
from FullRebuild.models.common import common
from airflow import DAG

csv.field_size_limit(sys.maxsize)

sqlFiles = common.getSqlFiles()
schemaname = common.getNewDatabaseName()
mysqlConnectorID = common.getGenericConnectionName()
inputKey = 'JensenLab textmining mentions'
sourceName = 'JensenLab'

testing = False

def processData():
    datafile = common.getConfigFile(inputKey, None, 'data')
    mysqlserver = common.getMysqlConnector()

    aliases = mysqlserver.get_records(f"select value, type, protein_id from {schemaname}.alias union select value, xtype as type, protein_id from {schemaname}.xref where xtype = 'Ensembl'")

    with open(datafile) as f:
        rd = csv.reader(f, delimiter="\t")
        count = 0
        for row in rd:
            inserts = []
            id = row[0]
            matches = list(filter(lambda match: id == match[0] or id == match[0].split('.')[0], aliases))
            if len(matches) > 0:
                matching_proteins = set(map(lambda match: match[2], matches))
                if len(matching_proteins) > 1:
                    print('found multiple matching ids')
                    print(id)
                    print(matching_proteins)
                for match in matching_proteins:
                    pmids = row[1].split(' ')
                    for pmid in pmids:
                        inserts.append((match, pmid, None, sourceName))
            else:
                print("couldn't match : " + id)
            if len(inserts) > 0:
                mysqlserver = common.getMysqlConnector()
                mysqlserver.insert_many_rows(f'{schemaname}.protein2pubmed', inserts, target_fields=('protein_id','pubmed_id', 'gene_id', 'source'))
                if testing and count > 10:
                    return


with DAG(
        '4-process-jl-mentions-job',
        default_args={
            'depends_on_past': False,
            'email': ['keith.kelleher@ncats.nih.gov'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 0,
            'retry_delay': timedelta(minutes=5),
        },
        description='Processes links from proteins to publications as retrieved from JensenLab.',
        schedule_interval=None,
        start_date=datetime(2022, 9, 1),
        catchup=False,
        tags=['TCRD', 'Pharos', 'IDG', 'JensenLab', 'Build from Scratch'],
) as dag:

    clearOldData = MySqlOperator(
        dag=dag,
        task_id=f"clear-old-jl-mentions",
        sql=f"DELETE FROM `protein2pubmed` WHERE source = '{sourceName}';",
        mysql_conn_id=mysqlConnectorID,
        database=schemaname
    )

    processDataTask = PythonOperator(
        dag=dag,
        task_id=f"process-jl-mentions",
        python_callable=processData
    )

    saveMetadataTask = common.getSaveMetadataTask(inputKey, dag)

    clearOldData >> processDataTask >> saveMetadataTask