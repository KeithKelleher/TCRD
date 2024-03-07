import sys
import os
import time

sys.path += [ os.path.dirname(__file__) ]
sys.path += [ os.path.dirname(__file__) + '/models' ]
sys.path += [ os.path.dirname(__file__) + '/compute' ]

from contextlib import closing

from FullRebuild.util import getDropFKTask
from FullRebuild.models.ncbiQueue import NcbiQueue
from datetime import datetime, timedelta
from FullRebuild.models.common import common
from FullRebuild.compute.tdl_computer import tdl_computer

from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow import DAG
from airflow.operators.python import PythonOperator

sqlFiles = common.getSqlFiles()
schemaname = common.getNewDatabaseName()
mysqlConnectorID = common.getGenericConnectionName()

testing = True
fetch_time = datetime.now()
formatted_fetch_time = fetch_time.strftime('%Y-%m-%d %H:%M:%S')

mysqlserver = common.getMysqlConnector()
databaseName = common.getNewDatabaseName()

def calculateTDLs():
    computer = tdl_computer(mysqlserver, databaseName)
    data = computer.calculateAllTDLs()
    rowData = [(obj['tdl'], key) for key, obj in data.items()]
    query = f"""
        UPDATE {databaseName}.target,
            {databaseName}.t2tc,
            {databaseName}.protein 
        SET 
            tdl = %s
        WHERE
            protein.uniprot = %s
            and t2tc.target_id = target.id
            and t2tc.protein_id = protein.id"""
    print(len(rowData))
    with closing(mysqlserver.get_conn()) as conn:
        with closing(conn.cursor()) as cur:
            cur.executemany(query, rowData)
        conn.commit()


with DAG(
        'last-calculate-TDLs',
        default_args={
            'depends_on_past': False,
            'email': ['keith.kelleher@ncats.nih.gov'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 0,
            'retry_delay': timedelta(minutes=5),
        },
        description='Calculates TDL levels based on TCRD data.',
        schedule_interval=None,
        start_date=datetime(2022, 9, 1),
        catchup=False,
        tags=['TCRD', 'Pharos', 'IDG', 'UniProt', 'Target Development Level'],
) as dag:

    calcTDL =  PythonOperator(
        dag=dag,
        python_callable=calculateTDLs,
        task_id='fetch-calculate-save-tdls'
    )

    calcMaxTDLforDiseases = MySqlOperator(
        dag=dag,
        database=schemaname,
        task_id='calculate-max-tdl-for-diseases',
        mysql_conn_id=mysqlConnectorID,
        sql=f"""
    UPDATE `ncats_disease` AS `ncats_disease`
        INNER JOIN
    (SELECT 
        `ncats_disease`.`id` AS `id`,
            MAX(CASE
                WHEN target.tdl = 'Tclin' THEN 4
                WHEN target.tdl = 'Tchem' THEN 3
                WHEN target.tdl = 'Tbio' THEN 2
                ELSE 1
            END) AS `tempTDL`
    FROM
        `ncats_disease` AS `ncats_disease`, `ncats_d2da` AS `ncats_d2da`, `disease` AS `disease`, `t2tc` AS `t2tc`, `target` AS `target`
    WHERE
        `ncats_disease`.`id` = ncats_d2da.ncats_disease_id
            AND `ncats_d2da`.`disease_assoc_id` = disease.id
            AND `disease`.`protein_id` = t2tc.protein_id
            AND `t2tc`.`target_id` = target.id
    GROUP BY `ncats_disease`.`id`) AS `subQ` ON `subQ`.`id` = `ncats_disease`.`id` 
SET 
    `maxTDL` = CASE
        WHEN subQ.tempTDL = 4 THEN 'Tclin'
        WHEN subQ.tempTDL = 3 THEN 'Tchem'
        WHEN subQ.tempTDL = 2 THEN 'Tbio'
        ELSE 'Tdark'
    END
where ncats_disease.id > 0""")

    calcTDL >> calcMaxTDLforDiseases




