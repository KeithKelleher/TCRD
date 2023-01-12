import sys, os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.mysql.operators.mysql import MySqlOperator

sys.path += [ os.path.dirname(__file__) ]
sys.path += [ os.path.dirname(__file__) + '/FullRebuild' ]
from FullRebuild.models.common import common

sqlFiles = common.getSqlFiles()
schemaname = common.getNewDatabaseName()

directory = os.path.dirname(__file__) + '/'

fetch_time = datetime.now()
formatted_fetch_time = fetch_time.strftime('%Y-%m-%d %H:%M:%S')

# when the

with DAG(
        '6-build-tinx',
        default_args={
            'depends_on_past': False,
            'email': ['keith.kelleher@ncats.nih.gov'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 0,
            'retry_delay': timedelta(minutes=5),
        },
        description='Rebuild the Tin-X tables based on data in the live Tin-X database on AWS',
        schedule_interval=None,
        start_date=datetime(2022, 4, 1),
        catchup=False,
        tags=['TCRD', 'Pharos', 'IDG', 'Tin-X'],
) as dag:
    mysqlConnectorID = common.getGenericConnectionName()
    tinxConnectorID = common.getTinxConnectionName()

    dropTables = MySqlOperator(
        dag=dag,
        task_id='drop-tinx-tables',
        sql=f"""
            DROP TABLE IF EXISTS `tinx_importance`;
            DROP TABLE IF EXISTS `tinx_disease`;
            DROP TABLE IF EXISTS `tinx_novelty`;
            """,
        mysql_conn_id=mysqlConnectorID,
        database=schemaname
    )

    tcrdServer = common.getMysqlConnector()
    tcrdConnection = tcrdServer.get_connection(mysqlConnectorID)

    tinxServer = common.getTinxConnector()
    tinxConnection = tinxServer.get_connection(tinxConnectorID)

    dump_file = directory + 'tinx-dump.sql'
    bash_dump = BashOperator(
        dag=dag,
        task_id='bash-dump-tinx',
        bash_command=f'mysqldump --single-transaction=TRUE --column-statistics=0 '
                     f'--set-gtid-purged=OFF -h{tinxConnection.host} -u{tinxConnection.login} '
                     f'-p"{tinxConnection.password}" --databases tcrd '
                     f'--tables tinx_importance tinx_disease tinx_novelty > {dump_file}',
    )

    bash_load = BashOperator(
        dag=dag,
        task_id='bash-load-tinx',
        bash_command=f'mysql -h{tcrdConnection.host} -u{tcrdConnection.login} '
                     f'-p"{tcrdConnection.password}" {schemaname} < {dump_file}'
    )

    add_indexes = MySqlOperator(
        dag=dag,
        task_id='add-pharos-indexes',
        sql=f"""
            ALTER TABLE `tinx_disease` 
            ADD FULLTEXT INDEX `tinx_disease_text_idx` (`name`, `summary`);
            ALTER TABLE `tinx_novelty` 
            ADD INDEX `tinx_novelty_idx3` (`protein_id` ASC, `score` ASC);
        """,
        mysql_conn_id=mysqlConnectorID,
        database=schemaname
    )

    # saveTinXMetadata = PythonOperator(
    #     dag=dag,
    #     task_id='save-tinx-metadata',
    #     python_callable=common.saveFetchMetadata,
    #     op_args=("TinX", "TinX", "Tin-X Associations",
    #              None, None, None, formatted_fetch_time)
    # )

    [dropTables, bash_dump] >> bash_load >> add_indexes \
    # >> saveTinXMetadata
    # bash_load >> saveTinXMetadata ## since the data we use isn't updated yet, let's figure out another way to determine the version
