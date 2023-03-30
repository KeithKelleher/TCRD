import os
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow import DAG
import pendulum

copyDB = Variable.get('CopyDB')

from FullRebuild.models.common import common
sqlFiles = common.getSqlFiles()
DAG_NAME = 'copy-database'
mysqlConnectorID = 'aws_conn'

mysqlserver = MySqlHook(mysqlConnectorID)

directory = os.path.dirname(__file__) + '/'

def getTables():
    etlTables = [
        'gtex', 'gtex-sample', 'gtex-subject', 'expression', 'tissue',
        'uberon_ancestry', 'pubmed', 'ancestry_do', 'ancestry_dto', 'ancestry_mondo', 'ancestry_uberon',
        'generif', 'generif2pubmed', 'input_version', 'protein2pubmed', 'uberon', 'uberon_ancestry',
        'uberon_parent', 'uberon_xref', 'tinx_importance', 'tinx_disease', 'tinx_novelty'
    ]

    unusedTables = [
        "clinvar", "clinvar_phenotype", 'expression_type', "clinvar_phenotype_xref", "compartment", "ncats_unfiltered_counts",
        "compartment_type", "do_xref", "feature", "homologene", "idg_evol", "knex_migrations",
        "knex_migrations_lock", "tinx_articlerank", "tinx_nds_rank", "knex_migrations_post_deploy_dev",
        "knex_migrations_post_deploy_dev_lock", "knex_migrations_post_deploy_prod", "ncats_generif_pubmed_map",
        "knex_migrations_post_deploy_prod_lock", "mlp_assay_info", "ppi", "rat_qtl", "rat_term",
        "rdo", "rdo_xref", "tdl_update_log", "techdev_contact", "techdev_info"
    ]

    tables = [vals[0] for vals in mysqlserver.get_records(f"""
        SELECT TABLE_NAME
        FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_TYPE='BASE TABLE'
        and TABLE_SCHEMA='{copyDB}'
    """) if vals[0] not in unusedTables and vals[0] not in etlTables]
    return " ".join(tables)

def createDumpDBDag(child_task_id, args):
    connection = mysqlserver.get_connection(mysqlConnectorID)
    dag = DAG(
        dag_id=f'{child_task_id}',
        default_args=args,
        start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
        catchup=False,
        schedule_interval=None,
        tags=['TCRD', 'Pharos', 'IDG', 'Copy Database', 'Utility']
    )

    dump_file = directory + 'tcrd-dump.sql'
    bash_dump = BashOperator(
        dag=dag,
        task_id='bash-dump-database',
        bash_command=f'mysqldump --single-transaction=TRUE --column-statistics=0 '
                     f'--set-gtid-purged=OFF -h{connection.host} -u{connection.login} '
                     f'-p"{connection.password}" --databases {copyDB} '
                     f'--tables {getTables()} > {dump_file}',
    )

    return dag

dag = createDumpDBDag('2b-dump-copyDB', {"retries": 0})