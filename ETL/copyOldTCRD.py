import os
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from util import doVersionInserts, version_is_same, getMysqlConnector, getBaseDirectory
from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow import DAG
import pendulum


DAG_NAME = 'copy-old-tcrd'
copySchema = Variable.get('CopyTCRD')
schemaname = 'tcrdinfinity'
mysqlConnectorID = schemaname
directory = os.path.dirname(__file__) + '/'

def check_input_versions(**kwargs):
    inputKey = kwargs['input']
    table = kwargs['table']
    doFullRebuild = Variable.get('FullRebuild')
    if (doFullRebuild == 'True'):
        print('doing full rebuild')
        return 'rebuild-' + inputKey + '-tables'

    print('checking versions for ' + inputKey)
    if (version_is_same(inputKey)):
        return 'bash-dump-' + table
    return 'rebuild-' + inputKey + '-tables'


def saveMetadata(**kwargs):
    inputKey = kwargs['input']
    doVersionInserts(inputKey)

def getTables():
    mysqlserver = getMysqlConnector()

    etlTables = [
        'gtex', 'gtex-sample', 'gtex-subject', 'expression', 'ncats_unfiltered_counts', 'tissue', 'uberon_ancestry'
    ]

    unusedTables = [
        "clinvar",
        "clinvar_phenotype",
        'expression_type',
        "clinvar_phenotype_xref",
        "compartment",
        "compartment_type",
        "do_xref",
        "feature",
        "homologene",
        "idg_evol",
        "knex_migrations",
        "knex_migrations_lock",
        "knex_migrations_post_deploy_dev",
        "knex_migrations_post_deploy_dev_lock",
        "knex_migrations_post_deploy_prod",
        "knex_migrations_post_deploy_prod_lock",
        "mlp_assay_info",
        "ppi",
        "rat_qtl",
        "rat_term",
        "rdo",
        "rdo_xref",
        "tdl_update_log",
        "techdev_contact",
        "techdev_info",
        "tinx_articlerank",
    ]
    tables = [vals[0] for vals in mysqlserver.get_records(f"""
        SELECT TABLE_NAME
        FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_TYPE='BASE TABLE'
        and TABLE_SCHEMA='{copySchema}'
    """) if vals[0] not in unusedTables and vals[0] not in etlTables]
    return " ".join(tables)
    # return "protein xref uberon t2tc target uberon_xref uberon_parent tdl_info ncats_dataSource_map ncats_dataSource info_type data_type"

def createCopyTCRDDag(parent_dag_name, child_task_id, args):
    mysqlserver = getMysqlConnector()
    connection = mysqlserver.get_connection(schemaname)
    dag_subdag = DAG(
        dag_id=f'{parent_dag_name}.{child_task_id}',
        default_args=args,
        start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
        catchup=False,
        schedule_interval=None,
        tags=['TCRD', 'Pharos', 'IDG']
    )

    dump_file = getBaseDirectory() + 'tcrd-dump.sql'

    bash_dump = BashOperator(
        dag=dag_subdag,
        task_id='bash-dump-tcrd',
        bash_command=f"""mysqldump --single-transaction=TRUE --column-statistics=0 --set-gtid-purged=OFF -h{connection.host} \
        -u{connection.login} -p"{connection.password}" --databases {copySchema} --tables {getTables()} > {dump_file}""",
    )

    bash_load = BashOperator(
        dag=dag_subdag,
        task_id='bash-load-tcrd',
        bash_command=f'mysql -h{connection.host} -u{connection.login} -p"{connection.password}" tcrdinfinity < {dump_file}'
    )

    truncateSchema = MySqlOperator(
        dag=dag_subdag,
        task_id='start-fresh',
        sql=f"""DROP SCHEMA IF EXISTS `tcrdinfinity`;
                CREATE SCHEMA `tcrdinfinity`;""",
        mysql_conn_id=mysqlConnectorID
    )

    truncateSchema >> bash_load
    bash_dump >> bash_load
    return dag_subdag

dag = createCopyTCRDDag('standalone', 'copy-old-tcrd', {"retries": 0})