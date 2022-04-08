import os
from datetime import datetime, timedelta
from steps.loadUniProt import loadUniprot
from sql.loadSqlFiles import getSqlFiles
# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
from airflow.operators.subdag import SubDagOperator
from airflow.operators.python import PythonOperator
from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.utils.task_group import TaskGroup
# from createSchema import getCreateSchemaDAG

sqlFiles = getSqlFiles()
schemaname = 'tcrdinfinity'
mysqlConnectorID = schemaname

def doNothing(**kwargs):
    print(kwargs['task'])
    return 1

with DAG(
        'populate-tcrd',
        # These args will get passed on to each operator
        # You can override them on a per-task basis during operator initialization
        default_args={
            'depends_on_past': False,
            'email': ['keith.kelleher@ncats.nih.gov'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 0,
            'retry_delay': timedelta(minutes=5),
            # 'queue': 'bash_queue',
            # 'pool': 'backfill',
            # 'priority_weight': 10,
            # 'end_date': datetime(2016, 1, 1),
            # 'wait_for_downstream': False,
            # 'sla': timedelta(hours=2),
            # 'execution_timeout': timedelta(seconds=300),
            # 'on_failure_callback': some_function,
            # 'on_success_callback': some_other_function,
            # 'on_retry_callback': another_function,
            # 'sla_miss_callback': yet_another_function,
            # 'trigger_rule': 'all_success'
        },
        description='An automated script to build TCRD',
        schedule_interval=timedelta(days=28),
        start_date=datetime(2022, 4, 1),
        catchup=False,
        tags=['TCRD', 'Pharos', 'IDG'],
) as dag:

    # createDatabase = SubDagOperator(
    #     dag=dag,
    #     task_id='create-tcrdinfinity',
    #     subdag=getCreateSchemaDAG()
    # )

    loadUniprot = PythonOperator(
        dag=dag,
        task_id='loadUniprot',
        python_callable=loadUniprot
    )

    loadUberon = PythonOperator(
        dag=dag,
        task_id='loadUberon',
        python_callable=doNothing
    )

    createUberonTable = PythonOperator(
        dag=dag,
        task_id='createUberonTable',
        python_callable=doNothing
    )
    with TaskGroup("loadExpression", tooltip="Create Tables for UniProt Data") as loadExpression:
        createExpressionTable = PythonOperator(
            dag=dag,
            task_id='createExpressionTable',
            python_callable=doNothing
        )
        loadHPA = PythonOperator(
            dag=dag,
            task_id='loadHPA',
            python_callable=doNothing
        )
        loadHPM = PythonOperator(
            dag=dag,
            task_id='loadHPM',
            python_callable=doNothing
        )
        loadGTeX = PythonOperator(
            dag=dag,
            task_id='loadGTeX',
            python_callable=doNothing
        )
        loadTISSUES = PythonOperator(
            dag=dag,
            task_id='loadTISSUES',
            python_callable=doNothing
        )
        createExpressionTable >> [loadHPA, loadHPM, loadGTeX, loadTISSUES]

    loadHarmonizome = PythonOperator(
        dag=dag,
        task_id='loadHarmonizome',
        python_callable=doNothing
    )

    loadKeggPathways = PythonOperator(
        dag=dag,
        task_id='loadKeggPathways',
        python_callable=doNothing
    )

    loadKeggDistances = PythonOperator(
        dag=dag,
        task_id='loadKeggDistances',
        python_callable=doNothing
    )

    # createDatabase >> loadUniprot
    loadUniprot >> [loadExpression, loadHarmonizome, loadKeggPathways]
    createUberonTable >> loadUberon >> [loadExpression]
    loadKeggPathways >> loadKeggDistances

# t2 = BashOperator(
    #     task_id='sleep',
    #     depends_on_past=False,
    #     bash_command='sleep 5',
    #     retries=3,
    # )
    # t1.doc_md = dedent(
    #     """\
    # #### Task Documentation
    # You can document your task using the attributes `doc_md` (markdown),
    # `doc` (plain text), `doc_rst`, `doc_json`, `doc_yaml` which gets
    # rendered in the UI's Task Instance Details page.
    # ![img](http://montcs.bloomu.edu/~bobmon/Semesters/2012-01/491/import%20soul.png)
    #
    # """
    # )

    # dag.doc_md = __doc__  # providing that you have a docstring at the beginning of the DAG
    # dag.doc_md = """
    # This is a documentation placed anywhere
    # """  # otherwise, type it like this
    # templated_command = dedent(
    #     """
    # {% for i in range(5) %}
    #     echo "{{ ds }}"
    #     echo "{{ macros.ds_add(ds, 7)}}"
    # {% endfor %}
    # """
    # )
    #
    # t3 = BashOperator(
    #     task_id='templated',
    #     depends_on_past=False,
    #     bash_command=templated_command,
    # )
    #
    # t1 >> [t2, t3]
