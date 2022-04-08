* install airflow according to instructions here:  https://airflow.apache.org/docs/apache-airflow/stable/start/local.html
* add connection to MySQL database where you will build your TCRD
  * make sure mysql operator is installed
    * pip3 install apache-airflow-providers-mysql
* run airflow standalone
  * create a connection called 'tcrdinfinity' using MySQL, and your connection settings (i.e. host, user, password, port)
  * create a variable calledd 'NewTCRDName' - the DAG called tcrd-snapshot will use that as the renamed tcrdinfinity
* run some DAGs as appropriate - TODO - fill out with details on which DAGs exist