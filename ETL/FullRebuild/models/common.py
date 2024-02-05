from os.path import exists

from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from contextlib import closing
import types
import os, csv, time
from datetime import date, datetime

from airflow.utils.trigger_rule import TriggerRule

buildDirectory = os.path.dirname(__file__) + '/../'

class common:
    @staticmethod
    def getGenericConnectionName():
        return 'aws_conn'

    @staticmethod
    def getTinxConnectionName():
        return 'tinx_conn'

    @staticmethod
    def getNewDatabaseName():
        return Variable.get('NewDB')

    @staticmethod
    def findMatches(proteinObj, listField, dataField, matchString):
        if listField in proteinObj:
            return filter(lambda match: match[dataField] == matchString, proteinObj[listField])
        return iter([])

    @staticmethod
    def getMysqlConnector():
        mysqlserver = MySqlHook(common.getGenericConnectionName())
        mysqlserver.insert_many_rows = types.MethodType(insert_many_rows, mysqlserver)
        return mysqlserver

    @staticmethod
    def getTinxConnector():
        tinxServer = MySqlHook(common.getTinxConnectionName())
        return tinxServer

    @staticmethod
    def getSqlFiles():
        fileDictionary = {}
        directory = os.path.dirname(__file__) + '/../../sql/'
        files = [f for f in os.listdir(directory) if os.path.isfile(directory + f) and f.endswith('.sql')]
        for f in files:
            fd = open(directory + f, 'r')
            sql = fd.read()
            fd.close()
            fileDictionary[f] = sql
        return fileDictionary

    @staticmethod
    def getFileVersions(inputKey):
        input_directory = buildDirectory + 'input_files/' + inputKey + '/'
        version_file = input_directory + 'version.csv'
        if (exists(version_file)):
            return getSourceDetailsFromFile(inputKey, '', version_file)
        else:
            version_dict = {}
            for innerKey in os.listdir(input_directory):
                if os.path.isdir(input_directory + innerKey):
                    version_dict = version_dict | getSourceDetailsFromFile(inputKey, innerKey, input_directory + innerKey + '/version.csv')
        return (version_dict)

    @staticmethod
    def getConfigFile(inputKey, innerKey, fieldKey):
        configMap = common.getFileVersions(inputKey)
        if (innerKey is None or innerKey == ""):
            relative_path = configMap[inputKey + '-' + fieldKey]['file']
            return buildDirectory + f'/input_files/{inputKey}/{relative_path}'
        relative_path = configMap[innerKey + '-' + fieldKey]['file']
        return buildDirectory + f'/input_files/{inputKey}/{innerKey}/{relative_path}'

    @staticmethod
    def getSaveMetadataTask(inputKey, dag):
        taskkey = inputKey.replace(' ','-')
        return PythonOperator(
            dag=dag,
            task_id='save-' + taskkey + '-metadata',
            python_callable=saveMetadata,
            op_kwargs={'input': inputKey},
            trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
        )

    @staticmethod
    def saveFetchMetadata(source_key, data_source, file_key, file, version, release_date, download_date):
        mysqlserver = common.getMysqlConnector()
        mysqlserver.insert_rows(f'{common.getNewDatabaseName()}.input_version', [(
            source_key,
            data_source,
            file_key,
            file,
            version,
            release_date,
            download_date
        )], getVersionInfoFields(), replace=True)

    @staticmethod
    def getchunks(l, n):
        n = max(1, n)
        return (l[i:i+n] for i in range(0, len(l), n))

def saveMetadata(**kwargs):
    inputKey = kwargs['input']
    doVersionInserts(inputKey)

def doVersionInserts(input_key):
    inserts = getVersionInserts(input_key)
    mysqlserver = common.getMysqlConnector()
    mysqlserver.insert_rows(f'{common.getNewDatabaseName()}.input_version', inserts, getVersionInfoFields(), replace=True)

def getVersionInfoFields():
    return ('source_key', 'data_source', 'file_key', 'file', 'version', 'release_date', 'download_date')

def getVersionInserts(input_key):
    version_info = common.getFileVersions(input_key)
    return [(
        input_key,
        key.split('-')[0],
        key.split('-')[1],
        version_info[key]['file'],
        version_info[key]['version'] if len(version_info[key]['version']) else None,
        version_info[key]['release_date'],
        version_info[key]['download_date'])
        for key in version_info
    ]

def getSourceDetailsFromFile(input_key, inner_key, version_file):
    with open(version_file, 'r') as csvFile:
        csvReader = csv.reader(csvFile, delimiter=',')
        data_source_info = dict((rows[0], rows[1]) for rows in csvReader)
    version = data_source_info['version']
    if ('release date' in data_source_info):
        release_date = datetime.fromisoformat(data_source_info['release date']).date()
    else:
        release_date = None
    return dict(
        ((inner_key if len(inner_key) > 0 else input_key) + '-' + key,
         {
             'file': data_source_info[key],
             'version': version,
             'release_date': release_date,
             'download_date': getDownloadDate(input_key, inner_key, data_source_info[key])
         }) for key in data_source_info if key != 'version' and key != 'release date'
    )

def getDownloadDate(input_key, inner_key, file):
    input_directory = buildDirectory + 'input_files/' + input_key + '/' + (inner_key + '/' if len(inner_key)> 0 else '')
    download_date = os.path.getmtime(input_directory + file)
    return date.fromtimestamp(download_date)

def insert_many_rows(self, table, rows, target_fields=None, replace=False, **kwargs):
    """
    An rewrite of MySqlHook.insert_rows that uses executemany, since it's like a million faster
    """
    count = 0
    i = 0
    while count < 3:
        try:
            count += 1
            with closing(self.get_conn()) as conn:
                if self.supports_autocommit:
                    self.set_autocommit(conn, False)
                conn.commit()
                with closing(conn.cursor()) as cur:
                    chunks = common.getchunks(rows, 5000)
                    for chunk in chunks:
                        sql = self._generate_insert_sql(table, chunk[0], target_fields, replace, **kwargs)
                        self.log.debug("Generated sql: %s", sql)
                        print(sql)
                        cur.executemany(sql, chunk)
                conn.commit()
                return True
        except Exception as e:
            print(f"Insert Failed: {table}")
            print(f"attempt {count}")
            if count >= 3:
                raise e
            time.sleep(30)
    return False



def tryInsert(inserts, maxAttempts = 3):
    count = 0
    mysqlserver = common.getMysqlConnector()
    while count < maxAttempts:
        try:
            count += 1
            mysqlserver.insert_many_rows(f'{schemaname}.protein2pubmed', inserts, target_fields=('protein_id','pubmed_id', 'gene_id', 'source'))
            return True
        except Exception as e:
            print(f"Insert Failed: {inserts}")
            print(f"attempt {count}")
            time.sleep(30)
    return False
