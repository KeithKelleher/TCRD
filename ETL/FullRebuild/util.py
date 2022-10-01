import csv
import os
import types
import scipy.stats
from contextlib import closing
from datetime import date
from os.path import exists
import numpy as np
from airflow.models import Variable
from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.mysql.hooks.mysql import MySqlHook

directory = os.path.dirname(__file__) + '/'
copySchema = Variable.get('CopyTCRD')
schemaname = 'tcrdinfinity'

def getBaseDirectory():
    return directory

def getSqlFiles():
    fileDictionary = {}
    directory = os.path.dirname(__file__) + '/sql/'
    files = [f for f in os.listdir(directory) if os.path.isfile(directory + f) and f.endswith('.sql')]
    for f in files:
        fd = open(directory + f, 'r')
        sql = fd.read()
        fd.close()
        fileDictionary[f] = sql
    return fileDictionary

def insert_many_rows(self, table, rows, target_fields=None, replace=False, **kwargs):
    """
    An rewrite of MySqlHook.insert_rows that uses execute_many, since it's like a million faster
    """
    i = 0
    with closing(self.get_conn()) as conn:
        if self.supports_autocommit:
            self.set_autocommit(conn, False)
        conn.commit()
        with closing(conn.cursor()) as cur:
            chunks = getchunks(rows, 5000)
            for chunk in chunks:
                sql = self._generate_insert_sql(table, chunk[0], target_fields, replace, **kwargs)
                self.log.debug("Generated sql: %s", sql)
                cur.executemany(sql, chunk)
        conn.commit()

def getchunks(l, n):
    n = max(1, n)
    return (l[i:i+n] for i in range(0, len(l), n))

def getMysqlConnector():
    mysqlserver = MySqlHook('tcrdinfinity')
    mysqlserver.insert_many_rows = types.MethodType(insert_many_rows, mysqlserver)
    return mysqlserver

def getHttpConnector():
    httpConnection = HttpHook(http_conn_id='ebi-ontology-mapper')
    return httpConnection



def getFileVersions(inputKey):
    input_directory = directory + 'input_files/' + inputKey + '/'
    version_file = input_directory + 'version.csv'
    if (exists(version_file)):
        return getSourceDetailsFromFile(inputKey, '', version_file)
    else:
        version_dict = {}
        for innerKey in os.listdir(input_directory):
            if os.path.isdir(input_directory + innerKey):
                version_dict = version_dict | getSourceDetailsFromFile(inputKey, innerKey, input_directory + innerKey + '/version.csv')
        return (version_dict)

def getDownloadDate(input_key, inner_key, file):
    input_directory = directory + 'input_files/' + input_key + '/' + (inner_key + '/' if len(inner_key)> 0 else '')
    download_date = os.path.getmtime(input_directory + file)
    return date.fromtimestamp(download_date)

def getSourceDetailsFromFile(input_key, inner_key, version_file):
    with open(version_file, 'r') as csvFile:
        csvReader = csv.reader(csvFile, delimiter=',')
        data_source_info = dict((rows[0], rows[1]) for rows in csvReader)
    version = data_source_info['version']
    if ('release date' in data_source_info):
        release_date = date.fromisoformat(data_source_info['release date'])
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

def getVersionInfoFields():
    return ('source_key', 'data_source', 'file_key', 'file', 'version', 'release_date', 'download_date')



def getVersionInserts(input_key):
    version_info = getFileVersions(input_key)
    return [(
        input_key,
        key.split('-')[0],
        key.split('-')[1],
        version_info[key]['file'],
        version_info[key]['version'],
        version_info[key]['release_date'],
        version_info[key]['download_date'])
        for key in version_info
    ]

def doVersionInserts(input_key):
    inserts = getVersionInserts(input_key)
    mysqlserver = getMysqlConnector()
    mysqlserver.insert_rows('input_version', inserts, getVersionInfoFields())

def getDBVersions(input_key):
    fields = ','.join(field for field in getVersionInfoFields())
    mysqlserver = getMysqlConnector()
    try:
        rows =  mysqlserver.get_records(f"""SELECT {fields} from {copySchema}.input_version where source_key = '{input_key}'""")
    except:
        return []
    return dict((
        row[1] + '-' + row[2],
        {
            'file': row[3],
            'version': row[4],
            'release_date': row[5],
            'download_date': row[6]
        }
    ) for row in rows)

def version_is_same(input_key):
    fileVersions = getFileVersions(input_key)
    dbVersions = getDBVersions(input_key)
    if (len(fileVersions) == 0):
        raise Exception('no file versions found')
    if (len(dbVersions) == 0):
        print('No DB versions found : first time for ETL')
        return False
    for fileKey in fileVersions:
        if fileKey in dbVersions:
            fileInfo = fileVersions[fileKey]
            dbInfo = dbVersions[fileKey]
            for info in fileInfo:
                if info in dbInfo:
                    if fileInfo[info] != dbInfo[info]:
                        print (info + ' doesnt match')
                        print (fileInfo[info])
                        print (dbInfo[info])
                        return False
                else:
                    print ('copy DB doesnt have value for ' + info)
        else:
            print('missing input file in copy DB ' + fileKey)
            return False
    print('versions match')
    return True

def getTissueMap(manual_file):
    mysqlserver = getMysqlConnector()
    uberon_table = mysqlserver.get_records(f"""
        SELECT name, uid from {schemaname}.uberon
    """)
    tissueMap = dict((tissue.lower(), uberon_id) for (tissue, uberon_id) in uberon_table)
    with open(manual_file) as mapFile:
        mapFile = csv.DictReader(mapFile, delimiter="\t", fieldnames=["tissue", "uberon_id"])
        return tissueMap | dict((row["tissue"].lower(), row["uberon_id"]) for row in mapFile)
    return tissueMap | getManualMap(manual_file)

def getManualMap(manual_file):
    with open(manual_file) as mapFile:
        mapFile = csv.DictReader(mapFile, delimiter="\t", fieldnames=["tissue", "uberon_id"])
        return dict((row["tissue"].lower(), row["uberon_id"]) for row in mapFile)

def getHumanEntities(file):
    map = {}
    with open(file) as entityFile:
        mapFile = csv.DictReader(entityFile, delimiter="\t", fieldnames=["?", "taxid", "entity"])
        for row in mapFile:
            map[row["entity"]]=1
    return map

def getOntologyMap():
    mysqlserver = getMysqlConnector()
    mappings = mysqlserver.get_records(f"""
SELECT 
    CONCAT(db, ':', value), GROUP_CONCAT(uberon_xref.uid)
FROM
    uberon_xref,
    uberon
WHERE
    uberon.uid = uberon_xref.uid
        AND db = 'bto'
        AND uberon.uid IN (SELECT DISTINCT
            uid
        FROM
            uberon_xref
        WHERE
            db = 'fma')
GROUP BY CONCAT(db, ':', value)
ORDER BY COUNT(*) DESC
    """)
    return dict((bto_id, uberons.split(',')) for (bto_id, uberons) in mappings)


def getENSGMap():
    mysqlserver = getMysqlConnector()
    xrefData = mysqlserver.get_records(f"""
        SELECT DISTINCT
            value, protein_id
        FROM
            {schemaname}.xref
        WHERE
            xtype = 'Ensembl'
            and protein_id is not null
    """)
    return dict((ensg_id, protein_id) for (ensg_id, protein_id) in xrefData)

def getRefSeqMap():
    mysqlserver = getMysqlConnector()
    xrefData = mysqlserver.get_records(f"""
        SELECT 
            SUBSTRING_INDEX(value, '.', 1),
            GROUP_CONCAT(DISTINCT protein_id)
        FROM
            {schemaname}.xref
        WHERE
            xtype = 'RefSeq'
        GROUP BY SUBSTRING_INDEX(value, '.', 1)
    """)
    return dict((refseq, list.split(',')) for (refseq, list) in xrefData)

def valOrNone(val):
    if val == '':
        return None
    return val

def lookupTissue(tissueMap, tissue):
    lower_tissue = tissue.lower()
    if (lower_tissue in tissueMap):
        return valOrNone(tissueMap[lower_tissue])
    if (" (" in lower_tissue):
        sub = lower_tissue.split(" (")[0]
        if (sub in tissueMap):
            return valOrNone(tissueMap[sub])
    if ("," in lower_tissue):
        sub = lower_tissue.split(",")[0]
        if (sub in tissueMap):
            return valOrNone(tissueMap[sub])
    return None

def lookupENSG(ensgMap, ensg_id):
    if (ensg_id in ensgMap):
        return ensgMap[ensg_id]
    return None

def getConfigFile(inputKey, innerKey, fieldKey):
    configMap = getFileVersions(inputKey)
    if (innerKey is None or innerKey == ""):
        relative_path = configMap[inputKey + '-' + fieldKey]['file']
        return os.path.dirname(__file__) + f'/input_files/{inputKey}/{relative_path}'
    relative_path = configMap[innerKey + '-' + fieldKey]['file']
    return os.path.dirname(__file__) + f'/input_files/{inputKey}/{innerKey}/{relative_path}'


if __name__ == '__main__':

    # print(doVersionInserts("GTEx"))
    print(doVersionInserts('Expression'))
    # print(version_is_same("GTEx"))
    # print(version_is_same("Expression"))



def calculateOneTissueSpecificity(list):
    if np.max(list) == 0:
        return 0
    return np.sum(1 - (list / np.max(list))) / (len(list)-1)


def getFilteredSampleTissueMap(): # remove samples with Moderate or Severe autolysis (SMATSSCR)
    mysqlserver = getMysqlConnector()
    records = mysqlserver.get_records(f"""SELECT distinct SAMPID, SMTSD, SMUBRID FROM `gtex_sample` WHERE SMATSSCR < 2""")
    return dict((x,(y,z)) for x,y,z in records)


def getFilteredSexMap(): # remove subjects where death_hardy > 2
    mysqlserver = getMysqlConnector()
    records = mysqlserver.get_records(f"""SELECT subject_id, sex FROM `gtex_subject` WHERE death_hardy <= 2""")
    return dict((x, y) for x, y in records)

def calculateRanks(list):
    maxVal = np.max(list)
    if maxVal == 0:
        return list
    ranks = scipy.stats.rankdata(list, method='average', axis=0) / len(list)
    maxim = np.max(ranks)
    minim = np.min(ranks)
    range = maxim - minim
    if range == 0:
        return ranks
    return (ranks - minim) / range


def saveTissueSpecificityToTDLInfo(inserts, itypes, descriptions):
    mysqlserver = getMysqlConnector()
    for index in range(len(itypes)):
        mysqlserver.run(f"""REPLACE INTO info_type (name, data_type, description) VALUES ('{itypes[index]}', 'Number', '{descriptions[index]}')""")

    mysqlserver.insert_many_rows('tdl_info', inserts,
                            target_fields=('itype', 'protein_id', 'number_value'))


def get_dataSource_inserts(proteinMap, dsKey):
    inserts = []
    for protein_id in proteinMap:
        inserts.append((dsKey, protein_id))
    return inserts