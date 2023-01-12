import csv, sys
import os
import types
import scipy.stats
from contextlib import closing
from os.path import exists
import numpy as np
from airflow.models import Variable
from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.providers.mysql.operators.mysql import MySqlOperator

sys.path += [ os.path.dirname(__file__) ]
sys.path += [ os.path.dirname(__file__) + '/FullRebuild' ]
from FullRebuild.models.common import common

directory = os.path.dirname(__file__) + '/'
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





def getVersionInfoFields():
    return ('source_key', 'data_source', 'file_key', 'file', 'version', 'release_date', 'download_date')

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
    return getAliasMap('Ensembl')

def getRefSeqMap():
    return getAliasMap('RefSeq')

def getAliasMap(type):
    mysqlserver = getMysqlConnector()
    aliasData = mysqlserver.get_records(f"""
        SELECT value, group_concat(DISTINCT protein_id) 
        FROM {schemaname}.alias
        WHERE type = '{type}'
        GROUP BY value
    """)
    return dict((value, list.split(',')) for (value, list) in aliasData)


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


def saveTissueSpecificityToTDLInfo(inserts):
    mysqlserver = getMysqlConnector()
    mysqlserver.insert_many_rows('tdl_info', inserts,
                            target_fields=('itype', 'protein_id', 'number_value'))


def get_dataSource_inserts(proteinMap, dsKey):
    inserts = []
    for protein_id in proteinMap:
        inserts.append((dsKey, protein_id))
    return inserts

def getDropFKTask(dag, schemaname, mysqlConnectorID, tablename, foreignKeyName):
    return MySqlOperator(
        dag=dag,
        task_id=f'remove-fk-{foreignKeyName}', # input_version keeps track of the data source now
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
                AND `table_name` = '{tablename}'
                AND `constraint_name` = '{foreignKeyName}'
                AND `constraint_type` = 'FOREIGN KEY'
            ;
            SET @statement := IF(
                @FOREIGN_KEY_my_foreign_key_ON_TABLE_my_table_EXISTS > 0,
                -- 'SELECT "info: foreign key exists."',
                'ALTER TABLE {tablename} DROP FOREIGN KEY {foreignKeyName}',
                'SELECT "info: foreign key does not exist."'
            );
            PREPARE statement FROM @statement;
            EXECUTE statement;
            """,
        mysql_conn_id=mysqlConnectorID,
        database=schemaname
    )