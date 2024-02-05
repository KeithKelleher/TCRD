import csv, sys
import os
import scipy.stats
import numpy as np
from airflow.models import Variable
from airflow.providers.http.hooks.http import HttpHook

sys.path += [ os.path.dirname(__file__) ]
sys.path += [ os.path.dirname(__file__) + '/FullRebuild' ]
from FullRebuild.models.common import common

directory = os.path.dirname(__file__) + '/'

schemaname = common.getNewDatabaseName()

def getTissueMap(manual_file):
    mysqlserver = common.getMysqlConnector()
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
    mysqlserver = common.getMysqlConnector()
    mappings = mysqlserver.get_records(f"""
        SELECT 
            CONCAT(db, ':', value), GROUP_CONCAT(uberon_xref.uid)
        FROM
            {schemaname}.uberon_xref,
            {schemaname}.uberon
        WHERE
            uberon.uid = uberon_xref.uid
                AND db = 'bto'
                AND uberon.uid IN (SELECT DISTINCT
                    uid
                FROM
                    {schemaname}.uberon_xref
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
    mysqlserver = common.getMysqlConnector()
    aliasData = mysqlserver.get_records(f"""
        SELECT value, group_concat(DISTINCT protein_id) 
        FROM {schemaname}.alias
        WHERE type = '{type}'
        and protein_id is not NULL
        GROUP BY value
        UNION SELECT value, group_concat(DISTINCT protein_id) 
        FROM {schemaname}.xref
        WHERE xtype = '{type}'
        and protein_id is not NULL
        GROUP BY value
    """)
    return dict((value.split('.')[0], list.split(',')) for (value, list) in aliasData)

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
    return []



if __name__ == '__main__':
    pass
    # print(doVersionInserts("GTEx"))
    # print(doVersionInserts('Expression'))
    # print(version_is_same("GTEx"))
    # print(version_is_same("Expression"))



def calculateOneTissueSpecificity(list):
    if np.max(list) == 0:
        return 0
    return np.sum(1 - (list / np.max(list))) / (len(list)-1)


def getFilteredSampleTissueMap(): # remove samples with Moderate or Severe autolysis (SMATSSCR)
    mysqlserver = common.getMysqlConnector()
    records = mysqlserver.get_records(f"""SELECT distinct SAMPID, SMTSD, SMUBRID FROM {schemaname}.`gtex_sample` WHERE SMATSSCR < 2""")
    return dict((x,(y,z)) for x,y,z in records)


def getFilteredSexMap(): # remove subjects where death_hardy > 2
    mysqlserver = common.getMysqlConnector()
    records = mysqlserver.get_records(f"""SELECT subject_id, sex FROM {schemaname}.`gtex_subject` WHERE death_hardy <= 2""")
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
    mysqlserver = common.getMysqlConnector()
    mysqlserver.insert_many_rows(f'{schemaname}.tdl_info', inserts,
                            target_fields=('itype', 'protein_id', 'number_value'))

def get_dataSource_inserts(proteinMap, dsKey):
    inserts = []
    for protein_id in proteinMap:
        inserts.append((dsKey, protein_id))
    return inserts