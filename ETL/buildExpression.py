import csv
import json
import math
import time
import pendulum

from airflow.providers.mysql.operators.mysql import MySqlOperator

from util import getSqlFiles, getTissueMap, getConfigFile, lookupTissue, getENSGMap, getMysqlConnector, lookupENSG, \
    getHumanEntities, getHttpConnector, getRefSeqMap, calculateOneTissueSpecificity, saveTissueSpecificityToTDLInfo, \
    calculateRanks
from airflow import DAG
from airflow.operators.python import PythonOperator

sqlFiles = getSqlFiles()
schemaname = 'tcrdinfinity'
mysqlConnectorID = schemaname
testing = False  # just do some rows of the input file, set to False do the full ETL
testCount = 7500

def tauStrings():
    return ["HPA RNA Tissue Specificity Index", "HPA Protein Tissue Specificity Index", "HPM Protein Tissue Specificity Index"]

def descriptions():
    return [
        'Tau as defined in Yanai, I. et. al., Bioinformatics 21(5): 650-659 (2005) calculated on HPA RNA data.',
        'Tau as defined in Yanai, I. et. al., Bioinformatics 21(5): 650-659 (2005) calculated on HPA Protein data.',
        'Tau as defined in Yanai, I. et. al., Bioinformatics 21(5): 650-659 (2005) calculated on HPM Protein data.',
    ]

def createExpressionDAG(parent_dag_name, child_task_id, args):
    dag_subdag = DAG(
        dag_id=f'{parent_dag_name}.{child_task_id}',
        default_args=args,
        start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
        catchup=False,
        schedule_interval=None,
    )

    # region Drop Tables
    dropAllTables = MySqlOperator(
        dag=dag_subdag,
        task_id='drop-all-tables',
        sql=f"""DROP TABLE IF EXISTS `expression`;
        DELETE FROM `tdl_info` WHERE itype in ('{tauStrings()[0]}','{tauStrings()[1]}','{tauStrings()[2]}')
        """,
        mysql_conn_id=mysqlConnectorID
    )
    #endregion

    createExpressionTables = MySqlOperator(
        dag=dag_subdag,
        task_id='create-expression-table',
        sql=sqlFiles['expression.sql'],
        mysql_conn_id=mysqlConnectorID
    )

    def add_ranks_and_format_inserts(proteinMap, etype):
        inserts = []
        for protein_id in proteinMap:
            list = proteinMap[protein_id]
            expressionList = [translateQualValue(row[0]) for row in list]
            ranks = calculateRanks(expressionList)
            for index in range(len(list)):
                (expressionValue, extras) = list[index]
                inserts.append((etype, protein_id, expressionValue, ranks[index]) + extras)
        return inserts

    def addExpressionObject(proteinMap, protein_id, expressionValue, extras):
        if protein_id in proteinMap:
            list = proteinMap[protein_id]
            list.append((expressionValue, extras))
        else:
            proteinMap[protein_id] = [(expressionValue, extras)]

    def doTDLinserts(itype, proteinMap, description):
        inserts = []
        for protein_id in proteinMap:
            tau = calculateOneTissueSpecificity([translateQualValue(row[0]) for row in proteinMap[protein_id]])
            inserts.append((itype, protein_id, tau))
        saveTissueSpecificityToTDLInfo(inserts, [itype], [description])

    #region HPA RNA
    def doETLforHPARNA():
        inputFile = getConfigFile("Expression", "HPA RNA", "data")
        mappingFile = getConfigFile("Expression", "HPA RNA", "manual map")
        tissueMap = getTissueMap(mappingFile)
        ensgMap = getENSGMap()
        count = 0
        proteinLists = {}
        with open(inputFile) as mapFile:
            next(mapFile)
            mapFile = csv.DictReader(mapFile, delimiter="\t", fieldnames=["Gene", "Gene name", "Tissue", "TPM", "pTPM", "nTPM"])
            for row in mapFile:
                uberon_id = lookupTissue(tissueMap, row["Tissue"])
                protein_id = lookupENSG(ensgMap, row["Gene"])
                if(protein_id is not None):
                    addExpressionObject(proteinLists, protein_id, float(row["nTPM"]), (row["Tissue"], uberon_id))
                count = count + 1
                if (testing and count >= testCount):
                    break
        mysqlserver = getMysqlConnector()
        doTDLinserts(tauStrings()[0], proteinLists, descriptions()[0])
        inserts = add_ranks_and_format_inserts(proteinLists, "HPA RNA")
        mysqlserver.insert_many_rows('expression', inserts, target_fields=('etype', 'protein_id', 'number_value', 'source_rank', 'tissue', 'uberon_id'))

    etlHPARNA = PythonOperator(
        dag=dag_subdag,
        task_id='etl-hpa-rna',
        python_callable=doETLforHPARNA
    )
    #endregion

    #region HPA Protein
    qualMap = {'Not detected' : 0,
               'Medium': 2,
               'High': 3,
               'Low': 1}

    def translateQualValue(val):
        if type(val) == str:
            return qualMap[val]
        return val

    def doETLforHPAProtein():
        inputFile = getConfigFile("Expression", "HPA Protein", "data")
        mappingFile = getConfigFile("Expression", "HPA Protein", "manual map")
        tissueMap = getTissueMap(mappingFile)
        ensgMap = getENSGMap()
        count = 0
        proteinLists = {}
        badQuals = {}
        with open(inputFile) as mapFile:
            next(mapFile)
            mapFile = csv.DictReader(mapFile, delimiter="\t", fieldnames=["Gene", "Gene name", "Tissue", "Cell type", "Level", "Reliability"])
            for row in mapFile:
                tissueAndCell = row["Tissue"] + ' - ' + row["Cell type"]
                uberon_id = lookupTissue(tissueMap, tissueAndCell)
                if uberon_id == None:
                    uberon_id = lookupTissue(tissueMap, row["Tissue"])
                protein_id = lookupENSG(ensgMap, row["Gene"])
                count = count + 1
                if(protein_id is not None and row["Tissue"] != "N/A" and row["Reliability"] != "Uncertain" and row["Level"] != 'Not representative'):
                    if row["Level"] in qualMap:
                        addExpressionObject(proteinLists, protein_id, row["Level"], (tissueAndCell, row["Reliability"], uberon_id))
                    else:
                        if row['Level'] in badQuals:
                            badQuals[row['Level']] = badQuals[row['Level']] + 1
                        else:
                            badQuals[row['Level']] = 1
                if (testing and count >= testCount):
                    break

        print(f"Bad Qualitative Values for HPA Protein")
        print(badQuals)
        mysqlserver = getMysqlConnector()
        doTDLinserts(tauStrings()[1], proteinLists, descriptions()[0])
        inserts = add_ranks_and_format_inserts(proteinLists, "HPA Protein")
        mysqlserver.insert_many_rows('expression', inserts, target_fields=('etype', 'protein_id', 'qual_value', 'source_rank', 'tissue', 'evidence', 'uberon_id'))

    etlHPAProtein = PythonOperator(
        dag=dag_subdag,
        task_id='etl-hpa-protein',
        python_callable=doETLforHPAProtein
    )
    #endregion

    #region JensenLab Integrated Channel

    def doETLforJensenLab(ti):
        inputFile = getConfigFile("Expression", "JensenLab TISSUES", "data")
        mappingFile = getConfigFile("Expression", "JensenLab TISSUES", "manual map")
        humanEntityFile = getConfigFile("Expression", "JensenLab TISSUES", "human entities")

        tissueMap = getTissueMap(mappingFile)
        ensgMap = getENSGMap()
        humanEntityMap = getHumanEntities(humanEntityFile)

        count = 0
        inserts = []
        unmatchedBTOs = []
        with open(inputFile) as mapFile:
            mapFile = csv.DictReader(mapFile, delimiter="\t", fieldnames=["gene_id", "sym", "ontology_id", "tissue", "confidence"])
            for row in mapFile:
                if (row["gene_id"] in humanEntityMap):
                    protein_id = lookupENSG(ensgMap, row["gene_id"])
                    if(protein_id is not None):
                        uberon_id = lookupTissue(tissueMap, row["tissue"])
                        if (uberon_id is None):
                            if (row["ontology_id"] not in unmatchedBTOs):
                                unmatchedBTOs.append(row["ontology_id"])
                        count = count + 1
                        inserts.append((
                            "JensenLab TISSUES", protein_id, row["tissue"], row["confidence"], row["ontology_id"], uberon_id
                        ))
                        if (testing and count >= testCount):
                            break
        btoMap = getBTO2UberonMappings(unmatchedBTOs, tissueMap.values())
        for index in range(len(inserts)):
            row = inserts[index]
            if row[5] is None and row[4] in btoMap:
                inserts[index] = (row[0], row[1], row[2], row[3], row[4], btoMap[row[4]])
        mysqlserver = getMysqlConnector()
        mysqlserver.insert_many_rows('expression', inserts, target_fields=('etype', 'protein_id', 'tissue', 'number_value', 'source_ontology_id', 'uberon_id'))

    def getBTO2UberonMappings(unmatchedBTOs, uberonList):
        print(unmatchedBTOs)
        nChunks = math.ceil(len(unmatchedBTOs)/500)
        httpConnection = getHttpConnector()
        mapping = {}
        for index in range(nChunks):
            httpResults = httpConnection.run(
                endpoint="/spot/oxo/api/search?size=500",
                headers={"Content-Type": "application/json"},
                data="""{
                    "ids" : """ + json.dumps(unmatchedBTOs[index::nChunks]) + """,
                    "mappingTarget" : [ "UBERON" ],
                    "distance" : 1
                }""")
            print('sleeping 30 seconds: wait for http connection pool')
            time.sleep(30)
            mapping = mapping | parse_mapping(httpResults, uberonList)
        return mapping

    etlJensenLabIntegrated = PythonOperator(
        dag=dag_subdag,
        task_id='etl-jensenlab-integrated',
        python_callable=doETLforJensenLab
    )
    def parse_mapping(response, uberonList):
        jsonResponse = response.json()["_embedded"]["searchResults"]
        return dict((
                        row["queryId"],
                        row["mappingResponseList"][0]["curie"]
                        if len(row["mappingResponseList"]) > 0 and row["mappingResponseList"][0]["curie"] in uberonList
                        else None
                    ) for row in jsonResponse)

    #endregion

    #region HPM
    def doETLforHPM():
        inputFile = getConfigFile("Expression", "HPM Protein", "data")
        mappingFile = getConfigFile("Expression", "HPM Protein", "manual map")
        tissueMap = getTissueMap(mappingFile)
        refseqMap = getRefSeqMap()
        count = 0
        done_list = []
        proteinLists = {}
        breaking = False
        with open(inputFile) as dataFile:
            dataFile = csv.DictReader(dataFile, delimiter=",")
            for row in dataFile:
                refseq_id = row["RefSeq Accession"].split('.')[0]
                if refseq_id in refseqMap:
                    protein_ids = refseqMap[refseq_id]
                    protein_ids = [p for p in protein_ids if p not in done_list]
                    done_list.extend(protein_ids)
                    for tissue in row:
                        if tissue != 'Accession' and tissue != 'RefSeq Accession':
                            uberon_id = lookupTissue(tissueMap, tissue)
                            for protein_id in protein_ids:
                                addExpressionObject(proteinLists, protein_id, float(row[tissue]), (tissue, uberon_id))
                            count = count + 1
                            if testing and count >= testCount:
                                breaking = True
                                break
                if breaking:
                    break

        mysqlserver = getMysqlConnector()
        doTDLinserts(tauStrings()[2], proteinLists, descriptions()[2])
        inserts = add_ranks_and_format_inserts(proteinLists, "HPM Protein")
        mysqlserver.insert_many_rows('expression', inserts, target_fields=('etype', 'protein_id', 'number_value', 'source_rank', 'tissue', 'uberon_id'))

    etlHPMProtein = PythonOperator(
        dag=dag_subdag,
        task_id='etl-hpm-protein',
        python_callable=doETLforHPM
    )


    #endregion
    dropAllTables >> createExpressionTables
    createExpressionTables >> [etlHPARNA, etlHPAProtein, etlJensenLabIntegrated, etlHPMProtein]

    return dag_subdag

dag = createExpressionDAG('standalone', 'rebuild-Expression-tables', {"retries": 0})