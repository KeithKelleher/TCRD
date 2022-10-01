import csv
import pendulum
from airflow.providers.mysql.operators.mysql import MySqlOperator

from util import getSqlFiles, getTissueMap, getConfigFile, lookupTissue, getENSGMap, getMysqlConnector, lookupENSG, \
    getHumanEntities, getRefSeqMap, calculateOneTissueSpecificity, saveTissueSpecificityToTDLInfo, \
    calculateRanks, getOntologyMap, getManualMap, get_dataSource_inserts
from airflow import DAG
from airflow.operators.python import PythonOperator

sqlFiles = getSqlFiles()
schemaname = 'tcrdinfinity'
mysqlConnectorID = schemaname
testing = False  # just do some rows of the input file, set to False do the full ETL
testCount = 7500

def dataSourceStrings():
    currentSources = tuple(expressionSources.keys())
    return ','.join(f"'{source}'" for source in currentSources)

def tauStrings():
    return ",".join(f"'{expressionSources[source]['tauString']}'" for source in expressionSources if 'tauString' in expressionSources[source])

hpakey = 'HPA RNA'
hpapkey = 'HPA Protein'
jensenlabkey = 'JensenLab TISSUES'
hpmkey = 'HPM Protein'
expressionSources = {
        hpakey: {
            'tauString': 'HPA RNA Tissue Specificity Index',
            'tauDescription': 'Tau as defined in Yanai, I. et. al., Bioinformatics 21(5): 650-659 (2005) calculated on HPA RNA data.',
            'dataSourceDescription': 'RNA-level expression values based on RNA-Seq experiments from Human Protein Atlas. Numeric value represents normalized transcripts per million, see <a href="https://www.proteinatlas.org/about/assays+annotation#normalization_rna" target="_blank">https://www.proteinatlas.org/about/assays+annotation#normalization_rna</a> for more.',
            'url': 'http://www.proteinatlas.org/',
            'license': 'Creative Commons Attribution-ShareAlike 3.0 International License',
            'licenseURL': 'https://www.proteinatlas.org/about/licence',
            'citation': None
        },
        hpapkey: {
            'tauString': 'HPA Protein Tissue Specificity Index',
            'tauDescription': 'Tau as defined in Yanai, I. et. al., Bioinformatics 21(5): 650-659 (2005) calculated on HPA Protein data.',
            'dataSourceDescription': 'Protein-level expression values based on immunohistochemical staining from Human Protein Atlas. Qualitative value represents the measured intensity of staining. Reliability scores are curated for all genes and reflect many knowledge sources, see <a href="https://www.proteinatlas.org/about/assays+annotation#ih_reliability" target="_blank">https://www.proteinatlas.org/about/assays+annotation#ih_reliability</a> for more.',
            'url':'https://www.proteinatlas.org/',
            'license':'Creative Commons Attribution-ShareAlike 3.0 International License',
            'licenseURL':'https://www.proteinatlas.org/about/licence',
            'citation': None
        },
        jensenlabkey: {
            'dataSourceDescription': 'Aggregate score based on many data sources, including RNA-seq, IHC, text-mining etc., from JensenLab TISSUES. Numeric values represent the confidence that a target is expressed in a tissue, see <a href="https://doi.org/10.7717/peerj.1054/table-1" target="_blank">https://doi.org/10.7717/peerj.1054/table-1</a> for more.',
            'url':'https://tissues.jensenlab.org/Search',
            'license':None,
            'licenseURL':None,
            'citation': 'https://academic.oup.com/database/article/doi/10.1093/database/bay003/4851151?login=false'
        },
        hpmkey: {
            'tauString': 'HPM Protein Tissue Specificity Index',
            'tauDescription': 'Tau as defined in Yanai, I. et. al., Bioinformatics 21(5): 650-659 (2005) calculated on HPM Protein data.',
            'dataSourceDescription': 'Protein-level expression values based on Fourier Transform Mass Spectrometry from Human Proteome Map. Numeric value represents the normalized spectral count per gene per tissue, see <a href="https://www.humanproteomemap.org/faqs.html" target="_blank">https://www.humanproteomemap.org/faqs.html</a> for more.',
            'url':'https://www.humanproteomemap.org/',
            'license':None,
            'licenseURL':None,
            'citation': 'https://pubmed.ncbi.nlm.nih.gov/24870542/'
        }
    }

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
        sql=f"""DROP TABLE IF EXISTS `expression_temp`;
        DROP TABLE IF EXISTS `expression`;
        DROP TABLE IF EXISTS `tissue`;
        DELETE FROM `tdl_info` WHERE itype in ({tauStrings()});
        DELETE FROM `ncats_dataSource` where dataSource in ({dataSourceStrings()});
        DELETE FROM `ncats_dataSource_map` where dataSource in ({dataSourceStrings()});
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
        dataSourceDetails = expressionSources[hpakey]
        inputFile = getConfigFile("Expression", hpakey, "data")
        mappingFile = getConfigFile("Expression", hpakey, "manual map")
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
                expressionValue = float(row["nTPM"])
                if(protein_id is not None):
                    addExpressionObject(proteinLists, protein_id, expressionValue, (row["Tissue"], uberon_id, row["Gene"], (expressionValue > 0)))
                count = count + 1
                if (testing and count >= testCount):
                    break
        mysqlserver = getMysqlConnector()

        doTDLinserts(dataSourceDetails['tauString'], proteinLists, dataSourceDetails['tauDescription'])

        dsInserts = get_dataSource_inserts(proteinLists, hpakey)
        mysqlserver.insert_rows('ncats_dataSource', [(hpakey, dataSourceDetails['dataSourceDescription'], dataSourceDetails['url'], dataSourceDetails['license'], dataSourceDetails['licenseURL'], dataSourceDetails['citation'])],
                                target_fields=('dataSource', 'dataSourceDescription', 'url', 'license', 'licenseURL', 'citation'))
        mysqlserver.insert_many_rows('ncats_dataSource_map', dsInserts, target_fields=('dataSource', 'protein_id'))

        inserts = add_ranks_and_format_inserts(proteinLists, hpakey)
        mysqlserver.insert_many_rows('expression_temp', inserts, target_fields=('etype', 'protein_id', 'number_value', 'source_rank', 'tissue', 'uberon_id', 'source_id', 'expressed'))

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
        dataSourceDetails = expressionSources[hpapkey]
        inputFile = getConfigFile("Expression", hpapkey, "data")
        mappingFile = getConfigFile("Expression", hpapkey, "manual map")
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
                        addExpressionObject(proteinLists, protein_id, row["Level"], (tissueAndCell, row["Reliability"], uberon_id, row["Gene"], (row["Level"] != 'Not detected')))
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

        doTDLinserts(dataSourceDetails['tauString'], proteinLists, dataSourceDetails['tauDescription'])

        dsInserts = get_dataSource_inserts(proteinLists, hpapkey)
        mysqlserver.insert_rows('ncats_dataSource', [(hpapkey, dataSourceDetails['dataSourceDescription'], dataSourceDetails['url'], dataSourceDetails['license'], dataSourceDetails['licenseURL'], dataSourceDetails['citation'])],
                                target_fields=('dataSource', 'dataSourceDescription', 'url', 'license', 'licenseURL', 'citation'))
        mysqlserver.insert_many_rows('ncats_dataSource_map', dsInserts, target_fields=('dataSource', 'protein_id'))

        inserts = add_ranks_and_format_inserts(proteinLists, hpapkey)
        mysqlserver.insert_many_rows('expression_temp', inserts, target_fields=('etype', 'protein_id', 'qual_value', 'source_rank', 'tissue', 'evidence', 'uberon_id', 'source_id', 'expressed'))

    etlHPAProtein = PythonOperator(
        dag=dag_subdag,
        task_id='etl-hpa-protein',
        python_callable=doETLforHPAProtein
    )
    #endregion

    #region JensenLab Integrated Channel

    def doETLforJensenLab(ti):
        dataSourceDetails = expressionSources[jensenlabkey]
        inputFile = getConfigFile("Expression", jensenlabkey, "data")
        mappingFile = getConfigFile("Expression", jensenlabkey, "manual map")
        humanEntityFile = getConfigFile("Expression", jensenlabkey, "human entities")

        ontologyMap = getOntologyMap()
        tissueMap = getManualMap(mappingFile)
        ensgMap = getENSGMap()
        humanEntityMap = getHumanEntities(humanEntityFile)

        count = 0
        inserts = []
        proteinLists = {}
        with open(inputFile) as mapFile:
            mapFile = csv.DictReader(mapFile, delimiter="\t", fieldnames=["gene_id", "sym", "ontology_id", "tissue", "confidence"])
            for row in mapFile:
                if (row["gene_id"] in humanEntityMap):
                    protein_id = lookupENSG(ensgMap, row["gene_id"])
                    if protein_id is not None:
                        proteinLists[protein_id] = 1
                        uberon_id = lookupTissue(tissueMap,row["ontology_id"])
                        if uberon_id is None:
                            uberon_id = ontologyMap[row["ontology_id"]][0]\
                                if row["ontology_id"] in ontologyMap and len(ontologyMap[row["ontology_id"]]) > 0 \
                                else None
                        if uberon_id is not None:
                            count = count + 1
                            expressionValue = float(row["confidence"])
                            inserts.append((
                                jensenlabkey, protein_id, row["tissue"], expressionValue, row["ontology_id"], uberon_id, row["gene_id"], (expressionValue > 0)
                            ))
                            if (testing and count >= testCount):
                                break

        mysqlserver = getMysqlConnector()

        dsInserts = get_dataSource_inserts(proteinLists, jensenlabkey)
        mysqlserver.insert_rows('ncats_dataSource', [(jensenlabkey, dataSourceDetails['dataSourceDescription'], dataSourceDetails['url'], dataSourceDetails['license'], dataSourceDetails['licenseURL'], dataSourceDetails['citation'])],
                                target_fields=('dataSource', 'dataSourceDescription', 'url', 'license', 'licenseURL', 'citation'))
        mysqlserver.insert_many_rows('ncats_dataSource_map', dsInserts, target_fields=('dataSource', 'protein_id'))

        mysqlserver.insert_many_rows('expression_temp', inserts, target_fields=('etype', 'protein_id', 'tissue', 'number_value', 'oid', 'uberon_id', 'source_id', 'expressed'))


    etlJensenLabIntegrated = PythonOperator(
        dag=dag_subdag,
        task_id='etl-jensenlab-integrated',
        python_callable=doETLforJensenLab
    )

    #endregion

    #region HPM
    def doETLforHPM():
        dataSourceDetails = expressionSources[hpmkey]
        inputFile = getConfigFile("Expression", hpmkey, "data")
        mappingFile = getConfigFile("Expression", hpmkey, "manual map")
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
                            expressionValue = float(row[tissue])
                            for protein_id in protein_ids:
                                addExpressionObject(proteinLists, protein_id, expressionValue, (tissue, uberon_id, row["RefSeq Accession"], (expressionValue > 0)))
                            count = count + 1
                            if testing and count >= testCount:
                                breaking = True
                                break
                if breaking:
                    break

        mysqlserver = getMysqlConnector()

        doTDLinserts(dataSourceDetails['tauString'], proteinLists, dataSourceDetails['tauDescription'])

        dsInserts = get_dataSource_inserts(proteinLists, hpmkey)
        mysqlserver.insert_rows('ncats_dataSource', [(hpmkey, dataSourceDetails['dataSourceDescription'], dataSourceDetails['url'], dataSourceDetails['license'], dataSourceDetails['licenseURL'], dataSourceDetails['citation'])],
                                target_fields=('dataSource', 'dataSourceDescription', 'url', 'license', 'licenseURL', 'citation'))
        mysqlserver.insert_many_rows('ncats_dataSource_map', dsInserts, target_fields=('dataSource', 'protein_id'))

        inserts = add_ranks_and_format_inserts(proteinLists, hpmkey)
        mysqlserver.insert_many_rows('expression_temp', inserts, target_fields=('etype', 'protein_id', 'number_value', 'source_rank', 'tissue', 'uberon_id', 'source_id', 'expressed'))

    etlHPMProtein = PythonOperator(
        dag=dag_subdag,
        task_id='etl-hpm-protein',
        python_callable=doETLforHPM
    )
    #endregion

    createTissueTable = MySqlOperator(
        dag=dag_subdag,
        task_id='create-tissue-table',
        sql=sqlFiles['tissue.sql'],
        mysql_conn_id=mysqlConnectorID
    )

    def updateDiscreteTissue():
        mysqlserver = getMysqlConnector()
        allexpressions = mysqlserver.get_records("""SELECT * from expression_temp""")
        tissueMap = {}
        index = 1
        inserts = []
        for row in allexpressions:
            tissue = row[4]
            if tissue.lower() in tissueMap:
                (_, tissue_id) = tissueMap[tissue.lower()]
            else:
                tissue_id = index
                tissueMap[tissue.lower()] = (tissue, index)
                index = index + 1
            inserts.append(row + (tissue_id,))
        mysqlserver.insert_many_rows('tissue', [(tissueMap[key][1], tissueMap[key][0]) for key in tissueMap], target_fields=('id', 'name'))
        mysqlserver.insert_many_rows('expression', inserts, target_fields=('id', 'etype', 'protein_id', 'source_id', 'tissue', 'qual_value', 'number_value', 'expressed', 'source_rank', 'evidence', 'oid', 'uberon_id', 'tissue_id'))
        mysqlserver.run("DROP TABLE `expression_temp`")

    updateTissueTable = PythonOperator(
        dag=dag_subdag,
        task_id='update-tissue-table',
        python_callable=updateDiscreteTissue
    )

    dropAllTables >> createTissueTable >> createExpressionTables
    createExpressionTables >> [etlHPARNA, etlHPAProtein, etlJensenLabIntegrated, etlHPMProtein] >> updateTissueTable
    createTissueTable >> updateTissueTable

    return dag_subdag

dag = createExpressionDAG('standalone', 'rebuild-Expression-tables', {"retries": 0})