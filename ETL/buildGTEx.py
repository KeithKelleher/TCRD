from util import calculateOneTissueSpecificity, getENSGMap, getFilteredSampleTissueMap, \
    getFilteredSexMap, saveTissueSpecificityToTDLInfo, calculateRanks, get_dataSource_inserts
from airflow import DAG
from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.operators.python import PythonOperator
import statistics
import csv, os, sys, gzip
import pendulum

sys.path += [ os.path.dirname(__file__) ]
sys.path += [ os.path.dirname(__file__) + '/FullRebuild' ]

from FullRebuild.models.common import common

sqlFiles = common.getSqlFiles()
testing = False  # just do some rows of the input file, set to False do the full ETL
testCount = 750
schemaname = common.getNewDatabaseName()
mysqlConnectorID = common.getGenericConnectionName()
csv.field_size_limit(sys.maxsize)
directory = os.path.dirname(__file__) + '/'

full = "GTEx"
male = "GTEx - Male"
female = "GTEx = Female"
gtexSources = {
    full: {
        'tauString': 'GTEx Tissue Specificity Index',
        'tauDescription': 'Tau as defined in Yanai, I. et. al., Bioinformatics 21(5): 650-659 (2005) calculated on GTEx data.',
        'dataSourceDescription': 'RNA-level expression values based on RNA-Seq experiments from GTEx. Numeric values represent the median transcripts per million for each gene in each tissue. Sex-specific values represent the median transcripts per million for the same data segregated by subject sex. Subjects are filtered to exclude those with a death_hardy score > 2. Samples are filtered to exclude those with a moderate or severe degree of autolylsis.',
        'url': 'https://www.gtexportal.org/home/',
        'license': '',
        'licenseURL': '',
        'citation': 'The Genotype-Tissue Expression (GTEx) Project was supported by the <a href="https://commonfund.nih.gov/GTEx" target="_blank">Common Fund</a> of the Office of the Director of the National Institutes of Health, and by NCI, NHGRI, NHLBI, NIDA, NIMH, and NINDS.'
    },
    male: {
        'tauString': 'GTEx Tissue Specificity Index - Male',
        'tauDescription': 'Tau as defined in Yanai, I. et. al., Bioinformatics 21(5): 650-659 (2005) calculated on GTEx data for male subjects.',
    },
    female: {
        'tauString': 'GTEx Tissue Specificity Index - Female',
        'tauDescription': 'Tau as defined in Yanai, I. et. al., Bioinformatics 21(5): 650-659 (2005) calculated on GTEx data for female subjects.',
    }
}

def calculateMedian(values):
    return statistics.median(values) if (values != None and len(values) > 0) else None

def fmt_expression_detail_obj(protein_id, intissue, median_tpm, median_tpm_male, median_tpm_female):
    (tissue, uberon_id) = intissue
    return {
        'protein_id': protein_id,
        'tissue': tissue,
        'uberon_id': uberon_id,
        'median_tpm': median_tpm,
        'median_tpm_male': median_tpm_male,
        'median_tpm_female': median_tpm_female
    }

def expression_detail_to_insert(expObj, tpm_rank, tpm_male_rank, tpm_female_rank):
    inserts = []
    inserts.append((
        expObj["protein_id"],
        expObj["tissue"],
        expObj["median_tpm"],
        tpm_rank,
        expObj["median_tpm_male"],
        tpm_male_rank,
        expObj["median_tpm_female"],
        tpm_female_rank,
        formatUberon(expObj["uberon_id"])
    ))
    return inserts

def get_tdl_inserts(protein_id, allMedians, maleMedians, femaleMedians):
    inserts = []
    allTau = calculateOneTissueSpecificity(allMedians)
    maleTau = calculateOneTissueSpecificity(maleMedians)
    femaleTau = calculateOneTissueSpecificity(femaleMedians)

    inserts.append((gtexSources[full]['tauString'], protein_id, allTau))
    inserts.append((gtexSources[male]['tauString'], protein_id, maleTau))
    inserts.append((gtexSources[female]['tauString'], protein_id, femaleTau))
    return inserts

def formatUberon(dbValue):
    if ("_" in dbValue):
        return None #dbValue.replace("_",":")
    return "UBERON:" + dbValue

def populateGtexTable():
    mysqlserver = common.getMysqlConnector()
    ensemblMap = getENSGMap()
    tissueMap = getFilteredSampleTissueMap()
    sexMap = getFilteredSexMap()
    tdlInserts = []
    inserts = []
    proteinLists = {}
    dataFile = common.getConfigFile("GTEx", None, "data")
    with gzip.open(dataFile, 'rt') as file:
        next(file), next(file) # first two lines are not data
        tsv_file = csv.DictReader(file, delimiter="\t")
        count = 0
        for geneRow in tsv_file:
            ensg_id = geneRow["Name"].split('.')[0]

            if ensg_id not in ensemblMap:
                continue

            maleTissueDict = {}
            femaleTissueDict = {}
            protein_ids = ensemblMap[ensg_id]
            for key in geneRow:
                if (key != 'Description' and key != 'Name'):
                    subject_id = get_subject_id(key)
                    expressionValue = float(geneRow[key])
                    if key not in tissueMap or subject_id not in sexMap: # unmapped values either failed death_hardy criteria, or autolysis criteria
                        continue
                    (tissue, uberon_id) = tissueMap[key]
                    if (sexMap[subject_id]=='male'):
                        addExpressionToDict(maleTissueDict, tissue, uberon_id, expressionValue)
                    else:
                        addExpressionToDict(femaleTissueDict, tissue, uberon_id, expressionValue)

            expressionDetails = []
            allTissues = maleTissueDict.keys() | femaleTissueDict.keys()
            allMedians = []
            maleMedians = []
            femaleMedians = []
            for tissue in allTissues:
                allValues = []
                maleVals = []
                femaleVals = []

                if tissue in maleTissueDict:
                    maleVals = maleTissueDict[tissue]
                    allValues.extend(maleVals)

                if tissue in femaleTissueDict:
                    femaleVals = femaleTissueDict[tissue]
                    allValues.extend(femaleVals)

                median_tpm = calculateMedian(allValues)
                median_tpm_male = calculateMedian(maleVals)
                median_tpm_female = calculateMedian(femaleVals)

                allMedians.append(median_tpm)
                if median_tpm_male != None:
                    maleMedians.append(median_tpm_male)
                if median_tpm_female != None:
                    femaleMedians.append(median_tpm_female)

                for protein_id in protein_ids:
                    expressionDetails.append(fmt_expression_detail_obj(protein_id, tissue, median_tpm, median_tpm_male, median_tpm_female))


            for protein_id in protein_ids:
                tdlInserts.extend(get_tdl_inserts(protein_id, allMedians, maleMedians, femaleMedians))
                proteinLists[protein_id] = 1
            inserts.extend(add_ranks_and_format_inserts(allMedians, femaleMedians, maleMedians, expressionDetails))

            if (testing):
                count = count + 1
                if (count >= testCount):
                    break

    mysqlserver.insert_many_rows(f'{schemaname}.gtex', inserts, target_fields=('protein_id', 'tissue', 'tpm', 'tpm_rank', 'tpm_male',
                                                        'tpm_male_rank', 'tpm_female', 'tpm_female_rank', 'uberon_id'))
    dsInserts = get_dataSource_inserts(proteinLists, full)
    mysqlserver.insert_rows(f'{schemaname}.ncats_dataSource', [(full, gtexSources[full]['dataSourceDescription'], gtexSources[full]['url'], gtexSources[full]['license'], gtexSources[full]['licenseURL'], gtexSources[full]['citation'])],
                            target_fields=('dataSource', 'dataSourceDescription', 'url', 'license', 'licenseURL', 'citation'))
    mysqlserver.insert_many_rows(f'{schemaname}.ncats_dataSource_map', dsInserts, target_fields=('dataSource', 'protein_id'))

    saveTissueSpecificityToTDLInfo(tdlInserts)

def popSampTable():
    mysqlserver = common.getMysqlConnector()
    inputFile = common.getConfigFile("GTEx", None, "sample attributes")
    inserts = []
    with open(inputFile) as dataFile:
        dataDict = csv.DictReader(dataFile, delimiter="\t")
        for row in dataDict:
            inserts.append((row['SAMPID'], row['SMTSD'], row['SMUBRID'], row['SMATSSCR']))
    mysqlserver.insert_many_rows(f'{schemaname}.gtex_sample', inserts, target_fields=('SAMPID', 'SMTSD', 'SMUBRID', 'SMATSSCR'))
    return

def popSubTable():
    mysqlserver = common.getMysqlConnector()
    inputFile = common.getConfigFile("GTEx", None, "subject phenotypes")
    inserts = []
    with open(inputFile) as dataFile:
        dataDict = csv.DictReader(dataFile, delimiter="\t")
        for row in dataDict:

            inserts.append((row['SUBJID'], row['SEX'], row['AGE'], row['DTHHRDY'] if len(row['DTHHRDY'])>0 else None))
    mysqlserver.insert_many_rows(f'{schemaname}.gtex_subject', inserts, target_fields=('subject_id', 'sex', 'age', 'death_hardy'))
    return

def add_ranks_and_format_inserts(allMedians, femaleMedians, maleMedians, protein_expression_detail_list):
    inserts = []
    allRanks = calculateRanks(allMedians)
    maleRanks = calculateRanks(maleMedians)
    femaleRanks = calculateRanks(femaleMedians)
    allRankDict = {allMedians[i]: allRanks[i] for i in range(len(allMedians))}
    maleRankDict = {maleMedians[i]: maleRanks[i] for i in range(len(maleMedians))}
    femaleRankDict = {femaleMedians[i]: femaleRanks[i] for i in range(len(femaleMedians))}
    for expObj in protein_expression_detail_list:
        tpm_rank = allRankDict[expObj['median_tpm']]
        tpm_male_rank = maleRankDict[expObj['median_tpm_male']] if expObj['median_tpm_male'] != None else None
        tpm_female_rank = femaleRankDict[expObj['median_tpm_female']] if expObj['median_tpm_female'] != None else None
        inserts.extend(expression_detail_to_insert(expObj, tpm_rank, tpm_male_rank, tpm_female_rank))
    return inserts

def get_subject_id(key):
    chunks = key.split('-')
    subject_id = chunks[0] + '-' + chunks[1]
    return subject_id


def addExpressionToDict(tissueDict, tissue, uberon_id, expressionValue):
    if ((tissue, uberon_id) in tissueDict):
        tissueDict[(tissue, uberon_id)].append(expressionValue)
    else:
        tissueDict[(tissue, uberon_id)] = [expressionValue]

def createGTExDAG(parent_dag_name, child_task_id, args):
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
        sql=f"""DROP TABLE IF EXISTS `gtex_sample`;
                DROP TABLE IF EXISTS `gtex_subject`;
                DROP TABLE IF EXISTS `gtex`;
                DELETE FROM `tdl_info` WHERE itype in ('{gtexSources[full]['tauString']}','{gtexSources[male]['tauString']}','{gtexSources[female]['tauString']}');
                DELETE FROM `ncats_dataSource_map` where dataSource = '{full}';
                DELETE FROM `ncats_dataSource` where dataSource = '{full}';""",
        mysql_conn_id=mysqlConnectorID,
        database=schemaname
    )

    # region Subject Table
    createSubjectTable = MySqlOperator(
        dag=dag_subdag,
        task_id='create-subject-table',
        sql=sqlFiles['gtex_subject.sql'],
        mysql_conn_id=mysqlConnectorID,
        database=schemaname
    )

    populateSubjectTable = PythonOperator(
        dag=dag_subdag,
        task_id='populate-subject-table',
        python_callable=popSubTable
    )

    populateSampleTable = PythonOperator(
        dag=dag_subdag,
        task_id='populate-sample-table',
        python_callable=popSampTable
    )
    # endregion

    # region Sample Table
    createSampleTable = MySqlOperator(
        dag=dag_subdag,
        task_id='create-sample-table',
        sql=sqlFiles['gtex_sample.sql'],
        mysql_conn_id=mysqlConnectorID,
        database=schemaname
    )

    # endregion

    #region GTEx Summary Table
    createSummaryTable = MySqlOperator(
        dag=dag_subdag,
        task_id='create-summary-table',
        sql=sqlFiles['gtex.sql'],
        mysql_conn_id=mysqlConnectorID,
        database=schemaname
    )
    #endregion

    populateGtexDataTables = PythonOperator(
        dag=dag_subdag,
        task_id='populate-gtex_tables',
        python_callable=populateGtexTable
    )

    dropTempTables = MySqlOperator(
        dag=dag_subdag,
        task_id='drop-temp-tables',
        sql=f"""DROP TABLE IF EXISTS `gtex_sample`;
                    DROP TABLE IF EXISTS `gtex_subject`;""",
        mysql_conn_id=mysqlConnectorID,
        database=schemaname
    )

    dropAllTables >> [createSubjectTable, createSampleTable, createSummaryTable]

    [
        createSubjectTable >> populateSubjectTable,
        createSummaryTable,
        createSampleTable >> populateSampleTable
    ] >> populateGtexDataTables >> dropTempTables

    return dag_subdag

# dag = createGTExDAG('standalone', 'rebuild-GTEx-tables', {"retries": 0})