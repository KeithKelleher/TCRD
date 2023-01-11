import sys
import os, re
from os.path import exists
from urllib.parse import urlparse
import zipfile, tarfile

sys.path += [ os.path.dirname(__file__) ]
sys.path += [ os.path.dirname(__file__) + '/models' ]

from urllib import request, parse
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import xml.etree.ElementTree as ET

testing = False
outputPath = os.path.dirname(__file__) + '/input_files/'

def downloadData(**kwargs):
    sourceObj = kwargs['sourceObj']
    sourceDirectory = outputPath + sourceObj['source']

    if 'githubReleaseUrl' in sourceObj and 'githubCallback' in sourceObj:
        print(sourceObj)
        parseGithubRelease(sourceObj)
        print(sourceObj)

    fullQuery = sourceObj['baseUrl'] + parse.quote(sourceObj['query'])
    if testing and 'testQuery' in sourceObj:
        fullQuery = sourceObj['baseUrl'] + parse.quote(sourceObj['testQuery'])
    os.makedirs(sourceDirectory, exist_ok=True)
    fileName = sourceObj['query']
    if 'fileName' in sourceObj:
        fileName = sourceObj['fileName']
    destinationFile = sourceDirectory + '/' + fileName
    if exists(destinationFile) and sourceObj['onceOnly']:
        print(f"skipping: {fullQuery}")
        print(f"Download is configured to run only one time, and the download file already exists: {destinationFile}")
        return
    print(f"fetching: {fullQuery}")
    dataFileReq = request.urlretrieve(fullQuery, destinationFile)
    if destinationFile.endswith('.zip'):
        with zipfile.ZipFile(destinationFile, 'r') as zip_ref:
            zip_ref.extractall(sourceDirectory)
    if type(sourceObj['version']) == str and sourceObj['version'].startswith('http'):
        versionFileReq = request.urlopen(sourceObj['version'])
        version = sourceObj['versionCallback'](versionFileReq)
    else:
        version = sourceObj['version']
    version['data'] = fileName.split('.zip')[0] if fileName.endswith('.zip') else fileName
    if 'release date' not in version:
        version['release date'] = datetime.strptime(dataFileReq[1]['Last-Modified'], '%a, %d %b %Y %H:%M:%S %Z')
    if 'supplemental files' in sourceObj:
        for url in sourceObj['supplemental files']:
            a = urlparse(url)
            suppFileName = os.path.basename(a.path)
            destinationFile =sourceDirectory + '/' + suppFileName
            request.urlretrieve(url, destinationFile)
            if destinationFile.endswith('.zip'):
                with zipfile.ZipFile(destinationFile, 'r') as zip_ref:
                    zip_ref.extractall(sourceDirectory)
            if destinationFile.endswith('.tar.gz'):
                file = tarfile.open(destinationFile)
                file.extractall(sourceDirectory)
                file.close()
    saveVersionInfo(version, sourceDirectory)
    print(f"saved: {destinationFile}")

def saveVersionInfo(versionInfo, sourceDirectory):
    with open(f"{sourceDirectory}/version.csv", 'w') as csvFile:
        for key in versionInfo.keys():
            csvFile.write("%s,%s\n"%(key,versionInfo[key]))

def parseUniProtVersion(reqData):
    last_modified = reqData.headers['last-modified']
    last_modified_time =  datetime.strptime(last_modified, '%a, %d %b %Y %H:%M:%S %Z')
    root = ET.fromstring(reqData.read())
    vers = root.find('{http://www.metalinker.org/}version')
    return {'version': vers.text, 'release date': last_modified_time.isoformat()}

def parseGithubRelease(sourceObj):
    githubLatestReleaseUrl = sourceObj['githubReleaseUrl']
    callback = sourceObj['githubCallback']
    versionFileReq = request.urlopen(githubLatestReleaseUrl)
    text = versionFileReq.read().decode('utf-8')
    callback(sourceObj, text)

def parseUberonData(sourceObj, text):
    version = {}
    matches = re.findall('(https://github.com/obophenotype/uberon/releases/tag/(v([0-9\-]+)))', text)
    tagUrl = matches[0][0]
    downloadUrl = tagUrl.replace('/tag/', '/download/')
    sourceObj['baseUrl'] = downloadUrl + '/'
    version['version'] = matches[0][1]
    version['release date'] = datetime.strptime(matches[0][2], '%Y-%m-%d')
    sourceObj['version'] = version


with DAG(
        '1-fetch-new-input_files',
        default_args={
            'depends_on_past': False,
            'email': ['keith.kelleher@ncats.nih.gov'],
            'email_on_failure': True,
            'email_on_retry': False,
            'retries': 0,
            'retry_delay': timedelta(minutes=5),
        },
        description='Fetches new flat files for the regularly updated datasets.',
        schedule_interval=None,
        start_date=datetime(2022, 9, 1),
        catchup=False,
        tags=['TCRD', 'Pharos', 'IDG', 'Build from Scratch'],
) as dag:

    sources = [
        {
            'key': 'uniprot',
            'source': 'UniProt',
            'baseUrl': 'https://rest.uniprot.org/uniprotkb/stream?compressed=true&format=json&query=',
            'query': '(*) AND (reviewed:true) AND (model_organism:9606)',
            'testQuery': 'accession:A0A0C5B5G6 OR accession:A0A1B0GTW7 OR '
                         'accession:A0JNW5 OR accession:A0JP26 OR accession:A0PK11 OR accession:A1A4S6 OR '
                         'accession:A1A519 OR accession:A1L190 OR accession:A1L3X0 OR accession:A1X283 OR '
                         'accession:A2A2Y4 OR accession:A2RU14 OR accession:A2RUB6 OR accession:A2RUC4 OR '
                         'accession:A4D1B5 OR accession:A4GXA9 OR accession:A5D8V7 OR accession:A5PLL7 OR '
                         'accession:A6BM72 OR accession:A6H8Y1 OR accession:A6NCS4 OR accession:A6NFY7 OR '
                         'accession:A6NGG8 OR accession:A6NI61 OR accession:A6NKB5',
            'fileName': 'uniprot.json.gz',
            'version': 'https://ftp.uniprot.org/pub/databases/uniprot/current_release/RELEASE.metalink',
            'versionCallback': parseUniProtVersion,
            'onceOnly': False
        },
        {
            'key': 'jensenlab-textmining-mentions',
            'source': 'JensenLab textmining mentions',
            'baseUrl': 'https://download.jensenlab.org/',
            'query': 'human_textmining_mentions.tsv',
            'version': {'version': ''},
            'onceOnly': False
        },
        {
            'key': 'gtex',
            'source': 'GTEx',
            'baseUrl': 'https://storage.googleapis.com/gtex_analysis_v8/rna_seq_data/',
            'query': 'GTEx_Analysis_2017-06-05_v8_RNASeQCv1.1.9_gene_tpm.gct.gz',
            'version': {
                'version': 'GTEx Analysis Version 8',
                'release date': '2017-06-05',
                'sample attributes': 'GTEx_Analysis_v8_Annotations_SampleAttributesDS.txt',
                'subject phenotypes': 'GTEx_Analysis_v8_Annotations_SubjectPhenotypesDS.txt'
            },
            'supplemental files': [
                'https://storage.googleapis.com/gtex_analysis_v8/annotations/GTEx_Analysis_v8_Annotations_SampleAttributesDD.xlsx',
                'https://storage.googleapis.com/gtex_analysis_v8/annotations/GTEx_Analysis_v8_Annotations_SampleAttributesDS.txt',
                'https://storage.googleapis.com/gtex_analysis_v8/annotations/GTEx_Analysis_v8_Annotations_SubjectPhenotypesDD.xlsx',
                'https://storage.googleapis.com/gtex_analysis_v8/annotations/GTEx_Analysis_v8_Annotations_SubjectPhenotypesDS.txt'
            ],
            'onceOnly': True
        },
        {
            'key': 'hpa-protein',
            'source': 'Expression/HPA Protein',
            'baseUrl': 'https://www.proteinatlas.org/download/',
            'query': 'normal_tissue.tsv.zip',
            'version': {
                'version': 'The Human Protein Atlas version 21.1',
                'release date': '2022-05-31',
                'manual map': '../manual_uberon_map.tsv'
            },
            'onceOnly': True
        },
        {
            'key': 'hpa-rna',
            'source': 'Expression/HPA RNA',
            'baseUrl': 'https://www.proteinatlas.org/download/',
            'query': 'rna_tissue_hpa.tsv.zip',
            'version': {
                'version': 'The Human Protein Atlas version 21.1',
                'release date': '2022-05-31',
                'manual map': '../manual_uberon_map.tsv'
            },
            'onceOnly': True
        },
        {
            'key': 'jensenlab-tissues',
            'source': 'Expression/JensenLab TISSUES',
            'baseUrl': 'https://download.jensenlab.org/',
            'query': 'human_tissue_integrated_full.tsv',
            'supplemental files': [
                'https://download.jensenlab.org/human_dictionary.tar.gz',
            ],
            'version': {
                'version': 'TISSUES 2.0',
                'manual map': '../manual_uberon_map.tsv',
                'human entities': 'human_entities.tsv'
            },
            'onceOnly': False
        },
        {
            'key': 'uberon',
            'source': 'Uberon',
            'githubReleaseUrl': 'https://github.com/obophenotype/uberon/releases/latest',
            'githubCallback': parseUberonData,
            'query': 'ext.obo',
            'onceOnly': False
        }
    ]

    downloadTasks = []
    for sourceObj in sources:
        downloadTasks.append(
            PythonOperator(
                dag=dag,
                task_id=f"download-{sourceObj['key']}",
                python_callable=downloadData,
                op_kwargs={'sourceObj': sourceObj}
            )
        )
