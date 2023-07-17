import time
from common import common
from datetime import datetime
from urllib import request, parse
import json
import calendar
import xml.etree.ElementTree as ET

# Gene Details XML Selectors
rootValidGeneSelector = "Entrezgene/Entrezgene_track-info/Gene-track/Gene-track_status[@value='live']/../../.."
geneGeneIDSelector = "Entrezgene_track-info/Gene-track/Gene-track_geneid"
geneSummarySelector = "Entrezgene_summary"
geneGOpubSelector = "Entrezgene_properties/Gene-commentary/Gene-commentary_comment/Gene-commentary/Gene-commentary_comment/Gene-commentary/Gene-commentary_refs/Pub/Pub_pmid/PubMedId"
genePubSelector = "Entrezgene_comments/Gene-commentary/Gene-commentary_refs/Pub/Pub_pmid/PubMedId"
geneGenerifSelector = "Entrezgene_comments/Gene-commentary/Gene-commentary_type[@value='generif']/.."
rifTextSelector = "Gene-commentary_text"
rifRefSelector = "Gene-commentary_refs/Pub/Pub_pmid/PubMedId"
rifInteractionNodeSelector = "Gene-commentary_heading"
rifCreateDateSelector = 'Gene-commentary_create-date/Date/Date_std/Date-std'
rifUpdateDateSelector = 'Gene-commentary_update-date/Date/Date_std/Date-std'

months_rdict = {v: str(i) for i,v in enumerate(calendar.month_abbr)}
mysqlserver = common.getMysqlConnector()
databaseName = common.getNewDatabaseName()
uniprotDict = {uniprot:id for (id, uniprot) in mysqlserver.get_records(f"select id, uniprot from {databaseName}.protein")}
current_date = datetime.now()
rifDelimiter = "||||"
generifID = 0
def getGenerifID():
    global generifID
    if generifID == 0:
        res = mysqlserver.get_records(f"select max(id) from {databaseName}.generif")
        if res[0][0] is not None:
            generifID = res[0][0]
    generifID += 1
    return generifID

class NcbiQueue:
    def __init__(self):
        self.requestQueue = []
        self.resultQueue = []

    def __str__(self):
        res = f"Jobs in Queue: {len(self.requestQueue)}"
        for entry in self.requestQueue:
            res = res + f"\n\t{entry}"
        res = res + f"\nJob Results: {len(self.resultQueue)}"
        for entry in self.resultQueue:
            res = res + f"\n\t{entry}"
        return res

    def addJob(self, type, arguments):
        self.requestQueue.append(NcbiJob(type, arguments))

    def runOneJob(self):
        if len(self.requestQueue) == 0:
            return False
        job = self.requestQueue.pop(0)
        if not job.tryJob(self):
            if (job.attempts <= 4):
                self.requestQueue.append(job)
            else:
                print(f'Job permanently failed {job}')
        else:
            self.resultQueue.append(job)
        return True

    def getInserts(self):
        geneIDinserts = []
        generifInserts = []
        protein2pubmedInserts = []
        generif2pubmedInserts = []
        for job in self.resultQueue:
            if job.type == 'GetIDs':
                for geneid in job.results:
                    geneIDinserts.append(['NCBI Gene ID', uniprotDict[job.arguments], geneid])
            if job.type == 'GetDetails':
                protein_id = uniprotDict[job.arguments['accession']]
                for geneid in job.results:
                    rifDict = {}
                    for row in job.results[geneid]['pmids']:
                        pmid = row['pmid']
                        protein2pubmedInserts.append((protein_id, pmid, geneid, 'NCBI'))
                        if 'rifs' in row:
                            for rifObj in row['rifs']:
                                rifString = f"{rifObj['rif']}{rifDelimiter}{rifObj['rifDate'].isoformat()}"
                                if rifString in rifDict:
                                    rifDict[rifString].append(pmid)
                                else:
                                    rifDict[rifString] = [pmid]
                    for rifString in rifDict:
                        (rifText, rifDate) = rifString.split(rifDelimiter)
                        generifID = getGenerifID()
                        generifInserts.append((generifID, protein_id, geneid, rifText, rifDate))
                        generif2pubmedInserts.extend([(generifID, pmid) for pmid in rifDict[rifString]])

        return (geneIDinserts, generifInserts, protein2pubmedInserts, generif2pubmedInserts)

    def doInserts(self, generifInserts, protein2pubmedInserts, generif2pubmedInserts, geneIDinserts):
        mysqlserver.insert_many_rows(f'{databaseName}.generif', generifInserts, target_fields=('id', 'protein_id', 'gene_id', 'text', 'date'))
        mysqlserver.insert_many_rows(f'{databaseName}.protein2pubmed', protein2pubmedInserts, target_fields=('protein_id','pubmed_id', 'gene_id', 'source'))
        mysqlserver.insert_many_rows(f'{databaseName}.generif2pubmed', generif2pubmedInserts, target_fields=('generif_id','pubmed_id'))
        mysqlserver.insert_many_rows(f'{databaseName}.alias', geneIDinserts, target_fields=('type','protein_id', 'value'))

class NcbiJob:
    def __init__(self, type, arguments):
        self.type = type
        self.arguments = arguments
        self.attempts = 0
        self.results = []

    def __str__(self):
        res = f"{self.type} -> {self.arguments} -> {self.attempts} attempts"
        if self.results is not None:
            res += f"{self.__strObject(self.results)}"
        return res

    def __strObject(self, object):
        res = ""
        if type(object) is list:
            for entry in object:
                res += f"\n{self.__strObject(entry)}"
        elif type(object) is dict:
            for key in object:
                res += f"\n{key}: {self.__strObject(object[key])}"
        else:
            res += f"{object}"
        return res

    def tryJob(self, queue):
        self.attempts += 1
        try:
            if (self.type == 'GetIDs'):
                ids = fetchGeneIDs(self.arguments, 'primaryAccession')
                self.results = ids
                if ids is not None and len(ids) > 0:
                    queue.addJob('GetDetails', {'ids': ",".join(ids), 'accession': self.arguments})
            if (self.type == 'GetDetails'):
                results = fetchGeneDetails(self.arguments['ids'])
                self.results = results
            return True
        except Exception as e:
            print(f"Job failed: {self}")
            print(e)
            return False

def appendPub(list, pmid, rif, rifDate):
    pubObj = next(filter(lambda each: pmid == each['pmid'], list), None)
    isNew = False
    if pubObj is None:
        pubObj = {'pmid': pmid}
        isNew = True
        list.append(pubObj)
    addRifData(pubObj, rif, rifDate)
    return isNew

def addRifData(pubObj, rif, rifDate):
    if rif is not None:
        if 'rifs' not in pubObj:
            pubObj['rifs'] = []
        rifObj = {'rif': rif, 'rifDate': rifDate}
        pubObj['rifs'].append(rifObj)

def fetchLastModDate(node):
    updateTime = fetchPythonDateFromStdNode(node.find(rifUpdateDateSelector))
    if updateTime is not None:
        return updateTime
    return fetchPythonDateFromStdNode(node.find(rifCreateDateSelector))

def fetchPythonDateFromStdNode(node):
    if node is None:
        return None
    yearNode = node.find('Date-std_year')
    monthNode = node.find('Date-std_month')
    dayNode = node.find('Date-std_day')
    hourNode = node.find('Date-std_hour')
    minuteNode = node.find('Date-std_minute')
    secondNode = node.find('Date-std_second')
    return datetime(getIntegerFromTextNode(yearNode), getIntegerFromTextNode(monthNode), getIntegerFromTextNode(dayNode),
                    getIntegerFromTextNode(hourNode), getIntegerFromTextNode(minuteNode), getIntegerFromTextNode(secondNode))

def getIntegerFromTextNode(node):
    if node is not None:
        return int(node.text)
    return 0

def fetchGeneDetails(geneList):
    query = f"http://www.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi?db=Gene&rettype=xml&id={geneList}"
    print(query)
    req = request.urlopen(query)
    root = ET.fromstring(req.read())
    results = {}
    for activeGene in root.findall(rootValidGeneSelector):
        geneid = int(activeGene.find(geneGeneIDSelector).text)
        results[geneid] = {'pmids': [], 'summary': None}
        summary = activeGene.find(geneSummarySelector)
        if summary is not None:
            results[geneid]['summary'] = summary.text
        for goPub in activeGene.findall(geneGOpubSelector):
            appendPub(results[geneid]['pmids'], goPub.text, None, None)
        for pub in activeGene.findall(genePubSelector):
            appendPub(results[geneid]['pmids'], pub.text, None, None)
        for rif in activeGene.findall(geneGenerifSelector):
            interactionNode = rif.find(rifInteractionNodeSelector)
            if (interactionNode is not None and interactionNode.text == 'Interactions'):
                continue
            rifText = rif.find(rifTextSelector)
            rifDate = fetchLastModDate(rif)
            for ref in rif.findall(rifRefSelector):
                appendPub(results[geneid]['pmids'], ref.text, rifText.text, rifDate)
    return results

def fetchGeneIDs(id, field):
    params = f"9606[taxid]{id}[{field}]"
    query = f"http://www.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi?db=Gene&retmode=json&term={parse.quote(params)}"
    print(query)
    req = request.urlopen(query)
    resp = json.loads(req.read())
    if 'esearchresult' in resp:
        if 'idlist' in resp['esearchresult']:
            return resp['esearchresult']['idlist']
    return None

def setIfExists(dictionary, field, node, selector):
    dataNode = node.find(selector)
    if dataNode is not None:
        dictionary[field] = dataNode.text
    else:
        dictionary[field] = None

if __name__ == '__main__':
    getGenerifID()
    uniprotList = [
        'P34096', # RNASE4
        'A0A087X1C5', # normal
        'P01023', # gene_id = 2
        'B2RXF5', # 1 good id, and 1 old gene id
        'P02812', # 1 good id, and 3 old gene ids
        'O75920', # 2 valid gene ids
        'Q9GZZ8', # multiple rifs for one pub
        'P0C0S8', # 5 valid gene ids
        'THISISABADID' # no corresponding gene ids
    ]
    queue = NcbiQueue()
    # for uniprot in uniprotList:
    #     queue.addJob("GetIDs", uniprot)

    # queue.addJob("GetDetails", {'ids': '100509620', 'accession': 'A0A075B734'}) # failed from the thing
    # queue.addJob("GetDetails", {'ids': '102723475', 'accession': 'A0A087WTH5'}) # failed from the thing
    # queue.addJob("GetDetails", {'ids': '100996693', 'accession': 'A0A087WV53'}) # failed from the thing
    # queue.addJob("GetDetails", {'ids': '100507462', 'accession': 'A0A096LPI5'}) # failed from the thing

    while(queue.runOneJob()):
        time.sleep(0.35)

    geneIDinserts, generifInserts, protein2pubmedInserts, generif2pubmedInserts = queue.getInserts()

    # mysqlserver.insert_many_rows('generif', generifInserts, target_fields=('id', 'protein_id', 'gene_id', 'text', 'date'))
    # mysqlserver.insert_many_rows('protein2pubmed', protein2pubmedInserts, target_fields=('protein_id','pubmed_id', 'gene_id', 'source'))
    # mysqlserver.insert_many_rows('generif2pubmed', generif2pubmedInserts, target_fields=('generif_id','pubmed_id'))
    # mysqlserver.insert_many_rows('alias', geneIDinserts, target_fields=('type','protein_id', 'value'))

    # [protein2pubmedInserts, generif2pubmedInserts]
    # geneRifInserts >> generif2pubmedInserts

