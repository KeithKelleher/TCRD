from .keyword import keyword
from .alias import alias
from common import common
from goterm import goterm, go_association

class protein:
    @staticmethod
    def getSymbol(uniProtObj):
        symbol = None
        if ('genes' in uniProtObj and 'geneName' in uniProtObj['genes'][0]):
            symbol = uniProtObj['genes'][0]['geneName']['value']
        return symbol

    def __init__(self, uniProtObj):
        self.name = uniProtObj['uniProtkbId']
        self.description = uniProtObj['proteinDescription']['recommendedName']['fullName']['value']
        self.uniprot = uniProtObj['primaryAccession']
        self.sym = protein.getSymbol(uniProtObj)
        self.family = findFirstComment(uniProtObj, 'SIMILARITY')
        self.seq = uniProtObj['sequence']['value']
        self.preferred_symbol = None
        self.go_associations = findGOTerms(uniProtObj)
        self.keywords = findKeywords(uniProtObj)
        self.aliases = findAliases(uniProtObj)

    def getInsertTuple(self):
        return (self.id, self.name, self.description, self.uniprot, self.sym, self.family, self.seq, self.preferred_symbol)

    def __str__(self):
        return f"{self.uniprot}: {self.description} ({self.preferred_symbol})"

    @staticmethod
    def getFields():
        return ('id', 'name', 'description', 'uniprot', 'sym', 'family', 'seq', 'preferred_symbol' )

    @staticmethod
    def calculatePreferredSymbols(proteinList):
        symbol_dict = {}
        for p in proteinList:
            if p.sym is None or p.sym == '':
                p.preferred_symbol = p.uniprot
            if p.sym in symbol_dict:
                symbol_dict[p.sym].append(p)
            else:
                symbol_dict[p.sym] = [p]
        for key in symbol_dict:
            matching_proteins = symbol_dict[key]
            if len(matching_proteins) > 1:
                for pro in matching_proteins:
                    pro.preferred_symbol = pro.uniprot
            else:
                matching_proteins[0].preferred_symbol = matching_proteins[0].sym

    @staticmethod
    def assignIDs(proteinList):
        id = 1
        for pro in proteinList:
            pro.id = id
            id += 1

    @staticmethod
    def extractGOterms(proteinList):
        go_dict = {}
        association_list = []
        for pro in proteinList:
            for association in pro.go_associations:
                association.protein_id = pro.id
                association_list.append(association)
                if association.id not in go_dict:
                    go_dict[association.id] = goterm(association)

        return (association_list, go_dict)

    @staticmethod
    def extractKeywords(proteinList):
        return protein.extractObjects(proteinList, 'keywords')

    @staticmethod
    def extractAliases(proteinList):
        return protein.extractObjects(proteinList, 'aliases')

    @staticmethod
    def extractObjects(proteinList, field):
        list = []
        for pro in proteinList:
            for obj in getattr(pro, field):
                obj.protein_id = pro.id
                list.append(obj)
        return list


def findFirstComment(proteinObj, type):
    first = next(findComments(proteinObj, type), None)
    if (first is not None and len(first) > 0):
        return first['texts'][0]['value']
    return None

def findComments(proteinObj, type):
    return common.findMatches(proteinObj, 'comments', 'commentType', type)

def findGOTerms(proteinObj):
    return [go_association(term) for term in findCrossRefs(proteinObj, 'GO')]

def findCrossRefs(proteinObj, type):
    return common.findMatches(proteinObj, 'uniProtKBCrossReferences', 'database', type)

def findKeywords(proteinObj):
    return [keyword(keywordObj) for keywordObj in proteinObj['keywords']]

def findAliases(proteinObj):
    aliases = []
    aliases.append(alias('primary accession', proteinObj['primaryAccession']))
    if 'secondaryAccessions' in proteinObj:
        for id in proteinObj['secondaryAccessions']:
            aliases.append(alias('secondary accession', id))
    aliases.append(alias('uniprot kb', proteinObj['uniProtkbId']))
    aliases.append(alias('full name', proteinObj['proteinDescription']['recommendedName']['fullName']['value']))
    if 'shortNames' in proteinObj['proteinDescription']['recommendedName']:
        for obj in proteinObj['proteinDescription']['recommendedName']['shortNames']:
            aliases.append(alias('short name', obj['value']))
    if 'genes' in proteinObj and len(proteinObj['genes']) > 0:
        for gene in proteinObj['genes']:
            if 'geneName' in gene:
                aliases.append(alias('symbol', gene['geneName']['value']))
            if 'synonyms' in gene and len(gene['synonyms']) > 0:
                for synonym in gene['synonyms']:
                    aliases.append(alias('synonym', synonym['value']))
    ensemblObjs = common.findMatches(proteinObj, 'uniProtKBCrossReferences', 'database', 'Ensembl')
    for match in ensemblObjs:
        aliases.append(alias('Ensembl', trimVersion(match['id'])))
        if 'properties' in match:
            for prop in match['properties']:
                aliases.append(alias('Ensembl', trimVersion(prop['value'])))
    stringObjs = common.findMatches(proteinObj, 'uniProtKBCrossReferences', 'database', 'STRING')
    for match in stringObjs:
        aliases.append(alias('STRING', trimSpecies(match['id'])))
    return aliases

def trimVersion(ensembl_id):
    return ensembl_id.split('.')[0]

def trimSpecies(string_id):
    return string_id.split('.')[1]
