from lxml import objectify
from airflow.models import Variable

UP_HUMAN_FILE = './ETL/input_files/uniprot-short.xml'
ECO_OBO_FILE = '../data/eco.obo'
NS = '{http://uniprot.org/uniprot}'

def loadUniprot(*args, **kwargs):
    root = objectify.parse(UP_HUMAN_FILE).getroot()
    eco_map = [] # mk_eco_map()
    for uniprotEntry in root.entry:
        target = entry2target(uniprotEntry, 1, eco_map)
        print(target)
    return 1


def entry2target(entry, dataset_id, e2e):
    """
    Convert an entry element of type lxml.objectify.ObjectifiedElement parsed from a UniProt XML entry and return a target dictionary suitable for passing to TCRD.DBAdaptor.ins_target().
    """
    target = {'name': entry.protein.recommendedName.fullName, 'ttype': 'Single Protein'}
    target['components'] = {}
    target['components']['protein'] = []
    protein = {'uniprot': entry.accession} # returns first accession
    protein['name'] = entry.name
    protein['description'] = entry.protein.recommendedName.fullName
    protein['sym'] = None
    aliases = []
    if entry.find(NS+'gene'):
        if entry.gene.find(NS+'name'):
            for gn in entry.gene.name: # returns all gene.names
                if gn.get('type') == 'primary':
                    protein['sym'] = gn
                elif gn.get('type') == 'synonym':
                    # HGNC symbol alias
                    aliases.append( {'type': 'symbol', 'dataset_id': dataset_id, 'value': gn} )
    protein['seq'] = str(entry.sequence).replace('\n', '')
    protein['up_version'] = entry.sequence.get('version')
    for acc in entry.accession: # returns all accessions
        if acc != protein['uniprot']:
            aliases.append( {'type': 'uniprot', 'dataset_id': dataset_id, 'value': acc} )
    if entry.protein.recommendedName.find(NS+'shortName') != None:
        sn = entry.protein.recommendedName.shortName
        aliases.append( {'type': 'uniprot', 'dataset_id': dataset_id, 'value': sn} )
    protein['aliases'] = aliases
    # TDL Infos, Family, Diseases, Pathways from comments
    tdl_infos = []
    pathways = []
    diseases = []
    if entry.find(NS+'comment'):
        for c in entry.comment:
            if c.get('type') == 'function':
                tdl_infos.append( {'itype': 'UniProt Function',  'string_value': c.getchildren()[0]} )
            if c.get('type') == 'pathway':
                pathways.append( {'pwtype': 'UniProt', 'name': c.getchildren()[0]} )
            if c.get('type') == 'similarity':
                protein['family'] = c.getchildren()[0]
            if c.get('type') == 'disease':
                if not c.find(NS+'disease'):
                    continue
                if c.disease.find(NS+'name') == None:
                    # some dont have a name, so skip those
                    continue
                da = {'dtype': 'UniProt Disease' }
                for el in c.disease.getchildren():
                    if el.tag == NS+'name':
                        da['name'] = el
                    elif el.tag == NS+'description':
                        da['description'] = el
                    elif el.tag == NS+'dbReference':
                        da['did'] = "%s:%s"%(el.attrib['type'], el.attrib['id'])
                if 'evidence' in c.attrib:
                    da['evidence'] = c.attrib['evidence']
                diseases.append(da)
    protein['tdl_infos'] = tdl_infos
    protein['diseases'] = diseases
    protein['pathways'] = pathways
    # GeneID, XRefs, GOAs from dbReferences
    xrefs = []
    goas = []
    for dbr in entry.dbReference:
        if dbr.attrib['type'] == 'GeneID':
            # Some UniProt records have multiple Gene IDs
            # So, only take the first one and fix manually after loading
            if 'geneid' not in protein:
                protein['geneid'] = dbr.attrib['id']
        elif dbr.attrib['type'] in ['InterPro', 'Pfam', 'PROSITE', 'SMART']:
            xtra = None
            for el in dbr.findall(NS+'property'):
                if el.attrib['type'] == 'entry name':
                    xtra = el.attrib['value']
                xrefs.append( {'xtype': dbr.attrib['type'], 'dataset_id': dataset_id,
                               'value': dbr.attrib['id'], 'xtra': xtra} )
        elif dbr.attrib['type'] == 'GO':
            name = None
            goeco = None
            assigned_by = None
            for el in dbr.findall(NS+'property'):
                if el.attrib['type'] == 'term':
                    name = el.attrib['value']
                elif el.attrib['type'] == 'evidence':
                    goeco = el.attrib['value']
                elif el.attrib['type'] == 'project':
                    assigned_by = el.attrib['value']
            goas.append( {'go_id': dbr.attrib['id'], 'go_term': name,
                          'goeco': goeco, 'evidence': 'hellifino', 'assigned_by': assigned_by} )
        elif dbr.attrib['type'] == 'Ensembl':
            xrefs.append( {'xtype': 'Ensembl', 'dataset_id': dataset_id, 'value': dbr.attrib['id']} )
            for el in dbr.findall(NS+'property'):
                if el.attrib['type'] == 'protein sequence ID':
                    xrefs.append( {'xtype': 'Ensembl', 'dataset_id': dataset_id, 'value': el.attrib['value']} )
                elif el.attrib['type'] == 'gene ID':
                    xrefs.append( {'xtype': 'Ensembl', 'dataset_id': dataset_id, 'value': el.attrib['value']} )
        elif dbr.attrib['type'] == 'STRING':
            xrefs.append( {'xtype': 'STRING', 'dataset_id': dataset_id, 'value': dbr.attrib['id']} )
        elif dbr.attrib['type'] == 'DrugBank':
            xtra = None
            for el in dbr.findall(NS+'property'):
                if el.attrib['type'] == 'generic name':
                    xtra = el.attrib['value']
            xrefs.append( {'xtype': 'DrugBank', 'dataset_id': dataset_id, 'value': dbr.attrib['id'],
                           'xtra': xtra} )
        elif dbr.attrib['type'] in ['BRENDA', 'ChEMBL', 'MIM', 'PANTHER', 'PDB', 'RefSeq', 'UniGene']:
            xrefs.append( {'xtype': dbr.attrib['type'], 'dataset_id': dataset_id,
                           'value': dbr.attrib['id']} )
    protein['goas'] = goas
    # Keywords
    for kw in entry.keyword:
        xrefs.append( {'xtype': 'UniProt Keyword', 'dataset_id': dataset_id, 'value': kw.attrib['id'],
                       'xtra': kw} )
    protein['xrefs'] = xrefs
    # Expression
    exps = []
    for ref in entry.reference:
        if ref.find(NS+'source'):
            if ref.source.find(NS+'tissue'):
                ex = {'etype': 'UniProt Tissue', 'tissue': ref.source.tissue, 'boolean_value': 1}
                for el in ref.citation.findall(NS+'dbReference'):
                    if el.attrib['type'] == 'PubMed':
                        ex['pubmed_id'] = el.attrib['id']
                exps.append(ex)
    protein['expressions'] = exps
    # Features
    features = []
    for f in entry.feature:
        init = {'type': f.attrib['type']}
        if 'evidence' in f.attrib:
            init['evidence'] = f.attrib['evidence']
        if 'description' in f.attrib:
            init['description'] = f.attrib['description']
        if 'id' in f.attrib:
            init['srcid'] = f.attrib['id']
        for el in f.location.getchildren():
            if el.tag == NS+'position':
                init['position'] = el.attrib['position']
            else:
                if el.tag == NS+'begin':
                    if 'position' in el.attrib:
                        init['begin'] = el.attrib['position']
                if el.tag == NS+'end':
                    if 'position' in el.attrib:
                        init['end'] = el.attrib['position']
        features.append(init)
    protein['features'] = features

    target['components']['protein'].append(protein)

    return target

# def mk_eco_map():
#     """
#     Return a mapping of Evidence Ontology ECO IDs to Go Evidence Codes.
#     """
#     eco = {}
#     eco_map = {}
#     print ("\nParsing Evidence Ontology file {}".format(ECO_OBO_FILE))
#     parser = obo.Parser(ECO_OBO_FILE)
#     for stanza in parser:
#         eco[stanza.tags['id'][0].value] = stanza.tags
#     regex = re.compile(r'GOECO:([A-Z]{2,3})')
#     for e,d in eco.items():
#         if not e.startswith('ECO:'):
#             continue
#         if 'xref' in d:
#             for x in d['xref']:
#                 m = regex.match(x.value)
#                 if m:
#                     eco_map[e] = m.group(1)
#     return eco_map