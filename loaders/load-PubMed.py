#!/usr/bin/env python
"""Load PubMed data into TCRD via EUtils.

Usage:
    load-PubMed.py [--debug=<int> | --quiet] [--dbhost=<str>] [--dbname=<str>] [--logfile=<file>] [--loglevel=<int>] [--pastid=<int>]
    load-PubMed.py -h | --help

Options:
  -h --dbhost DBHOST   : MySQL database host name [default: localhost]
  -n --dbname DBNAME   : MySQL database name [default: tcrd]
  -l --logfile LOGF    : set log file name
  -v --loglevel LOGL   : set logging level [default: 30]
                         50: CRITICAL
                         40: ERROR
                         30: WARNING
                         20: INFO
                         10: DEBUG
                          0: NOTSET
  -p --pastid PASTID   : TCRD target id to start at (for restarting frozen run)
  -q --quiet           : set output verbosity to minimal level
  -d --debug DEBUGL    : set debugging output level (0-3) [default: 0]
  -? --help            : print this message and exit 
"""
__author__    = "Steve Mathias"
__email__     = "smathias @salud.unm.edu"
__org__       = "Translational Informatics Division, UNM School of Medicine"
__copyright__ = "Copyright 2015-2016, Steve Mathias"
__license__   = "Creative Commons Attribution-NonCommercial (CC BY-NC)"
__version__   = "2.0.0"

import os,sys,time,urllib,re
from docopt import docopt
from TCRD import DBAdaptor
import logging
from progressbar import *
import requests
from bs4 import BeautifulSoup
import shelve
import calendar

PROGRAM = os.path.basename(sys.argv[0])
EMAIL = 'smathias@salud.unm.edu'
EFETCHURL = "http://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi?&db=pubmed&retmode=xml&email=%s&tool=%s&id=" % (urllib.quote(EMAIL), urllib.quote(PROGRAM))
SHELF_FILE = '/home/app/TCRD/scripts/tcrd3logs/load-PubMed.db'

def main():
  args = docopt(__doc__, version=__version__)
  debug = int(args['--debug'])
  if debug:
    print "\n[*DEBUG*] ARGS:\n%s\n"%repr(args)
  
  loglevel = int(args['--loglevel'])
  if args['--logfile']:
    logfile = args['--logfile']
  else:
    logfile = "%s.log" % PROGRAM
  logger = logging.getLogger(__name__)
  logger.setLevel(loglevel)
  if not debug:
    logger.propagate = False # turns off console logging when debug is 0
  fh = logging.FileHandler(logfile)
  fmtr = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s: %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
  fh.setFormatter(fmtr)
  logger.addHandler(fh)

  # DBAdaptor uses same logger as main()
  dba_params = {'dbhost': args['--dbhost'], 'dbname': args['--dbname'], 'logger_name': __name__}
  dba = DBAdaptor(dba_params)
  dbi = dba.get_dbinfo()
  logger.info("Connected to TCRD database %s (schema ver %s; data ver %s)", args['--dbname'], dbi['schema_ver'], dbi['data_ver'])
  if not args['--quiet']:
    print "\n%s (v%s) [%s]:" % (PROGRAM, __version__, time.strftime("%c"))
    print "\nConnected to TCRD database %s (schema ver %s; data ver %s)" % (args['--dbname'], dbi['schema_ver'], dbi['data_ver'])
  
  # Dataset
  dataset_id = dba.ins_dataset( {'name': 'PubMed', 'source': 'NCBI E-Utils', 'app': PROGRAM, 'app_version': __version__, 'url': 'https://www.ncbi.nlm.nih.gov/pubmed'} )
  if not dataset_id:
    print "WARNING: Error inserting dataset See logfile %s for details." % logfile
    sys.exit(1)
  # Provenance
  rv = dba.ins_provenance({'dataset_id': dataset_id, 'table_name': ''pubmed})
  if not rv:
    print "WARNING: Error inserting provenance. See logfile %s for details." % logfile
    sys.exit(1)

  start_time = time.time()

  s = shelve.open(SHELF_FILE, writeback=True)
  s['pmids'] = [] # list of stored pubmed ids
  s['p2p_ct'] = 0

  pbar_widgets = ['Progress: ',Percentage(),' ',Bar(marker='#',left='[',right=']'),' ',ETA()]
  if args['--pastid']:
    tct = dba.get_target_count(idg=False, past_id=args['--pastid'])
  else:
    tct = dba.get_target_count(idg=False)
  if not args['--quiet']:
    print "\nLoading pubmeds for %d TCRD targets" % tct
    logger.info("Loading pubmeds for %d TCRD targets" % tct)
  pbar = ProgressBar(widgets=pbar_widgets, maxval=tct).start()  
  ct = 0
  net_err_ct = 0
  dba_err_ct = 0
  if args['--pastid']:
    past_id = args['--pastid']
  else:
    past_id = 0
  for target in dba.get_targets(include_annotations=True, past_id=past_id):
    ct += 1
    logger.info("Processing target %d: %s" % (target['id'], target['name']))
    p = target['components']['protein'][0]
    if 'PubMed' not in p['xrefs']: continue
    pmids = [d['value'] for d in p['xrefs']['PubMed']]
    chunk_ct = 0
    for chunk in chunker(pmids, 200):
      chunk_ct += 1
      r = get_pubmed(chunk)
      if not r or r.status_code != 200:
        logger.error("Bad E-Utils response for target %s, chunk %d" % (target['id'], chunk_ct))
        net_err_ct += 1
        continue       
      soup = BeautifulSoup(r.text, "xml")
      pmas = soup.find('PubmedArticleSet')
      for pma in pmas.findAll('PubmedArticle'):
        pmid = pma.find('PMID').text
        if pmid not in s['pmids']:
          # only store each pubmed once
          logger.debug("  parsing XML for PMID: %s" % pmid)
          init = parse_pubmed_article(pma)
          rv = dba.ins_pubmed(init)
          if not rv:
            dba_err_ct += 1
            continue
          s['pmids'].append(pmid) # add pubmed id to list of saved ones
        rv = dba.ins_protein2pubmed({'protein_id': p['id'], 'pubmed_id': pmid})
        if not rv:
          dba_err_ct += 1
          continue
        s['p2p_ct'] += 1
      pbar.update(ct)
    time.sleep(0.5)
  pbar.finish()
  elapsed = time.time() - start_time
  print "Processed %d targets. Elapsed time: %s" % (ct, secs2str(elapsed))
  print "  Inserted %d new pubmed rows" % len(s['pmids'])
  print "  Inserted %d new protein2pubmed rows" % s['p2p_ct']
  if dba_err_ct > 0:
    print "WARNING: %d DB errors occurred. See logfile %s for details." % (dba_err_ct, logfile)
  if net_err_ct > 0:
    print "WARNING: %d Network/E-Utils errors occurred. See logfile %s for details." % (net_err_ct, logfile)

  start_time = time.time()
  tinx_pmids = [str(pmid) for pmid in dba.get_tinx_pmids()]
  tinx_pmid_ct = len(tinx_pmids)
  if not args['--quiet']:
    print "\nProcessing %d TIN-X PubMed IDs" % tinx_pmid_ct
    logger.info("Processing %d TIN-X PubMed IDs" % tinx_pmid_ct)
  ct = 0
  pm_ct = 0
  net_err_ct = 0
  dba_err_ct = 0
  chunk_ct = 0
  for chunk in chunker(tinx_pmids, 200):
    chunk_ct += 1
    r = get_pubmed(chunk)
    if not r or r.status_code != 200:
      logger.error("Bad E-Utils response for chunk %d" % chunk_ct)
      net_err_ct += 1
      continue       
    soup = BeautifulSoup(r.text, "xml")
    pmas = soup.find('PubmedArticleSet')
    for pma in pmas.findAll('PubmedArticle'):
      ct += 1
      pmid = pma.find('PMID').text
      if pmid not in s['pmids']:
        # only store each pubmed once
        logger.debug("  parsing XML for PMID: %s" % pmid)
        init = parse_pubmed_article(pma)
        rv = dba.ins_pubmed(init)
        if not rv:
          dba_err_ct += 1
          continue
        pm_ct += 1
        s['pmids'].append(pmid) # add pubmed id to list of saved ones
    time.sleep(0.5)
  elapsed = time.time() - start_time
  print "Processed %d TIN-X PubMed IDs. Elapsed time: %s" % (ct, secs2str(elapsed))
  print "  Inserted %d new pubmed rows" % pm_ct
  if dba_err_ct > 0:
    print "WARNING: %d DB errors occurred. See logfile %s for details." % (dba_err_ct, logfile)
  if net_err_ct > 0:
    print "WARNING: %d Network/E-Utils errors occurred. See logfile %s for details." % (net_err_ct, logfile)
  
  s.close()
  
  print "\n%s: Done." % PROGRAM
  print

def chunker(l, size):
  return (l[pos:pos + size] for pos in xrange(0, len(l), size))

def get_pubmed(pmids):
  url = EFETCHURL + ','.join(pmids)
  attempts = 0
  r = None
  while attempts <= 5:
    try:
      r = requests.get(url)
      break
    except:
      attempts += 1
      time.sleep(2)
  if r:
    return r
  else:
    return False

# map abbreviated month names to ints
months_rdict = {v: str(i) for i,v in enumerate(calendar.month_abbr)}
mld_regex = re.compile(r'(\d{4}) (\w{3}) (\d\d?)-')

def pubdate2isostr(pubdate):
  """Turn a PubDate XML element into an ISO-type string (ie. YYYY-MM-DD)."""
  if pubdate.find('MedlineDate'):
    mld = pubdate.find('MedlineDate').text
    m = mld_regex.search(mld)
    if not m:
      return None
    month = months_rdict.get(m.groups(1), None)
    if not month:
      return m.groups()[0]
    return "%s-%s-%s" % (m.groups()[0], month, m.groups()[2])
  else:
    year = pubdate.find('Year').text
    if not pubdate.find('Month'):
      return year
    month = pubdate.find('Month').text
    if not month.isdigit():
      month = months_rdict.get(month, None)
      if not month:
        return year
    if pubdate.find('Day'):
      day = pubdate.find('Day').text
      return "%s-%s-%s" % (year, month.zfill(2), day.zfill(2))
    else:
      return "%s-%s" % (year, month.zfill(2))

def parse_pubmed_article(pma):
  """
  Parse a BeautifulSoup PubmedArticle into a dict suitable to use as an argument
  to TCRC.DBAdaptor.ins_pubmed().
  """
  pmid = pma.find('PMID').text
  article = pma.find('Article')
  title = article.find('ArticleTitle').text
  init = {'id': pmid, 'title': title }
  journal = article.find('Journal')
  pd = journal.find('PubDate')
  if pd:
    init['date'] = pubdate2isostr(pd)
  jt = journal.find('Title')
  if jt:
    init['journal'] = jt.text
  authors = pma.findAll('Author')
  if len(authors) > 0:
    if len(authors) > 5:
      # For papers with more than five authors, the authors field will be
      # formated as: "Mathias SL and 42 more authors."
      a = authors[0]
      # if the first author has no last name, we skip populating the authors field
      if a.find('LastName'):
        astr = "%s" % a.find('LastName').text
        if a.find('ForeName'):
          astr += ", %s" % a.find('ForeName').text
        if a.find('Initials'):
          astr += " %s" % a.find('Initials').text
        init['authors'] = "%s and %d more authors." % (astr, len(authors)-1)
    else:
      # For papers with five or fewer authors, the authors field will have all their names
      auth_strings = []
      last_auth = authors.pop()
      # if the last author has no last name, we skip populating the authors field
      if last_auth.find('LastName'):
        last_auth_str = "%s" % last_auth.find('LastName').text
        if last_auth.find('ForeName'):
          last_auth_str += ", %s" % last_auth.find('ForeName').text
        if last_auth.find('Initials'):
          last_auth_str += " %s" % last_auth.find('Initials').text
        for a in authors:
          if a.find('LastName'): # if authors have no last name, we skip them
            astr = "%s" % a.find('LastName').text
            if a.find('ForeName'):
              astr += ", %s" % a.find('ForeName').text
            if a.find('Initials'):
              astr += " %s" % a.find('Initials').text
            auth_strings.append(astr)
        init['authors'] = "%s and %s." % (", ".join(auth_strings), last_auth_str)
  abstract = article.find('AbstractText')
  if abstract:
    init['abstract'] = abstract.text
  return init

# Use this to manually insert errors
# In [26]: t = dba.get_target(18821, include_annotations=True)
# In [27]: p = target['components']['protein'][0]
# In [28]: pmids = [d['value'] for d in p['xrefs']['PubMed']]
# In [29]: len(pmids)
# Out[29]: 1387
# In [34]: url = EFETCHURL + ','.join(pmids[0:200])
# In [35]: r = requests.get(url)
# In [36]: r
# Out[36]: <Response [200]>
# In [43]: parse_insert(r)
# Inserted/Skipped 200 pubmed rows
# ct = 1
# for chunk in chunker(pmids, 200):
#   url = EFETCHURL + ','.join(pmids[0:200])
#   r = requests.get(url)
#   print "Chunk %d: %s" % (ct, r.status_code)
#   parse_insert(r)
# def parse_insert(r):
#   ct = 0
#   soup = BeautifulSoup(r.text, "xml")
#   pmas = soup.find('PubmedArticleSet')
#   for pma in pmas.findAll('PubmedArticle'):
#     pmid = pma.find('PMID').text
#     article = pma.find('Article')
#     title = article.find('ArticleTitle').text
#     init = {'id': pmid, 'protein_id': p['id'], 'title': title }
#     pd = article.find('PubDate')
#     if pd:
#       init['date'] = pubdate2isostr(pd)
#       abstract = article.find('AbstractText')
#     if abstract:
#       init['abstract'] = abstract.text
#     dba.ins_pubmed(init)
#     ct += 1
#   print "Inserted/Skipped %d pubmed rows" % ct

def secs2str(t):
  return "%d:%02d:%02d.%03d" % reduce(lambda ll,b : divmod(ll[0],b) + ll[1:], [(t*1000,),1000,60,60])

if __name__ == '__main__':
  main()