#!/usr/bin/env python
# Time-stamp: <2019-01-18 11:43:18 smathias>
"""Load antibody count and URL tdl_infos into TCRD via Antibodtpedia.com API.

Usage:
    load-Antibodtpedia.py [--debug | --quiet] [--dbhost=<str>] [--dbname=<str>] [--logfile=<file>] [--loglevel=<int>]
    load-Antibodtpedia.py -h | --help

Options:
  -h --dbhost DBHOST   : MySQL database host name [default: localhost]
  -n --dbname DBNAME   : MySQL database name [default: tcrdev]
  -l --logfile LOGF    : set log file name
  -v --loglevel LOGL   : set logging level [default: 30]
                         50: CRITICAL
                         40: ERROR
                         30: WARNING
                         20: INFO
                         10: DEBUG
                          0: NOTSET
  -q --quiet           : set output verbosity to minimal level
  -d --debug           : turn on debugging output
  -? --help            : print this message and exit 
"""
__author__ = "Steve Mathias"
__email__ = "smathias@salud.unm.edu"
__org__ = "Translational Informatics Division, UNM School of Medicine"
__copyright__ = "Copyright 2019, Steve Mathias"
__license__ = "Creative Commons Attribution-NonCommercial (CC BY-NC)"
__version__ = "1.0.0"

import os,sys,time
from docopt import docopt
from TCRDMP import DBAdaptor
import logging
import requests
import json
from progressbar import *
import slm_tcrd_functions as slmf

PROGRAM = os.path.basename(sys.argv[0])
LOGDIR = "./tcrd6logs"
LOGFILE = "%s/%s.log" % (LOGDIR, PROGRAM)
ABPC_API_URL = 'http://www.antibodypedia.com/tools/antibodies.php?uniprot='
INFILE = 'tcrd6logs/Abp-Missing.txt'

def load(args):
  loglevel = int(args['--loglevel'])
  if args['--logfile']:
    logfile = args['--logfile']
  else:
    logfile = LOGFILE
  logger = logging.getLogger(__name__)
  logger.setLevel(loglevel)
  if not args['--debug']:
    logger.propagate = False # turns off console logging
  fh = logging.FileHandler(logfile)
  fmtr = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s: %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
  fh.setFormatter(fmtr)
  logger.addHandler(fh)

  # DBAdaptor uses same logger as load()
  dba_params = {'dbhost': args['--dbhost'], 'dbname': args['--dbname'], 'logger_name': __name__}
  dba = DBAdaptor(dba_params)
  dbi = dba.get_dbinfo()
  logger.info("Connected to TCRD database %s (schema ver %s; data ver %s)", args['--dbname'], dbi['schema_ver'], dbi['data_ver'])
  if not args['--quiet']:
    print "\nConnected to TCRD database {} (schema ver {}; data ver {})".format(args['--dbname'], dbi['schema_ver'], dbi['data_ver'])

  t2up = {}
  with open(INFILE) as ifh:
    for line in ifh:
      line = line.rstrip()
      [up, tid] = line.split(" ")
      t2up[tid] = up
  tct = len(t2up)
  if not args['--quiet']:
    print "\nGot {} UniProt accessions from file {}".format(tct, INFILE)

  pbar_widgets = ['Progress: ',Percentage(),' ',Bar(marker='#',left='[',right=']'),' ',ETA()]
  if not args['--quiet']:
    print "\nLoading Antibodypedia annotations for {} targets".format(tct)
  logger.info("Loading Antibodypedia annotations for {} targets".format(tct))
  pbar = ProgressBar(widgets=pbar_widgets, maxval=tct).start()
  ct = 0
  tiab_ct = 0
  timab_ct = 0
  tiurl_ct = 0
  dba_err_ct = 0
  net_err_ct = 0
  for tid,up in t2up.items():
    ct += 1
    pid = int(tid)
    pbar.update(ct)
    url = ABPC_API_URL + up
    r = None
    attempts = 1
    while attempts <= 5:
      try:
        logger.info("Getting {} [Target {}, attempt {}]".format(url, tid, attempts))
        r = requests.get(url)
        break
      except:
        attempts += 1
        time.sleep(1)
    if not r:
      net_err_ct += 1
      logger.error("No response for {} [Target {}, attempt {}]".format(url, tid, attempts))
      continue
    if r.status_code != 200:
      net_err_ct += 1
      logger.error("Bad response: {} for {} [Target {}, attempt {}]".format(r.status_code, url, tid, attempts))
      continue
    abpd = json.loads(r.text)
    rv = dba.ins_tdl_info({'protein_id': pid, 'itype': 'Ab Count',
                           'integer_value': int(abpd['num_antibodies'])})
    if rv:
      tiab_ct += 1
    else:
      dba_err_ct += 1
    if 'ab_type_monoclonal' in abpd:
      mab_ct = int(abpd['ab_type_monoclonal'])
    else:
      mab_ct = 0
    rv = dba.ins_tdl_info({'protein_id': pid, 'itype': 'MAb Count',
                           'integer_value': mab_ct})
    if rv:
      timab_ct += 1
    else:
      dba_err_ct += 1
    rv = dba.ins_tdl_info({'protein_id': pid, 'itype': 'Antibodypedia.com URL',
                           'string_value': abpd['url']})
    if rv:
      tiurl_ct += 1
    else:
      dba_err_ct += 1
    time.sleep(1)
    pbar.update(ct)
  pbar.finish()
  print "{} TCRD targets processed.".format(ct)
  print "  Inserted {} Ab Count tdl_info rows".format(tiab_ct)
  print "  Inserted {} MAb Count tdl_info rows".format(timab_ct)
  print "  Inserted {} Antibodypedia.com URL tdl_info rows".format(tiurl_ct)
  if net_err_ct > 0:
    print "WARNING: Network error for {} targets. See logfile {} for details.".format(net_err_ct, logfile)
  if dba_err_ct > 0:
    print "WARNING: {} DB errors occurred. See logfile {} for details.".format(dba_err_ct, logfile)

if __name__ == '__main__':
  print "\n%s (v%s) [%s]:" % (PROGRAM, __version__, time.strftime("%c"))
  args = docopt(__doc__, version=__version__)
  debug = int(args['--debug'])
  if debug:
    print "\n[*DEBUG*] ARGS:\n%s\n"%repr(args)
  start_time = time.time()
  load(args)
  elapsed = time.time() - start_time
  print "\n%s: Done. Elapsed time: %s\n" % (PROGRAM, slmf.secs2str(elapsed))
