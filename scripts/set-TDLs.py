#!/usr/bin/env python
# Time-stamp: <2016-11-16 16:35:34 smathias>
"""Update TCRD TDL assignments.

Usage:
    set-TDLs.py [--debug=<int> | --quiet] [--dbhost=<str>] [--dbname=<str>] [--logfile=<file>] [--loglevel=<int>]
    set-TDLs.py | --help

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

import os,sys,time,re
from docopt import docopt
from TCRD import DBAdaptor
import logging
from progressbar import *

PROGRAM = os.path.basename(sys.argv[0])
DBHOST = 'localhost'
DBPORT = 3306
DBNAME = 'tcrdev'
LOGFILE = './%s.log'%PROGRAM

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

  # Provenance
  rv = dba.ins_provenance({'dataset_id': 1, 'table_name': 'target', 'column_name': 'tdl'})
  if not rv:
    print "WARNING: Error inserting provenance. See logfile %s for details." % logfile
    sys.exit(1)
  
  start_time = time.time()
  pbar_widgets = ['Progress: ',Percentage(),' ',Bar(marker='#',left='[',right=']'),' ',ETA()]
  tct = dba.get_target_count(idg=False)
  if not args['--quiet']:
    print "\nProcessing %d TCRD targets" % tct
  pbar = ProgressBar(widgets=pbar_widgets, maxval=tct).start() 
  ct = 0
  clin_ct = 0
  chem_ct = 0
  bio_ct = 0
  bump_ct = 0
  dark_ct = 0
  dba_err_ct = 0
  upd_ct = 0
  for target in dba.get_targets(idg=False, include_annotations=True):
    ct += 1
    pbar.update(ct)
    if 'drug_activities' in target:
      moa_acts = [a for a in target['drug_activities'] if a['has_moa'] == 1]
      if len(moa_acts) > 0:
        # clin is clin, so set tdl and continue
        clin_ct += 1
        rv = dba.upd_target(target['id'], 'tdl', 'Tclin')
        if rv:
          upd_ct += 1
        else:
          dba_err_ct += 1
        continue
      else:
        # Non-MoA drug activities qualify a target as Tchem
        # chem is chem, so set tdl and continue
        chem_ct += 1
        rv = dba.upd_target(target['id'], 'tdl', 'Tchem')
        if rv:
          upd_ct += 1
        else:
          dba_err_ct += 1
        continue
    if 'chembl_activities' in target:
      # chem is chem, so set tdl and continue
      chem_ct += 1
      rv = dba.upd_target(target['id'], 'tdl', 'Tchem')
      if rv:
        upd_ct += 1
      else:
        dba_err_ct += 1
      continue
    p = target['components']['protein'][0]
    ptdlis = p['tdl_infos']
    pms =  float(ptdlis['JensenLab PubMed Score']['value'])
    rif_ct = 0 # GeneRIF count
    if 'generifs' in p:
      rif_ct = len(p['generifs'])
    ab_ct = int(ptdlis['Ab Count']['value'])
    efl_goa = False
    if 'Experimental MF/BP Leaf Term GOA' in ptdlis:
      efl_goa = True
    omim_ct = 0 # count of confirmed OMIM phenotypes
    if 'phenotypes' in p:
      omim_ct = len([d for d in p['phenotypes'] if d['ptype'] == 'OMIM'])
    
    dark_pts = 0    
    if pms < 5:
      dark_pts += 1
    if rif_ct <= 3:
      dark_pts += 1
    if ab_ct <= 50:
      dark_pts += 1
    
    if dark_pts >= 2:
      if efl_goa or omim_ct >= 1:
        # Bump dark to bio if target is annotated with a GO MF/BP leaf term(s) with Experimental Evidence codes OR has confirmed OMIM phenotype(s)
        bump_ct += 1
        bio_ct += 1
        tdl = 'Tbio'
      else:
        dark_ct += 1
        tdl = 'Tdark'
    else:
      bio_ct += 1
      tdl = 'Tbio'
    rv = dba.upd_target(target['id'], 'tdl', tdl)
    if rv:
      upd_ct += 1
    else:
      dba_err_ct += 1
  pbar.finish()

  elapsed = time.time() - start_time
  print "%d TCRD targets processed. Elapsed time: %s" % (ct, secs2str(elapsed))
  print "Set TDL values for %d targets" % upd_ct
  print "  %d targets are Tclin" % clin_ct
  print "  %d targets are Tchem" % chem_ct
  print "  %d targets are Tbio (%d bumped from Tdark)" % (bio_ct, bump_ct)
  print "  %d targets are Tdark" % dark_ct
  if dba_err_ct > 0:
    print "WARNING: %d database errors occured. See logfile %s for details." % (dba_err_ct, logfile)
  
  print "\n%s: Done." % PROGRAM
  print


def secs2str(t):
  return "%d:%02d:%02d.%03d" % reduce(lambda ll,b : divmod(ll[0],b) + ll[1:], [(t*1000,),1000,60,60])

if __name__ == '__main__':
  main()
