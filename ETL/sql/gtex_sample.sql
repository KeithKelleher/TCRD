CREATE TABLE IF NOT EXISTS `gtex_sample` (
  `SAMPID` varchar(45) NOT NULL,
  `SMATSSCR` varchar(45) DEFAULT NULL,
  `SMCENTER` varchar(45) DEFAULT NULL,
  `SMPTHNTS` varchar(512) DEFAULT NULL,
  `SMRIN` varchar(45) DEFAULT NULL,
  `SMTS` varchar(45) DEFAULT NULL,
  `SMTSD` varchar(45) DEFAULT NULL,
  `SMUBRID` varchar(45) DEFAULT NULL,
  `SMTSISCH` varchar(45) DEFAULT NULL,
  `SMTSPAX` varchar(45) DEFAULT NULL,
  `SMNABTCH` varchar(45) DEFAULT NULL,
  `SMNABTCHT` varchar(128) DEFAULT NULL,
  `SMNABTCHD` varchar(45) DEFAULT NULL,
  `SMGEBTCH` varchar(45) DEFAULT NULL,
  `SMGEBTCHD` varchar(45) DEFAULT NULL,
  `SMGEBTCHT` varchar(45) DEFAULT NULL,
  `SMAFRZE` varchar(45) DEFAULT NULL,
  `SMGTC` varchar(45) DEFAULT NULL,
  `SME2MPRT` varchar(45) DEFAULT NULL,
  `SMCHMPRS` varchar(45) DEFAULT NULL,
  `SMNTRART` varchar(45) DEFAULT NULL,
  `SMNUMGPS` varchar(45) DEFAULT NULL,
  `SMMAPRT` varchar(45) DEFAULT NULL,
  `SMEXNCRT` varchar(45) DEFAULT NULL,
  `SM550NRM` varchar(45) DEFAULT NULL,
  `SMGNSDTC` varchar(45) DEFAULT NULL,
  `SMUNMPRT` varchar(45) DEFAULT NULL,
  `SM350NRM` varchar(45) DEFAULT NULL,
  `SMRDLGTH` varchar(45) DEFAULT NULL,
  `SMMNCPB` varchar(45) DEFAULT NULL,
  `SME1MMRT` varchar(45) DEFAULT NULL,
  `SMSFLGTH` varchar(45) DEFAULT NULL,
  `SMESTLBS` varchar(45) DEFAULT NULL,
  `SMMPPD` varchar(45) DEFAULT NULL,
  `SMNTERRT` varchar(45) DEFAULT NULL,
  `SMRRNANM` varchar(45) DEFAULT NULL,
  `SMRDTTL` varchar(45) DEFAULT NULL,
  `SMVQCFL` varchar(45) DEFAULT NULL,
  `SMMNCV` varchar(45) DEFAULT NULL,
  `SMTRSCPT` varchar(45) DEFAULT NULL,
  `SMMPPDPR` varchar(45) DEFAULT NULL,
  `SMCGLGTH` varchar(45) DEFAULT NULL,
  `SMGAPPCT` varchar(45) DEFAULT NULL,
  `SMUNPDRD` varchar(45) DEFAULT NULL,
  `SMNTRNRT` varchar(45) DEFAULT NULL,
  `SMMPUNRT` varchar(45) DEFAULT NULL,
  `SMEXPEFF` varchar(45) DEFAULT NULL,
  `SMMPPDUN` varchar(45) DEFAULT NULL,
  `SME2MMRT` varchar(45) DEFAULT NULL,
  `SME2ANTI` varchar(45) DEFAULT NULL,
  `SMALTALG` varchar(45) DEFAULT NULL,
  `SME2SNSE` varchar(45) DEFAULT NULL,
  `SMMFLGTH` varchar(45) DEFAULT NULL,
  `SME1ANTI` varchar(45) DEFAULT NULL,
  `SMSPLTRD` varchar(45) DEFAULT NULL,
  `SMBSMMRT` varchar(45) DEFAULT NULL,
  `SME1SNSE` varchar(45) DEFAULT NULL,
  `SME1PCTS` varchar(45) DEFAULT NULL,
  `SMRRNART` varchar(45) DEFAULT NULL,
  `SME1MPRT` varchar(45) DEFAULT NULL,
  `SMNUM5CD` varchar(45) DEFAULT NULL,
  `SMDPMPRT` varchar(45) DEFAULT NULL,
  `SME2PCTS` varchar(45) DEFAULT NULL,
  PRIMARY KEY (`SAMPID`)
) ENGINE=InnoDB;