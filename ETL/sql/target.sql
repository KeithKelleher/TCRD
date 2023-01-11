CREATE TABLE IF NOT EXISTS `target` (
      `id` int(11) NOT NULL AUTO_INCREMENT,
      `name` varchar(255) COLLATE utf8_unicode_ci NOT NULL,
      `ttype` varchar(255) COLLATE utf8_unicode_ci NOT NULL,
      `description` text COLLATE utf8_unicode_ci,
      `comment` text COLLATE utf8_unicode_ci,
      `tdl` enum('Tclin+','Tclin','Tchem+','Tchem','Tbio','Tgray','Tdark') COLLATE utf8_unicode_ci DEFAULT NULL,
      `idg` tinyint(1) NOT NULL DEFAULT '0',
      `fam` enum('Enzyme','Epigenetic','GPCR','IC','Kinase','NR','oGPCR','TF','TF; Epigenetic','Transporter') COLLATE utf8_unicode_ci DEFAULT NULL,
      `famext` varchar(255) COLLATE utf8_unicode_ci DEFAULT NULL,
      PRIMARY KEY (`id`),
      KEY `target_tdl_idx` (`tdl`),
      KEY `target_fam_idx` (`fam`),
      FULLTEXT KEY `target_text_idx` (`name`,`description`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;