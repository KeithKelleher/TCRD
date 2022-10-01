CREATE TABLE `protein` (
   `id` int(11) NOT NULL AUTO_INCREMENT,
   `name` varchar(255) COLLATE utf8_unicode_ci NOT NULL,
   `description` text COLLATE utf8_unicode_ci NOT NULL,
   `uniprot` varchar(20) COLLATE utf8_unicode_ci NOT NULL,
   `sym` varchar(20) COLLATE utf8_unicode_ci DEFAULT NULL,
   `family` varchar(255) COLLATE utf8_unicode_ci DEFAULT NULL,
   `seq` text COLLATE utf8_unicode_ci,
   `dtoid` varchar(13) COLLATE utf8_unicode_ci DEFAULT NULL,        # comes from DTO
   `stringid` varchar(15) COLLATE utf8_unicode_ci DEFAULT NULL,     # ENSP formatted ID used by STRING-DB
   `dtoclass` varchar(255) COLLATE utf8_unicode_ci DEFAULT NULL,    # DTO class for this protein
   `preferred_symbol` varchar(20) COLLATE utf8_unicode_ci NOT NULL, # composite field - sym when it's unique, uniprot when sym is missing or degenerate
   PRIMARY KEY (`id`),
   UNIQUE KEY `protein_idx1` (`uniprot`),
   UNIQUE KEY `protein_idx2` (`name`),
   KEY `protein_sym_idx` (`sym`),
   FULLTEXT KEY `protein_text1_idx` (`name`,`description`),
   FULLTEXT KEY `protein_text2_idx` (`uniprot`,`sym`,`stringid`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;