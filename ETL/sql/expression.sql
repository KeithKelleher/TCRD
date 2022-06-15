CREATE TABLE `expression` (
      `id` int(11) NOT NULL AUTO_INCREMENT,
      `etype` varchar(255) COLLATE utf8_unicode_ci NOT NULL,
      `protein_id` int(11) DEFAULT NULL,
      `tissue` text COLLATE utf8_unicode_ci NOT NULL,
      `qual_value` enum('Not detected','Low','Medium','High') COLLATE utf8_unicode_ci DEFAULT NULL,
      `number_value` decimal(12,6) DEFAULT NULL,
      `source_rank` decimal(12,6) DEFAULT NULL,
      `evidence` varchar(255) COLLATE utf8_unicode_ci DEFAULT NULL,
      `source_ontology_id` varchar(20) COLLATE utf8_unicode_ci DEFAULT NULL,
      `uberon_id` varchar(20) COLLATE utf8_unicode_ci DEFAULT NULL,
      PRIMARY KEY (`id`),
      KEY `expression_idx1` (`etype`),
      KEY `expression_idx3` (`protein_id`),
      KEY `fk_expression_uberon` (`uberon_id`),
      KEY `expression_facet_idx` (`protein_id`,`etype`,`tissue`(256)),
      KEY `expression_idx7` (`etype`,`tissue`(256)),
      KEY `expression_idx8` (`protein_id`),
      FULLTEXT KEY `expression_text_idx` (`tissue`),
      CONSTRAINT `fk_expression__protein` FOREIGN KEY (`protein_id`) REFERENCES `protein` (`id`) ON DELETE CASCADE
#       ,CONSTRAINT `fk_expression_uberon` FOREIGN KEY (`uberon_id`) REFERENCES `uberon` (`uid`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;