CREATE TABLE `expression_temp` (
      `id` int(11) NOT NULL AUTO_INCREMENT,
      `etype` varchar(255) COLLATE utf8_unicode_ci NOT NULL,
      `protein_id` int(11) DEFAULT NULL,
      `source_id` varchar(20) NOT NULL,
      `tissue` varchar(255) COLLATE utf8_unicode_ci NOT NULL,
      `qual_value` enum('Not detected','Low','Medium','High') COLLATE utf8_unicode_ci DEFAULT NULL,
      `number_value` decimal(12,6) DEFAULT NULL,
      `expressed` TINYINT NOT NULL,
      `source_rank` decimal(12,6) DEFAULT NULL,
      `evidence` varchar(255) COLLATE utf8_unicode_ci DEFAULT NULL,
      `oid` varchar(20) COLLATE utf8_unicode_ci DEFAULT NULL,
      `uberon_id` varchar(20) COLLATE utf8_unicode_ci DEFAULT NULL,
      PRIMARY KEY (`id`)
       ) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;

CREATE TABLE `expression` (
      `id` int(11) NOT NULL AUTO_INCREMENT,
      `etype` varchar(255) COLLATE utf8_unicode_ci NOT NULL,
      `protein_id` int(11) DEFAULT NULL,
      `source_id` varchar(20) NOT NULL,
      `tissue` varchar(255) COLLATE utf8_unicode_ci NOT NULL,
      `tissue_id` int(11) NOT NULL,
      `qual_value` enum('Not detected','Low','Medium','High') COLLATE utf8_unicode_ci DEFAULT NULL,
      `number_value` decimal(12,6) DEFAULT NULL,
      `expressed` TINYINT NOT NULL,
      `source_rank` decimal(12,6) DEFAULT NULL,
      `evidence` varchar(255) COLLATE utf8_unicode_ci DEFAULT NULL,
      `oid` varchar(20) COLLATE utf8_unicode_ci DEFAULT NULL,
      `uberon_id` varchar(20) COLLATE utf8_unicode_ci DEFAULT NULL,
      PRIMARY KEY (`id`),
      KEY `expression_idx1` (`etype`),
      KEY `expression_idx3` (`protein_id`),
      KEY `expression_idx4` (`tissue_id`),
      KEY `expression_facet_idx` (`expressed`,`tissue_id`,`protein_id`),
      CONSTRAINT `fk_expression_tissue` FOREIGN KEY (`tissue_id`) REFERENCES `tissue` (`id`) ON DELETE CASCADE,
      CONSTRAINT `fk_expression_protein` FOREIGN KEY (`protein_id`) REFERENCES `protein` (`id`) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;

