CREATE TABLE IF NOT EXISTS `generif` (
                           `id` int(11) NOT NULL AUTO_INCREMENT,
                           `protein_id` int(11) NOT NULL,
                           `gene_id` int(11) NOT NULL,
                           `text` text COLLATE utf8_unicode_ci NOT NULL,
                           `date` datetime DEFAULT NULL,
                           PRIMARY KEY (`id`),
                           KEY `generif_idx1` (`protein_id`),
                           FULLTEXT KEY `generif_text_idx` (`text`),
                           CONSTRAINT `fk_generif_protein` FOREIGN KEY (`protein_id`) REFERENCES `protein` (`id`) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;