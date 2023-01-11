CREATE TABLE IF NOT EXISTS `protein2pubmed` (
      `protein_id` int(11) NOT NULL,
      `pubmed_id` int(11) NOT NULL,
      `gene_id` int(11),
      `source` varchar(45) COLLATE utf8_unicode_ci NOT NULL,
      KEY `protein2pubmed_idx1` (`protein_id`),
      KEY `protein2pubmed_idx2` (`pubmed_id`),
      KEY `protein2pubmed_type` (`source`),
      KEY `protein2pubmed_type_protein_id` (`source`, `protein_id`),
      CONSTRAINT `fk_protein2pubmed__protein` FOREIGN KEY (`protein_id`) REFERENCES `protein` (`id`) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;