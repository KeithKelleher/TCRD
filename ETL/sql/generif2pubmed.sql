CREATE TABLE IF NOT EXISTS `generif2pubmed` (
                                  `generif_id` int(11) NOT NULL,
                                  `pubmed_id` int(11) NOT NULL,
                                  KEY `generif2pubmed_idx1` (`generif_id`),
                                  KEY `generif2pubmed_idx2` (`pubmed_id`),
                                  CONSTRAINT `fk_generif2pubmed__generif` FOREIGN KEY (`generif_id`) REFERENCES `generif` (`id`) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;