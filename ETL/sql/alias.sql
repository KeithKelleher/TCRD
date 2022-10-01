CREATE TABLE `alias` (
     `id` int(11) NOT NULL AUTO_INCREMENT,
     `protein_id` int(11) NOT NULL,
     `type` varchar(255) COLLATE utf8_unicode_ci NOT NULL,
     `value` varchar(255) COLLATE utf8_unicode_ci NOT NULL,
     PRIMARY KEY (`id`),
     KEY `alias_idx1` (`protein_id`),
     FULLTEXT KEY `alias_text_idx` (`value`),
     CONSTRAINT `fk_alias_protein` FOREIGN KEY (`protein_id`) REFERENCES `protein` (`id`) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;