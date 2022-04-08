/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE IF NOT EXISTS `alias` (
                         `id` int(11) NOT NULL AUTO_INCREMENT,
                         `protein_id` int(11) NOT NULL,
                         `type` enum('symbol','uniprot') COLLATE utf8_unicode_ci NOT NULL,
                         `value` varchar(255) COLLATE utf8_unicode_ci NOT NULL,
                         `dataset_id` int(11) NOT NULL,
                         PRIMARY KEY (`id`),
                         KEY `alias_idx1` (`protein_id`),
                         KEY `alias_idx2` (`dataset_id`),
                         CONSTRAINT `fk_alias_dataset` FOREIGN KEY (`dataset_id`) REFERENCES `dataset` (`id`),
                         CONSTRAINT `fk_alias_protein` FOREIGN KEY (`protein_id`) REFERENCES `protein` (`id`) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;
/*!40101 SET character_set_client = @saved_cs_client */;