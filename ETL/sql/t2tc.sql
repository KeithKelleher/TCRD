/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE IF NOT EXISTS `t2tc` (
                        `target_id` int(11) NOT NULL,
                        `protein_id` int(11) DEFAULT NULL,
                        `nucleic_acid_id` int(11) DEFAULT NULL,
                        KEY `t2tc_idx1` (`target_id`),
                        KEY `t2tc_idx2` (`protein_id`),
                        CONSTRAINT `fk_t2tc__protein` FOREIGN KEY (`protein_id`) REFERENCES `protein` (`id`),
                        CONSTRAINT `fk_t2tc__target` FOREIGN KEY (`target_id`) REFERENCES `target` (`id`) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;
/*!40101 SET character_set_client = @saved_cs_client */;