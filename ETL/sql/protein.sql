/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE IF NOT EXISTS `protein` (
                           `id` int(11) NOT NULL AUTO_INCREMENT,
                           `name` varchar(255) COLLATE utf8_unicode_ci NOT NULL,
                           `description` text COLLATE utf8_unicode_ci NOT NULL,
                           `uniprot` varchar(20) COLLATE utf8_unicode_ci NOT NULL,
                           `up_version` int(11) DEFAULT NULL,
                           `geneid` int(11) DEFAULT NULL,
                           `sym` varchar(20) COLLATE utf8_unicode_ci DEFAULT NULL,
                           `family` varchar(255) COLLATE utf8_unicode_ci DEFAULT NULL,
                           `chr` varchar(255) COLLATE utf8_unicode_ci DEFAULT NULL,
                           `seq` text COLLATE utf8_unicode_ci,
                           `dtoid` varchar(13) COLLATE utf8_unicode_ci DEFAULT NULL,
                           `stringid` varchar(15) COLLATE utf8_unicode_ci DEFAULT NULL,
                           `dtoclass` varchar(255) COLLATE utf8_unicode_ci DEFAULT NULL,
                           PRIMARY KEY (`id`),
                           UNIQUE KEY `protein_idx1` (`uniprot`),
                           UNIQUE KEY `protein_idx2` (`name`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;
/*!40101 SET character_set_client = @saved_cs_client */;