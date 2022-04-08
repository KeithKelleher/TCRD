/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE IF NOT EXISTS `dataset` (
                           `id` int(11) NOT NULL AUTO_INCREMENT,
                           `name` varchar(255) COLLATE utf8_unicode_ci NOT NULL,
                           `source` text COLLATE utf8_unicode_ci NOT NULL,
                           `app` varchar(255) COLLATE utf8_unicode_ci DEFAULT NULL,
                           `app_version` varchar(255) COLLATE utf8_unicode_ci DEFAULT NULL,
                           `datetime` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
                           `url` text COLLATE utf8_unicode_ci,
                           `comments` text COLLATE utf8_unicode_ci,
                           PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;
/*!40101 SET character_set_client = @saved_cs_client */;