CREATE TABLE IF NOT EXISTS `ncats_dataSource` (
        `dataSource` varchar(50) COLLATE utf8_unicode_ci NOT NULL,
        `dataSourceDescription` varchar(512) COLLATE utf8_unicode_ci DEFAULT NULL,
        `url` varchar(128) COLLATE utf8_unicode_ci DEFAULT NULL,
        `license` varchar(128) COLLATE utf8_unicode_ci DEFAULT NULL,
        `licenseURL` varchar(128) COLLATE utf8_unicode_ci DEFAULT NULL,
        `citation` varchar(512) COLLATE utf8_unicode_ci DEFAULT NULL,
        PRIMARY KEY (`dataSource`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;