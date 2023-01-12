CREATE TABLE `uberon_xref` (
       `uid` varchar(30) COLLATE utf8_unicode_ci NOT NULL,
       `db` varchar(24) COLLATE utf8_unicode_ci NOT NULL,
       `value` varchar(255) COLLATE utf8_unicode_ci NOT NULL,
       PRIMARY KEY (`uid`,`db`,`value`),
       CONSTRAINT `fk_uberon_xref__uberon` FOREIGN KEY (`uid`) REFERENCES `uberon` (`uid`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;