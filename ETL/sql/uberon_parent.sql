CREATE TABLE `uberon_parent` (
     `uid` varchar(30) COLLATE utf8_unicode_ci NOT NULL,
     `parent_id` varchar(30) COLLATE utf8_unicode_ci NOT NULL,
     KEY `fk_uberon_parent__uberon` (`uid`),
     CONSTRAINT `fk_uberon_parent__uberon` FOREIGN KEY (`uid`) REFERENCES `uberon` (`uid`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;