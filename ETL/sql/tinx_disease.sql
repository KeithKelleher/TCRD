CREATE TABLE `tinx_disease` (
    `doid` varchar(20) CHARACTER SET utf8 COLLATE utf8_unicode_ci NOT NULL,
    `name` text CHARACTER SET utf8 COLLATE utf8_unicode_ci NOT NULL,
    `summary` text CHARACTER SET utf8 COLLATE utf8_unicode_ci,
    `score` decimal(34,16) DEFAULT NULL,
    PRIMARY KEY (`doid`),
    FULLTEXT KEY `tinx_disease_text_idx` (`name`,`summary`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3 COLLATE=utf8_unicode_ci;