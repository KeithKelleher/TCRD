CREATE TABLE `uberon_ancestry` (
    `uberon_id` varchar(20) COLLATE utf8_unicode_ci NOT NULL,
    `ancestor_uberon_id` varchar(20) COLLATE utf8_unicode_ci NOT NULL,
    KEY `uberon_ancestry_uberon_id_foreign` (`uberon_id`),
    KEY `uberon_ancestry_ancestor_uberon_id_foreign` (`ancestor_uberon_id`),
    CONSTRAINT `uberon_ancestry_ancestor_uberon_id_foreign` FOREIGN KEY (`ancestor_uberon_id`) REFERENCES `uberon` (`uid`),
    CONSTRAINT `uberon_ancestry_uberon_id_foreign` FOREIGN KEY (`uberon_id`) REFERENCES `uberon` (`uid`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci