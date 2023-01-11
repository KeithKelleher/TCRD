CREATE TABLE IF NOT EXISTS `ncats_dataSource_map` (
        `id` int(10) unsigned NOT NULL AUTO_INCREMENT,
        `dataSource` varchar(50) COLLATE utf8_unicode_ci NOT NULL,
        `old_url` varchar(128) COLLATE utf8_unicode_ci DEFAULT NULL,
        `old_license` varchar(128) COLLATE utf8_unicode_ci DEFAULT NULL,
        `old_licenseURL` varchar(128) COLLATE utf8_unicode_ci DEFAULT NULL,
        `protein_id` int(11) DEFAULT NULL,
        `ncats_ligand_id` int(10) unsigned DEFAULT NULL,
        `disease_name` text COLLATE utf8_unicode_ci,
        PRIMARY KEY (`id`),
        KEY `dataSource_dataSource` (`dataSource`),
        KEY `dataSource_protein` (`protein_id`),
        KEY `dataSource_ligand` (`ncats_ligand_id`),
        KEY `dataSource_disease` (`disease_name`(200)),
        CONSTRAINT `dataSource_ligand` FOREIGN KEY (`ncats_ligand_id`) REFERENCES `ncats_ligands` (`id`),
        CONSTRAINT `dataSource_protein` FOREIGN KEY (`protein_id`) REFERENCES `protein` (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;