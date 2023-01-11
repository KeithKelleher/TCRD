CREATE TABLE IF NOT EXISTS `ncats_ligands` (
         `id` int(10) unsigned NOT NULL AUTO_INCREMENT,
         `identifier` varchar(255) CHARACTER SET utf8 COLLATE utf8_unicode_ci NOT NULL COMMENT 'lychi hash if small molecule, otherwise a drug name, or other id',
         `name` text NOT NULL COMMENT 'name of drug or ligand',
         `isDrug` tinyint(1) DEFAULT NULL COMMENT 'is this ligand a drug',
         `smiles` text COMMENT 'molecular structure',
         `PubChem` varchar(255) DEFAULT NULL,
         `ChEMBL` varchar(255) DEFAULT NULL,
         `Guide to Pharmacology` varchar(255) DEFAULT NULL,
         `DrugCentral` varchar(255) DEFAULT NULL,
         `description` text COMMENT 'description of the drug from nlm_drug_info',
         `actCnt` int(11) DEFAULT NULL COMMENT 'activity count',
         `targetCount` int(11) DEFAULT NULL,
         `unii` varchar(10) DEFAULT NULL,
         `pt` varchar(128) DEFAULT NULL,
         PRIMARY KEY (`id`),
         KEY `ncats_ligands_identifier_index` (`identifier`),
         FULLTEXT KEY `text_search` (`name`,`ChEMBL`,`PubChem`,`Guide to Pharmacology`,`DrugCentral`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;