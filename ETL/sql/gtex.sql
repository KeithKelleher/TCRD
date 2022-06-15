CREATE TABLE `gtex` (
        `id` int(11) NOT NULL AUTO_INCREMENT,
        `protein_id` int(11) DEFAULT NULL,
        `tissue` text COLLATE utf8_unicode_ci NOT NULL,
        `tpm` decimal(12,6) NOT NULL,
        `tpm_rank` decimal(4,3) DEFAULT NULL,
        `tpm_male` decimal(12,6) NULL,
        `tpm_male_rank` decimal(4,3) DEFAULT NULL,
        `tpm_female` decimal(12,6) NULL,
        `tpm_female_rank` decimal(4,3) DEFAULT NULL,
        `uberon_id` varchar(20) COLLATE utf8_unicode_ci DEFAULT NULL,
        PRIMARY KEY (`id`),
        KEY `expression_idx1` (`protein_id`),
        KEY `fk_gtex_uberon` (`uberon_id`),
        CONSTRAINT `fk_gtex_protein` FOREIGN KEY (`protein_id`) REFERENCES `protein` (`id`) ON DELETE CASCADE,
        CONSTRAINT `fk_gtex_uberon` FOREIGN KEY (`uberon_id`) REFERENCES `uberon` (`uid`)
)