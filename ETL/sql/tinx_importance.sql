CREATE TABLE `tinx_importance` (
       `doid` varchar(20) CHARACTER SET utf8 COLLATE utf8_unicode_ci NOT NULL,
       `protein_id` int,
       `source_id` varchar(20) NOT NULL,
       `score` decimal(34,16) NOT NULL,
       PRIMARY KEY (`doid`,`source_id`),
       KEY `tinx_importance_idx1` (`protein_id`),
       KEY `tinx_importance_idx2` (`doid`),
       CONSTRAINT `fk_tinx_importance__protein` FOREIGN KEY (`protein_id`) REFERENCES `protein` (`id`),
       CONSTRAINT `fk_tinx_importance__tinx_disease` FOREIGN KEY (`doid`) REFERENCES `tinx_disease` (`doid`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3 COLLATE=utf8_unicode_ci;