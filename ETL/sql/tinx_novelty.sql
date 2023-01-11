CREATE TABLE `tinx_novelty` (
    `id` int NOT NULL AUTO_INCREMENT,
    `protein_id` int,
    `source_id` varchar(20) NOT NULL,
    `score` decimal(34,16) NOT NULL,
    PRIMARY KEY (`id`),
    KEY `tinx_novelty_idx1` (`protein_id`),
    KEY `tinx_novelty_idx3` (`protein_id`,`score`),
    CONSTRAINT `fk_tinx_novelty__protein` FOREIGN KEY (`protein_id`) REFERENCES `protein` (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3 COLLATE=utf8_unicode_ci;