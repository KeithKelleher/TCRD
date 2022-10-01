CREATE TABLE `go` (
                      `id` varchar(255) COLLATE utf8_unicode_ci NOT NULL,
                      `type` enum('Component','Function','Process') COLLATE utf8_unicode_ci NOT NULL,
                      `term` text COLLATE utf8_unicode_ci NOT NULL,
                      PRIMARY KEY (`id`),
                      FULLTEXT KEY `goa_text_idx` (`term`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;

CREATE TABLE `goa` (
                       `id` int(11) NOT NULL AUTO_INCREMENT,
                       `protein_id` int(11) NOT NULL,
                       `go_id` varchar(255) COLLATE utf8_unicode_ci NOT NULL,
                       `goeco` varchar(255) COLLATE utf8_unicode_ci NOT NULL,
                       `assigned_by` varchar(50) COLLATE utf8_unicode_ci DEFAULT NULL,
                       PRIMARY KEY (`id`),
                       KEY `goa_idx1` (`protein_id`),
                       KEY `goa_idx2` (`go_id`),
                       CONSTRAINT `fk_goa_protein` FOREIGN KEY (`protein_id`) REFERENCES `protein` (`id`) ON DELETE CASCADE,
                       CONSTRAINT `fk_goa_go` FOREIGN KEY (`go_id`) REFERENCES `go` (`id`) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;
