CREATE TABLE `xref` (
                        `id` int(11) NOT NULL AUTO_INCREMENT,
                        `xtype` varchar(255) COLLATE utf8_unicode_ci NOT NULL,
                        `subtype` varchar(255) COLLATE utf8_unicode_ci NOT NULL,
                        `protein_id` int(11) DEFAULT NULL,
                        `value` varchar(255) COLLATE utf8_unicode_ci NOT NULL,
                        `xtra` varchar(255) COLLATE utf8_unicode_ci DEFAULT NULL,
                        PRIMARY KEY (`id`),
                        UNIQUE KEY `xref_idx5` (`xtype`,`protein_id`,`value`),
                        KEY `xref_idx1` (`xtype`),
                        KEY `xref_idx4` (`protein_id`),
                        KEY `xref_idx7` (`xtra`),
                        KEY `xref_type_val_idx` (`xtype`,`value`),
                        FULLTEXT KEY `xref_text_idx` (`value`,`xtra`),
                        CONSTRAINT `fk_xref_protein` FOREIGN KEY (`protein_id`) REFERENCES `protein` (`id`) ON DELETE CASCADE
) ENGINE=InnoDB AUTO_INCREMENT=2696227 DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;