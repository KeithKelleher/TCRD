CREATE TABLE `ancestry_{ontology}` (
   `oid` varchar({length}) COLLATE utf8_unicode_ci DEFAULT NULL,
   `ancestor_id` varchar({length}) COLLATE utf8_unicode_ci DEFAULT NULL,
   KEY `ancestry_{ontology}_oid_foreign` (`oid`),
   KEY `ancestry_{ontology}_ancestor_id_foreign` (`ancestor_id`),
   CONSTRAINT `ancestry_{ontology}_ancestor_id_foreign` FOREIGN KEY (`ancestor_id`) REFERENCES `{ontology}` (`{ontology_id}`),
   CONSTRAINT `ancestry_{ontology}_oid_foreign` FOREIGN KEY (`oid`) REFERENCES `{ontology}` (`{ontology_id}`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci