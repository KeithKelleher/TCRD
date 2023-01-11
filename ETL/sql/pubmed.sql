CREATE TABLE IF NOT EXISTS `pubmed` (
      `id` int(11) NOT NULL,
      `title` text COLLATE utf8_unicode_ci NOT NULL,
      `journal` text COLLATE utf8_unicode_ci,
      `date` varchar(10) COLLATE utf8_unicode_ci DEFAULT NULL,
      `authors` text COLLATE utf8_unicode_ci,
      `abstract` text COLLATE utf8_unicode_ci,
      `fetch_date` datetime,
      PRIMARY KEY (`id`),
      KEY `pubmed_sort_idx` (`date`,`id`)
#      FULLTEXT KEY `pubmed_text_idx` (`title`,`abstract`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;