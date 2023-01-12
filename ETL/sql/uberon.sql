CREATE TABLE `uberon` (
      `uid` varchar(30) COLLATE utf8_unicode_ci NOT NULL,
      `name` text COLLATE utf8_unicode_ci NOT NULL,
      `def` text COLLATE utf8_unicode_ci,
      `comment` text COLLATE utf8_unicode_ci,
      PRIMARY KEY (`uid`),
      FULLTEXT KEY `uberon_text_idx` (`name`,`def`,`comment`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;