DROP TABLE IF EXISTS `ncats_unfiltered_counts`;
CREATE TABLE `ncats_unfiltered_counts` (
  `id` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `schema` varchar(20) NOT NULL,
  `model` varchar(20) NOT NULL,
  `filter` varchar(100) NOT NULL,
  `value` varchar(255) NOT NULL,
  `count` int(11) NOT NULL,
  `p` double NOT NULL,
  PRIMARY KEY (`id`),
  KEY `ncats_unfiltered_counts_schema_filter_value_index` (`schema`,`filter`,`value`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;