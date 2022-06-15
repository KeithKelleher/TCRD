DROP TABLE IF EXISTS `input_version`;
CREATE TABLE `input_version` (
     `id` int(11) NOT NULL AUTO_INCREMENT,
     `source_key` varchar(45) NOT NULL,
     `data_source` varchar(45) NOT NULL,
     `file_key` varchar(45) NOT NULL,
     `file` varchar(256) NOT NULL,
     `version` varchar(45) NOT NULL,
     `release_date` date,
     `download_date` date NOT NULL,
     PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;