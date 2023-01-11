DROP TABLE IF EXISTS `input_version`;
CREATE TABLE `input_version` (
     `source_key` varchar(45) NOT NULL,
     `data_source` varchar(45) NOT NULL,
     `file_key` varchar(45) NOT NULL,
     `file` varchar(256) NOT NULL,
     `version` varchar(45),
     `release_date` date,
     `download_date` date NOT NULL,
     PRIMARY KEY (`source_key`, `file_key`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;