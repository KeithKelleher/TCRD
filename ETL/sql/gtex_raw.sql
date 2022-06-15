CREATE TABLE IF NOT EXISTS `gtex_raw` (
   `id` INT(11) AUTO_INCREMENT NOT NULL,
   `sample_id` VARCHAR(45) NOT NULL,
   `protein_id` VARCHAR(11) NOT NULL,
   `value` VARCHAR(45) NULL,
   PRIMARY KEY (`id`),
   KEY `fk_gtex_sample_idx` (`sample_id`),
   CONSTRAINT `fk_gtex_sample` FOREIGN KEY (`sample_id`) REFERENCES `gtex_sample` (`SAMPID`) ON DELETE NO ACTION ON UPDATE NO ACTION
);