CREATE TABLE IF NOT EXISTS `tcrdinfinity`.`gtex_subject`
(
       `subject_id` VARCHAR(12) NOT NULL,
       `sex` ENUM('male', 'female') NOT NULL,
       `age` VARCHAR(5) NOT NULL,
       `death_hardy` TINYINT(1) NULL,
       PRIMARY KEY (`subject_id`),
       UNIQUE INDEX `subject_id_UNIQUE` (`subject_id` ASC)
);
