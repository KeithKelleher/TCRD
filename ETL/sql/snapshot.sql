SELECT CONCAT('RENAME TABLE $1.', table_name, ' TO $2.', table_name, '; ')
FROM information_schema.TABLES
WHERE table_schema='$1';