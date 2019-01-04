CREATE DATABASE invoices;

CREATE USER 'mysqluser'@'%' IDENTIFIED BY 'secret';
GRANT ALL PRIVILEGES ON invoices.* TO 'mysqluser'@'%';

USE invoices;

CREATE TABLE `invoices` (
  `id` VARCHAR(64) NOT NULL,
  `version` INT UNSIGNED DEFAULT 0,
  `created_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  `updated_at` VARCHAR(64) DEFAULT NULL,
  `customer_name` VARCHAR(64) DEFAULT NULL,
  `customer_email` VARCHAR(64) DEFAULT NULL,
  `issue_date` VARCHAR(64) DEFAULT NULL,
  `due_date` VARCHAR(64) DEFAULT NULL,
  `total` double DEFAULT NULL,
  `status` VARCHAR(64) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8


