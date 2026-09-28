-- =====================================================================
-- create_database.sql
-- Creates the MySQL database used by the Banking Transaction Data
-- Pipeline & Risk Analytics project.
--
-- Usage:
--   mysql -u <user> -p < sql/create_database.sql
-- =====================================================================

CREATE DATABASE IF NOT EXISTS banking_analytics
    CHARACTER SET utf8mb4
    COLLATE utf8mb4_unicode_ci;

USE banking_analytics;
