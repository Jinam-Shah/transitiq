-- Runs once, the first time the Postgres volume is created.
-- The main "transitiq" database is created by POSTGRES_DB; this adds Airflow's metadata DB.
CREATE DATABASE airflow;

-- Schemas for the app database. Gold tables are published here locally
-- (and to Neon in production) starting in Phase 5.
\connect transitiq
CREATE SCHEMA IF NOT EXISTS gold;
CREATE SCHEMA IF NOT EXISTS app;
