"""Gold layer operations: data quality checks and publishing gold tables to PostgreSQL.

transitiq-gold quality                 # checks over silver + gold (DuckDB)
transitiq-gold publish --target auto   # local Postgres, plus Neon when NEON_DATABASE_URL is set
"""
