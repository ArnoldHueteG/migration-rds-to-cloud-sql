--https://cloud.google.com/database-migration/docs/postgres/configure-source-database

-- for each database;
GRANT USAGE on SCHEMA pglogical to postgres;
GRANT USAGE on SCHEMA public to postgres;

-- for each database;
GRANT SELECT on ALL TABLES in SCHEMA pglogical to postgres;
GRANT SELECT on ALL TABLES in SCHEMA public to postgres;

-- for each database
GRANT SELECT on ALL SEQUENCES in SCHEMA pglogical to postgres;
GRANT SELECT on ALL SEQUENCES in SCHEMA public to postgres;


GRANT rds_replication to postgres;