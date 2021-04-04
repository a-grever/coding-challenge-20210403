

CREATE DATABASE warehouse;
\connect warehouse;
CREATE SCHEMA raw;
CREATE SCHEMA crm;
CREATE SCHEMA reports;

-- GRANT USAGE ON SCHEMA raw to etl;
-- GRANT USAGE ON SCHEMA crm to etl;
-- GRANT SELECT, UPDATE, INSERT, DELETE, TRUNCATE ON ALL TABLES IN SCHEMA raw TO etl;
-- GRANT SELECT, UPDATE, INSERT, DELETE, TRUNCATE ON ALL TABLES IN SCHEMA crm TO etl;
-- ALTER DEFAULT PRIVILEGES IN SCHEMA raw GRANT SELECT, UPDATE, INSERT, DELETE, TRUNCATE ON TABLES TO etl;
-- ALTER DEFAULT PRIVILEGES IN SCHEMA crm GRANT SELECT, UPDATE, INSERT, DELETE, TRUNCATE ON TABLES TO etl;
-- GRANT USAGE ON ALL SEQUENCES IN SCHEMA raw TO etl;
-- GRANT USAGE ON ALL SEQUENCES IN SCHEMA crm TO etl;
-- ALTER DEFAULT PRIVILEGES IN SCHEMA raw GRANT USAGE ON SEQUENCES TO etl;
-- ALTER DEFAULT PRIVILEGES IN SCHEMA crm GRANT USAGE ON SEQUENCES TO etl;


CREATE TABLE raw.user_events (
    event_sk SERIAL PRIMARY KEY,
    id TEXT NOT NULL,
    event_type TEXT,
    username TEXT,
    user_email TEXT,
    user_type TEXT,
    organization_name TEXT,
    plan_name TEXT,
    received_at TIMESTAMPTZ
);


CREATE TABLE crm.users_dim (
    event_sk BIGINT PRIMARY KEY,
    id TEXT NOT NULL,
    username TEXT NOT NULL,
    user_email TEXT NOT NULL,
    user_type TEXT NOT NULL,
    organization_name TEXT,
    plan_name TEXT,
    valid_from TIMESTAMPTZ NOT NULL,
    valid_to TIMESTAMPTZ,
    is_valid BOOLEAN  NOT NULL,
    is_deleted BOOLEAN  NOT NULL
);

CREATE TABLE crm.organizations_dim (
    organization_key TEXT PRIMARY KEY,
    organization_name TEXT,
    created_at TIMESTAMPTZ
);

CREATE TABLE reports.user_events_daily (
    event_date DATE PRIMARY KEY,
    n_created INTEGER,
    n_updated INTEGER,
    n_deleted INTEGER,
    n_unique INTEGER,
    n_total INTEGER
);
