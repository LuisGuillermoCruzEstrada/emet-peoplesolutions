-- sql/01_create_tables.sql

CREATE TABLE IF NOT EXISTS companies (
    company_id      TEXT PRIMARY KEY,
    company_name    TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS charges (
    charge_id       TEXT PRIMARY KEY,
    company_id      TEXT NOT NULL REFERENCES companies(company_id),
    amount          NUMERIC(14, 2) NOT NULL,
    status          TEXT NOT NULL,
    created_at      TIMESTAMP NOT NULL,
    paid_at         TIMESTAMP NULL
);

CREATE INDEX IF NOT EXISTS idx_charges_company_id ON charges(company_id);
CREATE INDEX IF NOT EXISTS idx_charges_created_at ON charges(created_at);
CREATE INDEX IF NOT EXISTS idx_charges_paid_at ON charges(paid_at);