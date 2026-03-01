-- sql/03_views.sql
-- Punto 1.5: vistas de monto total por día y compañía

-- 1) Vista general: suma todos los cargos (sin filtrar status)
CREATE OR REPLACE VIEW vw_daily_total_by_company AS
SELECT
    DATE(ch.created_at) AS day,
    ch.company_id,
    co.company_name,
    SUM(ch.amount) AS total_amount,
    COUNT(*) AS charges_count
FROM charges ch
JOIN companies co
  ON co.company_id = ch.company_id
GROUP BY
    DATE(ch.created_at),
    ch.company_id,
    co.company_name
ORDER BY
    day ASC,
    company_name ASC;

-- 2) Vista solo pagados: suma solo cargos con status = 'paid'
CREATE OR REPLACE VIEW vw_daily_paid_total_by_company AS
SELECT
    DATE(ch.created_at) AS day,
    ch.company_id,
    co.company_name,
    SUM(ch.amount) AS total_amount,
    COUNT(*) AS charges_count
FROM charges ch
JOIN companies co
  ON co.company_id = ch.company_id
WHERE ch.status = 'paid'
GROUP BY
    DATE(ch.created_at),
    ch.company_id,
    co.company_name
ORDER BY
    day ASC,
    company_name ASC;
    