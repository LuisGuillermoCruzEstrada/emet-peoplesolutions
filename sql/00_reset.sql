-- sql/00_reset.sql
-- Resetea TODO lo que haya en public (tablas, vistas, etc.)
DROP SCHEMA IF EXISTS public CASCADE;
CREATE SCHEMA public;

-- Opcional: permisos típicos
GRANT ALL ON SCHEMA public TO pt_user;