-- Day 26 — promote the dlt-landed cricket data from Postgres into the Iceberg lakehouse.
--
-- Prereqs: the Module 3 lakehouse is up (see README.md) and Trino has both catalogs:
--   * lakehouse  (Iceberg REST -> Lakekeeper -> MinIO)
--   * postgres   (CNPG appdb, where dlt landed cricket.batting/bowling/fielding)
--
-- Run it (from the repo .venv, against the Trino LoadBalancer):
--   .venv/bin/python - <<'PY'
--   from trino.dbapi import connect
--   cur = connect(host="192.168.169.192", port=8080, user="admin").cursor()
--   for stmt in open("examples/module3-lakehouse/promote-cricket-to-iceberg.sql").read().split(";"):
--       if stmt.strip(): cur.execute(stmt); cur.fetchall()
--   PY

CREATE SCHEMA IF NOT EXISTS lakehouse.cricket;

-- CREATE TABLE ... AS SELECT: Trino reads Postgres and writes a real Iceberg table
-- (Parquet + metadata in MinIO, registered in Lakekeeper). Idempotent via DROP.
DROP TABLE IF EXISTS lakehouse.cricket.batting;
CREATE TABLE lakehouse.cricket.batting  AS SELECT * FROM postgres.cricket.batting;

DROP TABLE IF EXISTS lakehouse.cricket.bowling;
CREATE TABLE lakehouse.cricket.bowling  AS SELECT * FROM postgres.cricket.bowling;

DROP TABLE IF EXISTS lakehouse.cricket.fielding;
CREATE TABLE lakehouse.cricket.fielding AS SELECT * FROM postgres.cricket.fielding;
