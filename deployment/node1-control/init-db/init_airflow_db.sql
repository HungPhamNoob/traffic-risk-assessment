-- Tạo database cho Airflow metadata
CREATE DATABASE airflow;

-- Tạo extension PostGIS cho capstone_db (nếu chưa có)
\c capstone_db
CREATE EXTENSION IF NOT EXISTS postgis;
CREATE EXTENSION IF NOT EXISTS postgis_topology;