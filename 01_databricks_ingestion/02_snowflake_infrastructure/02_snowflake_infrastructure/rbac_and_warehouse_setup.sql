-- 1. Create Dedicated Database and Schema
CREATE DATABASE IF NOT EXISTS ticketing_db;
CREATE SCHEMA IF NOT EXISTS ticketing_db.gold_layer;

-- 2. Create Cost-Optimized Compute Warehouse
CREATE WAREHOUSE IF NOT EXISTS compute_wh 
    WITH WAREHOUSE_SIZE = 'X-SMALL' 
    AUTO_SUSPEND = 60 
    AUTO_RESUME = TRUE;

-- 3. Enterprise Security: Role-Based Access Control (RBAC)
-- Principle of least privilege for the Databricks service account
CREATE ROLE IF NOT EXISTS ticketing_pipeline_role;

GRANT USAGE ON WAREHOUSE compute_wh TO ROLE ticketing_pipeline_role;
GRANT USAGE ON DATABASE ticketing_db TO ROLE ticketing_pipeline_role;
GRANT USAGE ON SCHEMA ticketing_db.gold_layer TO ROLE ticketing_pipeline_role;

-- Restrict service account to INSERT only
GRANT INSERT ON ALL TABLES IN SCHEMA ticketing_db.gold_layer TO ROLE ticketing_pipeline_role;
