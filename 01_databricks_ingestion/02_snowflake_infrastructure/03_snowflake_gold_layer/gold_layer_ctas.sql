USE DATABASE ticketing_db;
USE SCHEMA gold_layer;
USE WAREHOUSE compute_wh;

-- 1. Analytics Engineering: Materializing the Gold Data Mart
CREATE OR REPLACE TABLE gold_ticketing_insights AS
SELECT 
    server_node,
    anomaly_score, 
    
    -- Traffic Volume
    COUNT(*) as total_hits,
    COUNT(DISTINCT ip_address) as unique_users,
    
    -- Performance Metrics
    ROUND(AVG(CASE WHEN http_status_code = 200 THEN 1 ELSE 0 END) * 100, 2) as success_rate_pct,
    COUNT(CASE WHEN http_status_code = 429 THEN 1 END) as rate_limit_triggers,
    COUNT(CASE WHEN http_status_code >= 500 THEN 1 END) as server_errors,
    
    -- Demand Analysis
    COUNT(CASE WHEN user_id = 'GUEST' THEN 1 END) as guest_traffic_count,
    ROUND((COUNT(CASE WHEN user_id = 'GUEST' THEN 1 END) / COUNT(*)) * 100, 2) as pct_unauthenticated

FROM ticket_sales_fact
GROUP BY 
    server_node, 
    anomaly_score
ORDER BY total_hits DESC;

-- ==========================================
-- BUSINESS INTELLIGENCE & SECURITY QUERIES
-- ==========================================

-- Query 1: Executive Overview (Bot vs Normal Volume)
SELECT 
    anomaly_score, 
    COUNT(*) as volume
FROM ticket_sales_fact
GROUP BY anomaly_score
ORDER BY volume DESC;

-- Query 2: Tactical Security Drill-Down (Actionable Threat Intel)
SELECT 
    ip_address, 
    http_status_code, 
    anomaly_score
FROM ticket_sales_fact 
WHERE anomaly_score = 'SUSPECTED_BOT'
LIMIT 10;
