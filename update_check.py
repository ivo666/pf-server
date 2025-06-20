psql -h localhost -U postgres -d locserver -c "
SELECT 
    'table_clients' as table_name, 
    MAX(date) as max_date, 
    COUNT(*) as total_rows 
FROM cdm.table_clients
UNION ALL
SELECT 
    'table_page_views', 
    MAX(hit_ts::date), 
    COUNT(*) 
FROM cdm.table_page_views
UNION ALL
SELECT 
    'table_visits', 
    MAX(visit_date), 
    COUNT(*) 
FROM cdm.table_visits;
"
