-- PostgreSQL 18 statistics used by P3 before/after benchmark snapshots.

SELECT now() AS sampled_at,
       wal_records,
       wal_fpi,
       wal_bytes,
       wal_buffers_full,
       stats_reset
FROM pg_stat_wal;

SELECT backend_type,
       object,
       context,
       reads,
       read_bytes,
       read_time,
       writes,
       write_bytes,
       write_time,
       extends,
       extend_bytes,
       extend_time,
       hits,
       evictions,
       fsyncs,
       fsync_time
FROM pg_stat_io
WHERE object IN ('relation', 'wal')
ORDER BY object, backend_type, context;

SELECT relid::regclass AS relation,
       seq_scan,
       idx_scan,
       n_tup_ins,
       n_tup_upd,
       n_tup_del,
       n_live_tup,
       n_dead_tup,
       vacuum_count,
       autovacuum_count,
       last_vacuum,
       last_autovacuum
FROM pg_stat_user_tables
WHERE schemaname = 'vidpg'
ORDER BY relname;

SELECT relid::regclass AS relation,
       n_live_tup,
       n_dead_tup,
       n_tup_ins,
       n_tup_upd,
       n_tup_del,
       vacuum_count,
       autovacuum_count,
       last_vacuum,
       last_autovacuum
FROM pg_stat_all_tables
WHERE schemaname = 'vidpg'
ORDER BY relname;

SELECT c.oid::regclass AS relation,
       pg_relation_size(c.oid) AS heap_bytes,
       pg_indexes_size(c.oid) AS index_bytes,
       CASE WHEN c.reltoastrelid = 0 THEN 0
            ELSE pg_total_relation_size(c.reltoastrelid) END AS toast_total_bytes,
       pg_total_relation_size(c.oid) AS total_bytes
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname = 'vidpg'
  AND c.relkind = 'r'
ORDER BY c.relname;

SELECT pid,
       usename,
       application_name,
       state,
       xact_start,
       query_start,
       wait_event_type,
       wait_event
FROM pg_stat_activity
WHERE xact_start IS NOT NULL
ORDER BY xact_start;

SELECT a.pid,
       a.state,
       l.mode,
       l.granted,
       l.relation::regclass AS relation,
       a.xact_start
FROM pg_locks l
JOIN pg_stat_activity a ON a.pid = l.pid
JOIN pg_class c ON c.oid = l.relation
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname = 'vidpg'
ORDER BY l.granted, a.xact_start;

SELECT pg_notification_queue_usage() AS queue_fraction;
