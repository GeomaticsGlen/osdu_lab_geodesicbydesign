WITH unmatched AS (
    SELECT r.id, r.kind
    FROM records r
    LEFT JOIN schema_registry s ON r.kind = s.kind
    WHERE s.kind IS NULL
),
matched AS (
    SELECT COUNT(*) AS matched_count
    FROM records r
    WHERE r.kind IN (SELECT kind FROM schema_registry)
),
unmatched_count AS (
    SELECT COUNT(*) AS unmatched_count FROM unmatched
)

SELECT
    m.matched_count,
    u.unmatched_count
FROM matched m, unmatched_count u;

-- List unmatched records
SELECT id, kind FROM (
    SELECT r.id, r.kind
    FROM records r
    LEFT JOIN schema_registry s ON r.kind = s.kind
    WHERE s.kind IS NULL
) AS unmatched_records;

