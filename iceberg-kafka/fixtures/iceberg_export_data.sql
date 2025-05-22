SELECT
    concat('id_', toString(1000 + rand() % 9000)) AS id,
    concat('Item ', toString(rand() % 100)) AS name,
    round(rand() % 1000 + rand(), 2) AS value,
    now() - toIntervalSecond(rand() % (86400 * 30)) AS timestamp
FROM numbers(10)