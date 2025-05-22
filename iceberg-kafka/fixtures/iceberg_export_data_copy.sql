SELECT
    concat('id_', toString(rand() % 10000)) AS id,
    concat('name_', toString(rand() % 1000)) AS name,
    round(rand() * 100, 2) AS value,
    now() - toIntervalSecond(rand() % 2592000) AS timestamp
FROM numbers(10)