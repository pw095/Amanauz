SELECT * FROM tech$index_security_weight WHERE tech$expiration_dt != '2999-12-31';

SELECT tech$effective_dt, COUNT(*) AS cnt
  FROM tech$index_security_weight GROUP BY tech$effective_dt;
SELECT *
  FROM tech$index_security_weight;
alter table tech$index_security_weight rename to tech$index_security_weight2;
create table tech$index_security_weight as select * from tech$index_security_weight2 where 1 = 2;

-- Добавляем 1 запись

DELETE FROM tech$index_security_weight;
DELETE FROM index_security_weight;

INSERT
  INTO tech$index_security_weight
SELECT *
  FROM tech$index_security_weight2
 WHERE tech$load_id = 1
   AND index_id = 'IMOEX'
   AND trade_date = '2021-06-07'
   AND security_id = 'Аэрофлот'
   AND tech$effective_dt = '2021-06-08';

SELECT *
  FROM tech$index_security_weight;
SELECT *
  FROM index_security_weight;

INSERT
  INTO index_security_weight
  (
    tech$load_id,
    tech$effective_dt,
    tech$expiration_dt,
    tech$hash_value,
    index_id,
    trade_date,
    ticker,
    short_name,
    security_id,
    weight,
    trading_session
  )
SELECT
       1                  AS tech$load_id,
       tech$effective_dt,
       tech$expiration_dt,
       tech$hash_value,
       index_id,
       trade_date,
       ticker,
       short_name,
       security_id,
       weight,
       trading_session
  FROM (SELECT
               tech$effective_dt,
               tech$expiration_dt,
               tech$hash_value,
               index_id,
               trade_date,
               ticker,
               short_name,
               security_id,
               weight,
               trading_session
          FROM tech$index_security_weight src
         WHERE NOT EXISTS(SELECT
                                 NULL
                            FROM index_security_weight sat
                           WHERE sat.index_id = src.index_id
                             AND sat.trade_date = src.trade_date
                             AND sat.security_id = src.security_id
                             AND sat.tech$expiration_dt = '2999-12-31')
         UNION ALL
        SELECT
               CASE mrg.flg
                    WHEN 'INSERT' THEN
                        tech$effective_dt
                    WHEN 'UPDATE' THEN
                        tech$sat$effective_dt
               END AS tech$effective_dt,
               CASE mrg.flg
                    WHEN 'INSERT' THEN
                        tech$expiration_dt
                    WHEN 'UPDATE' THEN
                        tech$sat$expiration_dt
               END AS tech$expiration_dt,
               tech$hash_value,
               index_id,
               trade_date,
               ticker,
               short_name,
               security_id,
               weight,
               trading_session
          FROM (SELECT
                       tech$effective_dt,
                       tech$expiration_dt,
                       tech$sat$effective_dt,
                       tech$sat$expiration_dt,
                       tech$hash_value,
                       index_id,
                       trade_date,
                       ticker,
                       short_name,
                       security_id,
                       weight,
                       trading_session,
                       CASE
                            WHEN rn = 1
                              OR rn = 2 AND fv_equal_flag = 'EQUAL' THEN
                                'UPSERT'
                            ELSE
                                'INSERT'
                       END AS upsert_flg
                  FROM (SELECT
                               src.tech$effective_dt,
                               src.tech$expiration_dt,
                               sat.tech$effective_dt                 AS tech$sat$effective_dt,
                               DATE(src.tech$effective_dt, '-1 DAY') AS tech$sat$expiration_dt,
                               src.tech$hash_value,
                               src.index_id,
                               src.trade_date,
                               src.ticker,
                               src.short_name,
                               src.security_id,
                               src.weight,
                               src.trading_session,
                               FIRST_VALUE(CASE
                                                WHEN src.tech$hash_value != sat.tech$hash_value THEN
                                                    'NON_EQUAL'
                                                ELSE
                                                    'EQUAL'
                                           END) OVER (wnd) AS fv_equal_flag,
                               ROW_NUMBER() OVER (wnd)     AS rn
                          FROM tech$index_security_weight src
                               JOIN
                               index_security_weight sat
                                   ON sat.index_id = src.index_id
                                  AND sat.tech$effective_dt < src.tech$effective_dt
                                  AND sat.tech$expiration_dt = '2999-12-31'
                        WINDOW wnd AS (PARTITION BY src.index_id,
                                                    src.security_id,
                                                    src.trade_date
                                           ORDER BY src.tech$effective_dt))
                 WHERE rn = 1 AND fv_equal_flag = 'NON_EQUAL'
                    OR rn > 1) src
               CROSS JOIN
               (SELECT 'INSERT' AS flg
                 UNION ALL
                SELECT 'UPDATE' AS flg) mrg)
 WHERE 1 = 1
 ON CONFLICT(index_id, security_id, trade_date, tech$effective_dt)
 DO UPDATE
       SET tech$expiration_dt = excluded.tech$expiration_dt;

SELECT *
  FROM index_security_weight;

------------------------------

-- Добавляем 1 запись с тем же значением бизнес-ключа и hash_value, но другой датой начала актуальности

DELETE FROM tech$index_security_weight;

INSERT
  INTO tech$index_security_weight
SELECT *
  FROM tech$index_security_weight2
 WHERE tech$load_id = 1
   AND index_id = 'IMOEX'
   AND trade_date = '2021-06-07'
   AND security_id = 'Аэрофлот'
   AND tech$effective_dt = '2021-06-08';
UPDATE tech$index_security_weight
   SET tech$effective_dt = '2021-06-09'
 WHERE tech$load_id = 1
   AND index_id = 'IMOEX'
   AND trade_date = '2021-06-07'
   AND security_id = 'Аэрофлот'
   AND tech$effective_dt = '2021-06-08';

SELECT *
  FROM tech$index_security_weight;
SELECT *
  FROM index_security_weight;

INSERT
  INTO index_security_weight
  (
    tech$load_id,
    tech$effective_dt,
    tech$expiration_dt,
    tech$hash_value,
    index_id,
    trade_date,
    ticker,
    short_name,
    security_id,
    weight,
    trading_session
  )
SELECT
       1                  AS tech$load_id,
       tech$effective_dt,
       tech$expiration_dt,
       tech$hash_value,
       index_id,
       trade_date,
       ticker,
       short_name,
       security_id,
       weight,
       trading_session
  FROM (SELECT
               tech$effective_dt,
               tech$expiration_dt,
               tech$hash_value,
               index_id,
               trade_date,
               ticker,
               short_name,
               security_id,
               weight,
               trading_session
          FROM tech$index_security_weight src
         WHERE NOT EXISTS(SELECT
                                 NULL
                            FROM index_security_weight sat
                           WHERE sat.index_id = src.index_id
                             AND sat.trade_date = src.trade_date
                             AND sat.security_id = src.security_id
                             AND sat.tech$expiration_dt = '2999-12-31')
         UNION ALL
        SELECT
               CASE mrg.flg
                    WHEN 'INSERT' THEN
                        tech$effective_dt
                    WHEN 'UPDATE' THEN
                        tech$sat$effective_dt
               END AS tech$effective_dt,
               CASE mrg.flg
                    WHEN 'INSERT' THEN
                        tech$expiration_dt
                    WHEN 'UPDATE' THEN
                        tech$sat$expiration_dt
               END AS tech$expiration_dt,
               tech$hash_value,
               index_id,
               trade_date,
               ticker,
               short_name,
               security_id,
               weight,
               trading_session
          FROM (SELECT
                       tech$effective_dt,
                       tech$expiration_dt,
                       tech$sat$effective_dt,
                       tech$sat$expiration_dt,
                       tech$hash_value,
                       index_id,
                       trade_date,
                       ticker,
                       short_name,
                       security_id,
                       weight,
                       trading_session,
                       CASE
                            WHEN rn = 1
                              OR rn = 2 AND fv_equal_flag = 'EQUAL' THEN
                                'UPSERT'
                            ELSE
                                'INSERT'
                       END AS upsert_flg
                  FROM (SELECT
                               src.tech$effective_dt,
                               src.tech$expiration_dt,
                               sat.tech$effective_dt                 AS tech$sat$effective_dt,
                               DATE(src.tech$effective_dt, '-1 DAY') AS tech$sat$expiration_dt,
                               src.tech$hash_value,
                               src.index_id,
                               src.trade_date,
                               src.ticker,
                               src.short_name,
                               src.security_id,
                               src.weight,
                               src.trading_session,
                               FIRST_VALUE(CASE
                                                WHEN src.tech$hash_value != sat.tech$hash_value THEN
                                                    'NON_EQUAL'
                                                ELSE
                                                    'EQUAL'
                                           END) OVER (wnd) AS fv_equal_flag,
                               ROW_NUMBER() OVER (wnd)     AS rn
                          FROM tech$index_security_weight src
                               JOIN
                               index_security_weight sat
                                   ON sat.index_id = src.index_id
                                  AND sat.tech$effective_dt < src.tech$effective_dt
                                  AND sat.tech$expiration_dt = '2999-12-31'
                        WINDOW wnd AS (PARTITION BY src.index_id,
                                                    src.security_id,
                                                    src.trade_date
                                           ORDER BY src.tech$effective_dt))
                 WHERE rn = 1 AND fv_equal_flag = 'NON_EQUAL'
                    OR rn > 1) src
               CROSS JOIN
               (SELECT 'INSERT' AS flg
                 UNION ALL
                SELECT 'UPDATE' AS flg) mrg)
 WHERE 1 = 1
 ON CONFLICT(index_id, security_id, trade_date, tech$effective_dt)
 DO UPDATE
       SET tech$expiration_dt = excluded.tech$expiration_dt;

SELECT *
  FROM index_security_weight;

---- Добавляем 1 запись с тем же значением бизнес-ключа, но другим hash_value, и другой датой начала актуальности

DELETE FROM tech$index_security_weight;

INSERT
  INTO tech$index_security_weight
SELECT *
  FROM tech$index_security_weight2
 WHERE tech$load_id = 1
   AND index_id = 'IMOEX'
   AND trade_date = '2021-06-07'
   AND security_id = 'Аэрофлот'
   AND tech$effective_dt = '2021-06-08';
UPDATE tech$index_security_weight
   SET tech$effective_dt = '2021-06-09',
       weight = weight + 1,
       tech$hash_value = 'u'
 WHERE tech$load_id = 1
   AND index_id = 'IMOEX'
   AND trade_date = '2021-06-07'
   AND security_id = 'Аэрофлот'
   AND tech$effective_dt = '2021-06-08';

SELECT *
  FROM tech$index_security_weight;
SELECT *
  FROM index_security_weight;

INSERT
  INTO index_security_weight
  (
    tech$load_id,
    tech$effective_dt,
    tech$expiration_dt,
    tech$hash_value,
    index_id,
    trade_date,
    ticker,
    short_name,
    security_id,
    weight,
    trading_session
  )
SELECT
       1                  AS tech$load_id,
       tech$effective_dt,
       tech$expiration_dt,
       tech$hash_value,
       index_id,
       trade_date,
       ticker,
       short_name,
       security_id,
       weight,
       trading_session
  FROM (SELECT
               tech$effective_dt,
               tech$expiration_dt,
               tech$hash_value,
               index_id,
               trade_date,
               ticker,
               short_name,
               security_id,
               weight,
               trading_session
          FROM tech$index_security_weight src
         WHERE NOT EXISTS(SELECT
                                 NULL
                            FROM index_security_weight sat
                           WHERE sat.index_id = src.index_id
                             AND sat.trade_date = src.trade_date
                             AND sat.security_id = src.security_id
                             AND sat.tech$expiration_dt = '2999-12-31')
         UNION ALL
        SELECT
               CASE mrg.flg
                    WHEN 'INSERT' THEN
                        tech$effective_dt
                    WHEN 'UPDATE' THEN
                        tech$sat$effective_dt
               END AS tech$effective_dt,
               CASE mrg.flg
                    WHEN 'INSERT' THEN
                        tech$expiration_dt
                    WHEN 'UPDATE' THEN
                        tech$sat$expiration_dt
               END AS tech$expiration_dt,
               tech$hash_value,
               index_id,
               trade_date,
               ticker,
               short_name,
               security_id,
               weight,
               trading_session
          FROM (SELECT
                       tech$effective_dt,
                       tech$expiration_dt,
                       tech$sat$effective_dt,
                       tech$sat$expiration_dt,
                       tech$hash_value,
                       index_id,
                       trade_date,
                       ticker,
                       short_name,
                       security_id,
                       weight,
                       trading_session,
                       CASE
                            WHEN rn = 1
                              OR rn = 2 AND fv_equal_flag = 'EQUAL' THEN
                                'UPSERT'
                            ELSE
                                'INSERT'
                       END AS upsert_flg
                  FROM (SELECT
                               src.tech$effective_dt,
                               src.tech$expiration_dt,
                               sat.tech$effective_dt                 AS tech$sat$effective_dt,
                               DATE(src.tech$effective_dt, '-1 DAY') AS tech$sat$expiration_dt,
                               src.tech$hash_value,
                               src.index_id,
                               src.trade_date,
                               src.ticker,
                               src.short_name,
                               src.security_id,
                               src.weight,
                               src.trading_session,
                               FIRST_VALUE(CASE
                                                WHEN src.tech$hash_value != sat.tech$hash_value THEN
                                                    'NON_EQUAL'
                                                ELSE
                                                    'EQUAL'
                                           END) OVER (wnd) AS fv_equal_flag,
                               ROW_NUMBER() OVER (wnd)     AS rn
                          FROM tech$index_security_weight src
                               JOIN
                               index_security_weight sat
                                   ON sat.index_id = src.index_id
                                  AND sat.tech$effective_dt < src.tech$effective_dt
                                  AND sat.tech$expiration_dt = '2999-12-31'
                        WINDOW wnd AS (PARTITION BY src.index_id,
                                                    src.security_id,
                                                    src.trade_date
                                           ORDER BY src.tech$effective_dt))
                 WHERE rn = 1 AND fv_equal_flag = 'NON_EQUAL'
                    OR rn > 1) src
               CROSS JOIN
               (SELECT 'INSERT' AS flg
                 UNION ALL
                SELECT 'UPDATE' AS flg) mrg)
 WHERE 1 = 1
 ON CONFLICT(index_id, security_id, trade_date, tech$effective_dt)
 DO UPDATE
       SET tech$expiration_dt = excluded.tech$expiration_dt;

SELECT *
  FROM index_security_weight;


---- Добавляем 2 запись с тем же значением бизнес-ключа, но другими hash_value, и другими датами начала актуальности

DELETE FROM tech$index_security_weight;

INSERT
  INTO tech$index_security_weight
SELECT *
  FROM tech$index_security_weight2
 WHERE tech$load_id = 1
   AND index_id = 'IMOEX'
   AND trade_date = '2021-06-07'
   AND security_id = 'Аэрофлот'
   AND tech$effective_dt = '2021-06-08';
UPDATE tech$index_security_weight
   SET tech$effective_dt = '2021-06-10',
       weight = weight + 2,
       tech$hash_value = 'v'
 WHERE tech$load_id = 1
   AND index_id = 'IMOEX'
   AND trade_date = '2021-06-07'
   AND security_id = 'Аэрофлот'
   AND tech$effective_dt = '2021-06-08';


INSERT
  INTO tech$index_security_weight
SELECT *
  FROM tech$index_security_weight2
 WHERE tech$load_id = 1
   AND index_id = 'IMOEX'
   AND trade_date = '2021-06-07'
   AND security_id = 'Аэрофлот'
   AND tech$effective_dt = '2021-06-08';
UPDATE tech$index_security_weight
   SET tech$effective_dt = '2021-06-11',
       weight = weight + 3,
       tech$hash_value = 'w'
 WHERE tech$load_id = 1
   AND index_id = 'IMOEX'
   AND trade_date = '2021-06-07'
   AND security_id = 'Аэрофлот'
   AND tech$effective_dt = '2021-06-08';

UPDATE tech$index_security_weight
   SET tech$expiration_dt = '2021-06-10'
 WHERE tech$load_id = 1
   AND index_id = 'IMOEX'
   AND trade_date = '2021-06-07'
   AND security_id = 'Аэрофлот'
   AND tech$effective_dt = '2021-06-10';
SELECT *
  FROM tech$index_security_weight;
SELECT *
  FROM index_security_weight;

INSERT
  INTO index_security_weight
  (
    tech$load_id,
    tech$effective_dt,
    tech$expiration_dt,
    tech$hash_value,
    index_id,
    trade_date,
    ticker,
    short_name,
    security_id,
    weight,
    trading_session
  )
SELECT
       1                  AS tech$load_id,
       tech$effective_dt,
       tech$expiration_dt,
       tech$hash_value,
       index_id,
       trade_date,
       ticker,
       short_name,
       security_id,
       weight,
       trading_session
  FROM (SELECT
               tech$effective_dt,
               tech$expiration_dt,
               tech$hash_value,
               index_id,
               trade_date,
               ticker,
               short_name,
               security_id,
               weight,
               trading_session
          FROM tech$index_security_weight src
         WHERE NOT EXISTS(SELECT
                                 NULL
                            FROM index_security_weight sat
                           WHERE sat.index_id = src.index_id
                             AND sat.trade_date = src.trade_date
                             AND sat.security_id = src.security_id
                             AND sat.tech$expiration_dt = '2999-12-31')
         UNION ALL
        SELECT
               CASE mrg.flg
                    WHEN 'INSERT' THEN
                        tech$effective_dt
                    WHEN 'UPDATE' THEN
                        tech$sat$effective_dt
               END AS tech$effective_dt,
               CASE mrg.flg
                    WHEN 'INSERT' THEN
                        tech$expiration_dt
                    WHEN 'UPDATE' THEN
                        tech$sat$expiration_dt
               END AS tech$expiration_dt,
               tech$hash_value,
               index_id,
               trade_date,
               ticker,
               short_name,
               security_id,
               weight,
               trading_session
          FROM (SELECT
                       tech$effective_dt,
                       tech$expiration_dt,
                       tech$sat$effective_dt,
                       tech$sat$expiration_dt,
                       tech$hash_value,
                       index_id,
                       trade_date,
                       ticker,
                       short_name,
                       security_id,
                       weight,
                       trading_session,
                       CASE
                            WHEN rn = 1
                              OR rn = 2 AND fv_equal_flag = 'EQUAL' THEN
                                'UPSERT'
                            ELSE
                                'INSERT'
                       END AS upsert_flg
                  FROM (SELECT
                               src.tech$effective_dt,
                               src.tech$expiration_dt,
                               sat.tech$effective_dt                 AS tech$sat$effective_dt,
                               DATE(src.tech$effective_dt, '-1 DAY') AS tech$sat$expiration_dt,
                               src.tech$hash_value,
                               src.index_id,
                               src.trade_date,
                               src.ticker,
                               src.short_name,
                               src.security_id,
                               src.weight,
                               src.trading_session,
                               FIRST_VALUE(CASE
                                                WHEN src.tech$hash_value != sat.tech$hash_value THEN
                                                    'NON_EQUAL'
                                                ELSE
                                                    'EQUAL'
                                           END) OVER (wnd) AS fv_equal_flag,
                               ROW_NUMBER() OVER (wnd)     AS rn
                          FROM tech$index_security_weight src
                               JOIN
                               index_security_weight sat
                                   ON sat.index_id = src.index_id
                                  AND sat.tech$effective_dt < src.tech$effective_dt
                                  AND sat.tech$expiration_dt = '2999-12-31'
                        WINDOW wnd AS (PARTITION BY src.index_id,
                                                    src.security_id,
                                                    src.trade_date
                                           ORDER BY src.tech$effective_dt))
                 WHERE rn = 1 AND fv_equal_flag = 'NON_EQUAL'
                    OR rn > 1) src
               CROSS JOIN
               (SELECT 'INSERT' AS flg
                 UNION ALL
                SELECT 'UPDATE' AS flg) mrg
    WHERE src.upsert_flg = 'UPSERT'
       OR src.upsert_flg = 'INSERT' AND mrg.flg = 'INSERT')
 WHERE 1 = 1
 ON CONFLICT(index_id, security_id, trade_date, tech$effective_dt)
 DO UPDATE
       SET tech$expiration_dt = excluded.tech$expiration_dt;

SELECT *
  FROM index_security_weight;




---- Добавляем 2 записи с тем же значением бизнес-ключа, у первой (в порядке возрастания дат актуальности) hash_value такое же, у второй другое. Даты актуальности отличаются.

DELETE FROM tech$index_security_weight;

INSERT
  INTO tech$index_security_weight
SELECT *
  FROM tech$index_security_weight2
 WHERE tech$load_id = 1
   AND index_id = 'IMOEX'
   AND trade_date = '2021-06-07'
   AND security_id = 'Аэрофлот'
   AND tech$effective_dt = '2021-06-08';
UPDATE tech$index_security_weight
   SET tech$effective_dt = '2021-06-12',
       weight = weight + 3,
       tech$hash_value = 'w'
 WHERE tech$load_id = 1
   AND index_id = 'IMOEX'
   AND trade_date = '2021-06-07'
   AND security_id = 'Аэрофлот'
   AND tech$effective_dt = '2021-06-08';


INSERT
  INTO tech$index_security_weight
SELECT *
  FROM tech$index_security_weight2
 WHERE tech$load_id = 1
   AND index_id = 'IMOEX'
   AND trade_date = '2021-06-07'
   AND security_id = 'Аэрофлот'
   AND tech$effective_dt = '2021-06-08';
UPDATE tech$index_security_weight
   SET tech$effective_dt = '2021-06-13',
       weight = weight + 4,
       tech$hash_value = 'x'
 WHERE tech$load_id = 1
   AND index_id = 'IMOEX'
   AND trade_date = '2021-06-07'
   AND security_id = 'Аэрофлот'
   AND tech$effective_dt = '2021-06-08';

UPDATE tech$index_security_weight
   SET tech$expiration_dt = '2021-06-12'
 WHERE tech$load_id = 1
   AND index_id = 'IMOEX'
   AND trade_date = '2021-06-07'
   AND security_id = 'Аэрофлот'
   AND tech$effective_dt = '2021-06-12';
SELECT *
  FROM tech$index_security_weight;
SELECT *
  FROM index_security_weight;

INSERT
  INTO index_security_weight
  (
    tech$load_id,
    tech$effective_dt,
    tech$expiration_dt,
    tech$hash_value,
    index_id,
    trade_date,
    ticker,
    short_name,
    security_id,
    weight,
    trading_session
  )
SELECT
       1                  AS tech$load_id,
       tech$effective_dt,
       tech$expiration_dt,
       tech$hash_value,
       index_id,
       trade_date,
       ticker,
       short_name,
       security_id,
       weight,
       trading_session
  FROM (SELECT
               tech$effective_dt,
               tech$expiration_dt,
               tech$hash_value,
               index_id,
               trade_date,
               ticker,
               short_name,
               security_id,
               weight,
               trading_session
          FROM tech$index_security_weight src
         WHERE NOT EXISTS(SELECT
                                 NULL
                            FROM index_security_weight sat
                           WHERE sat.index_id = src.index_id
                             AND sat.trade_date = src.trade_date
                             AND sat.security_id = src.security_id
                             AND sat.tech$expiration_dt = '2999-12-31')
         UNION ALL
        SELECT
               CASE mrg.flg
                    WHEN 'INSERT' THEN
                        tech$effective_dt
                    WHEN 'UPDATE' THEN
                        tech$sat$effective_dt
               END AS tech$effective_dt,
               CASE mrg.flg
                    WHEN 'INSERT' THEN
                        tech$expiration_dt
                    WHEN 'UPDATE' THEN
                        tech$sat$expiration_dt
               END AS tech$expiration_dt,
               tech$hash_value,
               index_id,
               trade_date,
               ticker,
               short_name,
               security_id,
               weight,
               trading_session
          FROM (SELECT
                       tech$effective_dt,
                       tech$expiration_dt,
                       tech$sat$effective_dt,
                       tech$sat$expiration_dt,
                       tech$hash_value,
                       index_id,
                       trade_date,
                       ticker,
                       short_name,
                       security_id,
                       weight,
                       trading_session,
                       CASE
                            WHEN rn = 1
                              OR rn = 2 AND fv_equal_flag = 'EQUAL' THEN
                                'UPSERT'
                            ELSE
                                'INSERT'
                       END AS upsert_flg
                  FROM (SELECT
                               src.tech$effective_dt,
                               src.tech$expiration_dt,
                               sat.tech$effective_dt                 AS tech$sat$effective_dt,
                               DATE(src.tech$effective_dt, '-1 DAY') AS tech$sat$expiration_dt,
                               src.tech$hash_value,
                               src.index_id,
                               src.trade_date,
                               src.ticker,
                               src.short_name,
                               src.security_id,
                               src.weight,
                               src.trading_session,
                               FIRST_VALUE(CASE
                                                WHEN src.tech$hash_value != sat.tech$hash_value THEN
                                                    'NON_EQUAL'
                                                ELSE
                                                    'EQUAL'
                                           END) OVER (wnd) AS fv_equal_flag,
                               ROW_NUMBER() OVER (wnd)     AS rn
                          FROM tech$index_security_weight src
                               JOIN
                               index_security_weight sat
                                   ON sat.index_id = src.index_id
                                  AND sat.tech$effective_dt < src.tech$effective_dt
                                  AND sat.tech$expiration_dt = '2999-12-31'
                        WINDOW wnd AS (PARTITION BY src.index_id,
                                                    src.security_id,
                                                    src.trade_date
                                           ORDER BY src.tech$effective_dt))
                 WHERE rn = 1 AND fv_equal_flag = 'NON_EQUAL'
                    OR rn > 1) src
               CROSS JOIN
               (SELECT 'INSERT' AS flg
                 UNION ALL
                SELECT 'UPDATE' AS flg) mrg
    WHERE src.upsert_flg = 'UPSERT'
       OR src.upsert_flg = 'INSERT' AND mrg.flg = 'INSERT')
 WHERE 1 = 1
 ON CONFLICT(index_id, security_id, trade_date, tech$effective_dt)
 DO UPDATE
       SET tech$expiration_dt = excluded.tech$expiration_dt;

SELECT *
  FROM index_security_weight;
