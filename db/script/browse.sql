WITH RECURSIVE
     w_rec(elm_id, child_elm_id, level) AS
     (
       VALUES(6, NULL, 0) /*UNION VALUES(10, NULL, 0)*/
       UNION
       SELECT
              elmd.elmd_parent_elm_id AS elm_id,
              elmd.elmd_elm_id        AS child_elm_id,
              rec.level+1 AS level
--               elmd.elmd_elm_id
         FROM w_rec rec
              JOIN
              tbl_elm_dependency elmd
                  ON (elmd.elmd_elm_id = rec.elm_id)
     )
/*SELECT
       *
  FROM w_rec;*/
 SELECT
        elm_id,
        child_elm_id,
        level
   FROM (SELECT elm_id,
                child_elm_id,
                level,
                ROW_NUMBER() OVER (PARTITION BY elm_id/*, child_elm_id*/ ORDER BY level DESC) AS rn
           FROM w_rec)
  WHERE rn = 1;


WITH RECURSIVE
     w_rec(elm_id, child_elm_id, level) AS
     (
       VALUES(6, NULL, 0) UNION VALUES(9, NULL, 0)
       UNION
       SELECT
              elmd.elmd_parent_elm_id AS elm_id,
              elmd.elmd_elm_id        AS child_elm_id,
              rec.level + 1           AS level
         FROM w_rec rec
              JOIN
              tbl_elm_dependency elmd
                  ON (elmd.elmd_elm_id = rec.elm_id)
     )
 SELECT
        elm_id,
        child_elm_id,
        level
   FROM (SELECT elm_id,
                child_elm_id,
                level,
                ROW_NUMBER() OVER (PARTITION BY elm_id, child_elm_id ORDER BY level DESC) AS rn
           FROM w_rec)
 WHERE rn = 1
 ORDER BY level DESC;


WITH RECURSIVE
     w_rec(elm_id, child_elm_id, level) AS
     (
       SELECT
              elm_id,
              NULL,
              0
         FROM tbl_entity_layer_map
--        VALUES(6, NULL, 0) UNION VALUES(10, NULL, 0) UNION VALUES(10, NULL, 0)
       UNION
       SELECT
              elmd.elmd_parent_elm_id AS elm_id,
              elmd.elmd_elm_id        AS child_elm_id,
              rec.level+1             AS level
         FROM w_rec rec
              JOIN
              tbl_elm_dependency elmd
                  ON (elmd.elmd_elm_id = rec.elm_id)
     )
 SELECT
        child_elm_id,
        COUNT(*)      AS cnt
   FROM (SELECT elm_id,
                child_elm_id,
                level,
                ROW_NUMBER() OVER (PARTITION BY elm_id, child_elm_id ORDER BY level DESC) AS rn
           FROM w_rec)
 WHERE rn = 1
   AND child_elm_id IS NOT NULL
 GROUP BY
          child_elm_id;



SELECT * FROM tbl_elm_dependency;
SELECT *
  FROM tbl_entity ent
       JOIN
       tbl_entity_layer_map telm
           on ent.ent_id = telm.elm_ent_id;
SELECT * FROM sqlite_master;
SELECT * FROM pragma_index_list('tbl_entity');
SELECT * FROM pragma_index_info('tbl_entity');
SELECT * FROM pragma_analysis_limit(-1);
PRAGMA analysis_limit=-1;
select t.* from sqlite_master m, pragma_index_list(m.name) t, pragma_index_info;


select load_extension('C:/Users/pw095/Documents/Git/Amanauz/db/file/sha1.dll');

SELECT * FROM foreign_currency_dictionary WHERE id = 'R01010';
SELECT *
  FROM foreign_currency_rate;

SELECT count(*)
  FROM security_daily_info_bonds;

SELECT COUNT(*) AS cnt
  FROM security_rate_shares;

SELECT * FROM security_rate_shares;


SELECT
       board_id,
       trade_date,
       security_id,
       COUNT(*) AS cnt
  FROM security_rate_shares
 GROUP BY
          board_id,
          trade_date,
          security_id
HAVING COUNT(*) > 1;

SELECT *
  FROM security_rate_bonds;

SELECT
       board_id,
       trade_date,
       security_id,
       COUNT(*) AS cnt
  FROM security_rate_bonds
 GROUP BY
          board_id,
          trade_date,
          security_id
HAVING COUNT(*) > 1;
SELECT
       index_id,
       trade_date,
       security_id,
       COUNT(*) AS cnt
  FROM index_security_weight
 GROUP BY
          index_id,
          trade_date,
          security_id
HAVING COUNT(*) > 1;


SELECT
       index_id,
       trade_date,
       ticker,
       COUNT(*) AS cnt
  FROM index_security_weight
 GROUP BY
          index_id,
          trade_date,
          ticker
HAVING COUNT(*) > 1;

SELECT
       COUNT(tech$load_id),
       COUNT(tech$load_dttm),
       COUNT(index_id),
       COUNT(trade_date),
       COUNT(ticker),
       COUNT(short_name),
       COUNT(security_id),
       COUNT(weight),
       COUNT(trading_session)
  FROM index_security_weight;

select load_extension('C:/Users/pw095/Documents/Git/Amanauz/db/file/sha1.dll');
ATTACH DATABASE 'C:/Users/pw095/Documents/Git/Amanauz/db/file/repl.db' AS repl;
DETACH DATABASE repl;
SELECT * FROM repl.sqlite_master;
SELECT * FROM repl.tech$index_security_weight;
SELECT distinct tech$load_id FROM index_security_weight;
INSERT
  INTO repl.tech$index_security_weight
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
WITH
     w_raw AS
     (
         SELECT
                load_dt,
                sha1(concat_value) AS hash_value,
                index_id,
                trade_date,
                security_id,
                ticker,
                short_name,
                weight,
                trading_session
           FROM (SELECT
                        load_dt,
                        '_' || IFNULL(CAST(ticker          AS TEXT), '!@#$%^&*') ||
                        '_' || IFNULL(CAST(short_name      AS TEXT), '!@#$%^&*') ||
                        '_' || IFNULL(CAST(weight          AS TEXT), '!@#$%^&*') ||
                        '_' || IFNULL(CAST(trading_session AS TEXT), '!@#$%^&*') || '_' AS concat_value,
                        index_id,
                        trade_date,
                        security_id,
                        ticker,
                        short_name,
                        weight,
                        trading_session
                   FROM (SELECT
                                load_dt,
                                ROW_NUMBER() OVER (PARTITION BY index_id,
                                                                trade_date,
                                                                security_id,
                                                                load_dt
                                                       ORDER BY load_dttm DESC) AS rn,
                                index_id,
                                trade_date,
                                security_id,
                                ticker,
                                short_name,
                                weight,
                                trading_session
                           FROM (SELECT
                                        sha1(CAST(index_id    AS TEXT) ||
                                             CAST(trade_date  AS TEXT) ||
                                             CAST(security_id AS TEXT))   AS hash_key,
                                        DATE(tech$load_dttm)              AS load_dt,
                                        tech$load_dttm                    AS load_dttm,
                                        index_id,
                                        trade_date,
                                        security_id,
                                        ticker,
                                        short_name,
                                        weight,
                                        trading_session
                                   FROM index_security_weight
                                  WHERE tech$load_id >= 2))
                  WHERE rn = 1)
     )
SELECT
       1                                                                             AS tech$load_id,
       load_dt                                                                       AS tech$effective_dt,
       LEAD(DATE(load_dt, '-1 DAY'), 1, '2999-12-31') OVER (PARTITION BY index_id,
                                                                         trade_date,
                                                                         security_id
                                                                ORDER BY load_dt)    AS tech$expiration_dt,
       hash_value                                                                    AS tech$hash_value,
       index_id,
       trade_date,
       security_id,
       ticker,
       short_name,
       weight,
       trading_session
  FROM (SELECT
               load_dt,
               hash_value,
               index_id,
               trade_date,
               security_id,
               ticker,
               short_name,
               weight,
               trading_session
          FROM (SELECT
                       load_dt,
                       hash_value,
                       LAG(hash_value) OVER (PARTITION BY index_id,
                                                          trade_date,
                                                          security_id
                                                 ORDER BY load_dt) AS lag_hash_value,
                       index_id,
                       trade_date,
                       security_id,
                       ticker,
                       short_name,
                       weight,
                       trading_session
                  FROM w_raw)
         WHERE hash_value != lag_hash_value
            OR lag_hash_value IS NULL);




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
