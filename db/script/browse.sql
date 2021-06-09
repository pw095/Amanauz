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




