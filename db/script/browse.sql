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



WITH
     w_raw AS
     (
         SELECT
                hash_key,
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
                        hash_key,
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
                                hash_key,
                                load_dt,
                                ROW_NUMBER() OVER (PARTITION BY hash_key,
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
                                  WHERE tech$load_id IN (100, 101, 102, 103)
                                    AND security_id = 'AFLT'
                                    AND trade_date = '2021-05-12'))
                  WHERE rn = 1)
     )
SELECT
       hash_key                                                                   AS tech$hash_key,
       load_dt                                                                    AS tech$effective_dt,
       LEAD(DATE(load_dt, '-1 DAY'), 1, '2999-12-31') OVER (PARTITION BY hash_key
                                                                ORDER BY load_dt) AS tech$expiration_dt,
       hash_value                                                                 AS tech$hash_value,
       index_id,
       trade_date,
       security_id,
       ticker,
       short_name,
       weight,
       trading_session
  FROM (SELECT
               hash_key,
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
                       hash_key,
                       load_dt,
                       hash_value,
                       LAG(hash_value) OVER (PARTITION BY hash_key
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
