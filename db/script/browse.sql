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
                security_id,
                board_id,
                face_value,
                previous_date,
                sha1(concat_value) AS sha1_value,
                load_dt
           FROM (SELECT
                        security_id,
                        board_id,
                        face_value,
                        previous_date,
                        '_' || IFNULL(CAST(face_value    AS TEXT), '!@#$%^&*') ||
                        '_' || IFNULL(CAST(previous_date AS TEXT), '!@#$%^&*') || '_' AS concat_value,
                        load_dt
                   FROM (SELECT
                                security_id,
                                board_id,
                                face_value,
                                previous_date,
                                load_dt,
                                ROW_NUMBER() OVER (PARTITION BY security_id, board_id, load_dt
                                                       ORDER BY load_dttm DESC)                AS rn
                           FROM (SELECT
                                        data.security_id,
                                        data.board_id,
                                        data.face_value,
                                        data.previous_date,
                                        DATE(tech$load_dttm) AS load_dt,
                                        tech$load_dttm       AS load_dttm
                                   FROM security_daily_info data))
                  WHERE rn = 1)
     )
SELECT
       security_id,
       board_id,
       face_value,
       previous_date,
       load_dt                                                                                 AS tech$effective_dt,
       LEAD(DATE(load_dt, '-1 DAY'), 1, '2999-12-31') OVER (PARTITION BY security_id, board_id
                                                                ORDER BY load_dt)              AS tech$expiration_dt,
       sha1_value
  FROM (SELECT
               security_id,
               board_id,
               face_value,
               previous_date,
               sha1_value,
               load_dt
          FROM (SELECT
                       security_id,
                       board_id,
                       face_value,
                       previous_date,
                       sha1_value,
                       LAG(sha1_value) OVER (PARTITION BY security_id, board_id
                                                 ORDER BY load_dt)              AS lag_sha1_value,
                       load_dt
                  FROM w_raw)
         WHERE sha1_value != lag_sha1_value
            OR lag_sha1_value IS NULL);
