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
