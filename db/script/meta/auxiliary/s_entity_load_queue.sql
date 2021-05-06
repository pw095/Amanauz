WITH RECURSIVE
     w_rec(elm_id, child_elm_id, level) AS
     (
         SELECT
                elm_id,
                NULL,
                0
           FROM tbl_entity_layer_map
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
       elm_id,
       0        AS parent_cnt
  FROM w_rec rec
 WHERE NOT EXISTS(SELECT
                         NULL
                    FROM tbl_elm_dependency dep_inner
                   WHERE dep_inner.elmd_elm_id = rec.elm_id)
 UNION
SELECT
       child_elm_id  AS elm_id,
       COUNT(*)      AS parent_cnt
  FROM (SELECT elm_id,
               child_elm_id,
               level,
               ROW_NUMBER() OVER (PARTITION BY elm_id, child_elm_id ORDER BY level DESC) AS rn
          FROM w_rec)
WHERE rn = 1
  AND child_elm_id IS NOT NULL
GROUP BY
         child_elm_id
