SELECT
       MIN(rll_effective_to_dt) AS rll_effective_to_dt
  FROM (SELECT
               rll.rll_effective_to_dt,
               DENSE_RANK() OVER(PARTITION BY 1 ORDER BY ell.ell_flow_log_id DESC, ell.ell_id DESC) AS rnk
          FROM tbl_entity_layer_map elm
               JOIN
               tbl_elm_dependency elmd
                   ON elmd.elmd_elm_id = elm.elm_id
               JOIN
               tbl_entity_load_log ell
                   ON ell.ell_elm_id = elmd.elmd_parent_elm_id
                  AND ell.ell_iteration_insert_row_count != 0
               LEFT JOIN
               tbl_relation_load_log rll
                   ON rll.rll_ell_id = ell.ell_id
         WHERE elm.elm_id = :elm_id)
 WHERE rnk = 1
