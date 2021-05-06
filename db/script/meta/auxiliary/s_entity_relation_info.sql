SELECT
       elm_id,
       rll_effective_to_dt
  FROM (SELECT
               elm.elm_id,
               rll.rll_effective_to_dt,
               ROW_NUMBER() OVER (PARTITION BY elm.elm_id ORDER BY rll.rll_id DESC) AS rn
          FROM tbl_entity_layer_map elm
               LEFT JOIN
               tbl_entity_load_log ell
                   ON ell.ell_elm_id = elm.elm_id
               LEFT JOIN
               tbl_relation_load_log rll
                   ON rll.rll_ell_id = ell.ell_id)
 WHERE rn = 1
