SELECT
       rll_effective_to_dt
  FROM tbl_relation_load_log rll
 WHERE EXISTS(SELECT
                     NULL
                FROM tbl_entity_load_log ell
               WHERE ell.ell_id = rll.rll_ell_id
                 AND ell.ell_elm_id = :ell_elm_id)
 ORDER BY rll_id DESC
 LIMIT 1
