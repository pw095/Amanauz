SELECT
       ell_iteration_number
  FROM tbl_entity_load_log
 WHERE ell_elm_id = ?
 ORDER BY ell_id DESC
LIMIT 1
