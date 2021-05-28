SELECT
       ell_id
  FROM tbl_entity_load_log
 WHERE ell_flow_log_id = :ell_flow_log_id
   AND ell_elm_id = :ell_elm_id
