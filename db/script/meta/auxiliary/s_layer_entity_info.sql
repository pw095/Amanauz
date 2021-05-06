WITH w_log AS
     (
         SELECT
                ell_elm_id,
                ell_iteration_number
           FROM (SELECT
                        ell_elm_id,
                        ell_iteration_number,
                        ROW_NUMBER() OVER (PARTITION BY ell_elm_id ORDER BY ell_flow_log_id DESC) AS rn
                   FROM tbl_entity_load_log)
          WHERE rn = 1
     )
SELECT
       elm.elm_id,
       layer.layer_code,
       ent.ent_code,
       elm.elm_mode,
       elm.elm_enabled,
       log.ell_iteration_number
  FROM tbl_entity ent
       JOIN
       tbl_entity_layer_map elm
           ON elm.elm_ent_id = ent.ent_id
       JOIN
       tbl_layer layer
           ON layer.layer_id = elm.elm_layer_id
       LEFT JOIN
       w_log log
           ON log.ell_elm_id = elm.elm_id
