SELECT
       elm_id,
       elm_mode
  FROM tbl_entity_layer_map elm
 WHERE elm_enabled = 'enabled'
   AND EXISTS(SELECT
                     NULL
                FROM tbl_layer layer
               WHERE layer.layer_id = elm.elm_layer_id
                 AND layer.layer_code = ?)
   AND EXISTS(SELECT
                     NULL
                FROM tbl_entity ent
               WHERE ent.ent_id = elm.elm_ent_id
                 AND ent.ent_code = ?)
