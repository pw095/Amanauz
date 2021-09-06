INSERT OR IGNORE
  INTO tbl_elm_dependency
  (
    elmd_elm_id,
    elmd_parent_elm_id
  )
SELECT
       elm.elm_id    AS elmd_elm_id,
       parent.elm_id AS elmd_parent_elm_id
  FROM (SELECT
               elm_id
          FROM tbl_entity_layer_map elm
         WHERE EXISTS(SELECT
                             NULL
                        FROM tbl_layer layer
                       WHERE layer.layer_id = elm.elm_layer_id
                         AND layer.layer_code = 'dds')
           AND EXISTS(SELECT
                             NULL
                        FROM tbl_entity ent
                       WHERE ent.ent_id = elm.elm_ent_id
                         AND ent.ent_code = 'ref_fin_statement')) elm
       CROSS JOIN
       (SELECT
               elm_id
          FROM tbl_entity_layer_map elm
         WHERE EXISTS(SELECT
                             NULL
                        FROM tbl_layer layer
                       WHERE layer.layer_id = elm.elm_layer_id
                         AND layer.layer_code = 'repl')
           AND EXISTS(SELECT
                             NULL
                        FROM tbl_entity ent
                       WHERE ent.ent_id = elm.elm_ent_id
                         AND ent.ent_code = 'master_data_ref_fin_statement')) parent;
