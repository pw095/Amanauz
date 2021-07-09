WITH
     w_ell AS
     (
         SELECT
                MAX(ell_id) AS ell_id
           FROM tbl_entity_load_log ell
          WHERE EXISTS(SELECT
                              NULL
                         FROM tbl_entity_layer_map elm
                        WHERE elm.elm_id = ell.ell_elm_id
                          AND EXISTS(SELECT
                                            NULL
                                       FROM tbl_layer layer
                                      WHERE layer.layer_id = elm.elm_layer_id
                                        AND layer.layer_code = :layer_code)
                          AND EXISTS(SELECT
                                            NULL
                                       FROM tbl_entity ent
                                      WHERE ent.ent_id = elm.elm_ent_id
                                        AND ent.ent_code = :ent_code))
     )
SELECT
       fll_hash_sum
  FROM tbl_file_load_log fll
 WHERE EXISTS(SELECT
                     NULL
                FROM w_ell ell
               WHERE ell.ell_id = fll.fll_ell_id)
