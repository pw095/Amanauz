INSERT
  INTO tbl_entity_layer_map
  (
    elm_ent_id,
    elm_layer_id,
    elm_mode,
    elm_enabled
  )
SELECT
       ent.ent_id     AS elm_ent_id,
       layer.layer_id AS elm_layer_id,
       'full'         AS elm_mode,
       'enabled'      AS elm_enabled
  FROM tbl_layer layer
       CROSS JOIN
       tbl_entity ent
 WHERE layer.layer_code = 'stage'
   AND ent.ent_code = 'security_daily_marketdata_shares'
ON CONFLICT(elm_ent_id, elm_layer_id) DO UPDATE
   SET elm_mode = excluded.elm_mode,
       elm_enabled = excluded.elm_enabled
 WHERE IFNULL(elm_mode, 'FF') != IFNULL(excluded.elm_mode, 'FF')
    OR IFNULL(elm_enabled, 'FF') != IFNULL(excluded.elm_enabled, 'FF');
