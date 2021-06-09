INSERT
  INTO tbl_layer
  (
    layer_code
  )
VALUES
  (
    'repl'
  )
ON CONFLICT(layer_code) DO UPDATE
   SET layer_desc = excluded.layer_desc
 WHERE IFNULL(layer_desc, 'FF') != IFNULL(excluded.layer_desc, 'FF');
