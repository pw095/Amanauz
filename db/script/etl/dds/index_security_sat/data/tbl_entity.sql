INSERT
  INTO tbl_entity
  (
    ent_code
  )
VALUES
  (
    'index_security_sat'
  )
ON CONFLICT(ent_code) DO UPDATE
   SET ent_desc = excluded.ent_desc
 WHERE IFNULL(ent_desc, 'FF') != IFNULL(excluded.ent_desc, 'FF');
