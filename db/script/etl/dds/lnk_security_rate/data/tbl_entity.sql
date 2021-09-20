INSERT
  INTO tbl_entity
  (
    ent_code
  )
VALUES
  (
    'lnk_security_rate'
  )
ON CONFLICT(ent_code) DO UPDATE
   SET ent_desc = excluded.ent_desc
 WHERE IFNULL(ent_desc, 'FF') != IFNULL(excluded.ent_desc, 'FF');