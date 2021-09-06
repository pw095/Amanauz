INSERT
  INTO ref_fin_statement
  (
    tech$load_id,
    tech$load_dt,
    tech$record_source,
    fs_code,
    parent_code,
    full_name,
    leaf_code,
    parent_leaf_code,
    hier_level
  )
SELECT
       :tech$load_id AS tech$load_id,
       tech$load_dt,
       tech$record_source,
       fs_code,
       parent_code,
       full_name,
       leaf_code,
       parent_leaf_code,
       hier_level
  FROM tech$ref_fin_statement src
 WHERE NOT EXISTS(SELECT
                         NULL
                    FROM main.ref_fin_statement sat
                   WHERE sat.fs_code          = src.fs_code
                     AND sat.parent_code      = src.parent_code
                     AND sat.full_name        = src.full_name
                     AND sat.leaf_code        = src.leaf_code
                     AND sat.parent_leaf_code = src.parent_leaf_code
                     AND sat.hier_level       = src.hier_level)
ON CONFLICT(fs_code)
DO UPDATE
   SET tech$load_id       = excluded.tech$load_id,
       tech$load_dt       = excluded.tech$load_dt,
       tech$record_source = excluded.tech$record_source,
       fs_code            = excluded.fs_code,
       parent_code        = excluded.parent_code,
       full_name          = excluded.full_name,
       leaf_code          = excluded.leaf_code,
       parent_leaf_code   = excluded.parent_leaf_code,
       hier_level         = excluded.hier_level
