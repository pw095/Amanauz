INSERT
  INTO tech$ref_fin_statement
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
WITH
     w_pre AS
     (
         SELECT
                tech$effective_dt AS tech$load_dt,
                code              AS fs_code,
                parent_code,
                full_name,
                leaf_code,
                parent_leaf_code,
                hier_level
           FROM src.master_data_ref_fin_statement
          WHERE tech$expiration_dt = '2999-12-31'
            AND tech$effective_dt >= :tech$effective_dt
     )
SELECT
       :tech$load_id,
       tech$load_dt,
       'master_data'     AS tech$record_source,
       fs_code,
       parent_code,
       full_name,
       leaf_code,
       parent_leaf_code,
       hier_level
  FROM w_pre
