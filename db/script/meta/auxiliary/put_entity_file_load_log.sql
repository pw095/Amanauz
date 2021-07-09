INSERT
  INTO tbl_file_load_log
  (
    fll_ell_id,
    fll_name,
    fll_size,
    fll_hash_sum
  )
VALUES
  (
    :fll_ell_id,
    :fll_name,
    :fll_size,
    :fll_hash_sum
  )
