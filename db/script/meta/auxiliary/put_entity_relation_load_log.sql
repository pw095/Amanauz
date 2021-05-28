INSERT
  INTO tbl_relation_load_log
  (
    rll_ell_id,
    rll_mode,
    rll_effective_from_dt,
    rll_effective_to_dt
  )
VALUES
  (
    :rll_ell_id,
    :rll_mode,
    :rll_effective_from_dt,
    :rll_effective_to_dt
  )
