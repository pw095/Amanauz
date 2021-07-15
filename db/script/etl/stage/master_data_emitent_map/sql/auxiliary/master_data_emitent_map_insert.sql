INSERT
  INTO master_data_emitent_map
  (
    tech$load_id,
    tech$load_dttm,
    source_system_code,
    emitent_code,
    emitent_short_name
  )
VALUES
  (
    :tech$load_id,
    :tech$load_dttm,
    :source_system_code,
    :emitent_code,
    :emitent_short_name
  )
