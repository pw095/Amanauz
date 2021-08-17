INSERT
  INTO master_data_emitent_map
  (
    tech$load_id,
    tech$load_dttm,
    source_system_code,
    emitent_source_name,
    emitent_full_name
  )
VALUES
  (
    :tech$load_id,
    :tech$load_dttm,
    :source_system_code,
    :emitent_source_name,
    :emitent_full_name
  )
