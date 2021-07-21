INSERT
  INTO default_data_security_type
  (
    tech$load_id,
    tech$load_dttm,
    security_type_id,
    security_type_name
  )
VALUES
  (
    :tech$load_id,
    :tech$load_dttm,
    :security_type_id,
    :security_type_name
  )
