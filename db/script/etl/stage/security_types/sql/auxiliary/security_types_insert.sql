INSERT
  INTO security_types
  (
    id,
    trade_engine_id,
    trade_engine_name,
    trade_engine_title,
    security_type_name,
    security_type_title,
    security_group_name,
    tech$load_id
  )
VALUES
  (
    :id,
    :trade_engine_id,
    :trade_engine_name,
    :trade_engine_title,
    :security_type_name,
    :security_type_title,
    :security_group_name,
    :tech$load_id
  )
