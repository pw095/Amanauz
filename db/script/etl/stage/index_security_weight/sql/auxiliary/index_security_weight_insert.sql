INSERT
  INTO index_security_weight
  (
    index_name,
    trade_date,
    secid,
    weight,
    tech$load_id
  )
VALUES
  (
    :index_name,
    :trade_date,
    :secid,
    :weight,
    :tech$load_id
  )
