INSERT
  INTO index_security_weight
  (
    tech$load_id,
    tech$load_dttm,
    index_id,
    trade_date,
    ticker,
    short_name,
    security_id,
    weight,
    trading_session
  )
VALUES
  (
    :tech$load_id,
    :tech$load_dttm,
    :index_id,
    :trade_date,
    :ticker,
    :short_name,
    :security_id,
    :weight,
    :trading_session
  )
