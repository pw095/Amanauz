INSERT
  INTO foreign_currency_rate
  (
    tech$load_id,
    tech$load_dttm,
    trade_date,
    id,
    nominal,
    value
  )
VALUES
  (
    :tech$load_id,
    :tech$load_dttm,
    :trade_date,
    :id,
    :nominal,
    :value
  )
