INSERT
  INTO security_rate_bonds
  (
    tech$load_id,
    tech$load_dttm,
    board_id,
    trade_date,
    short_name,
    security_id,
    num_trades,
    value,
    low,
    high,
    close,
    legal_close_price,
    acc_int,
    wa_price,
    yield_close,
    open,
    volume,
    market_price_2,
    market_price_3,
    admitted_quote,
    mp2_val_trd,
    market_price_3_trades_value,
    admitted_value,
    mat_date,
    duration,
    yield_at_map,
    iri_cpi_close,
    bei_close,
    coupon_percent,
    coupon_value,
    buy_back_date,
    last_trade_date,
    face_value,
    currency_id,
    cbr_close,
    yield_to_offer,
    yield_last_coupon,
    offer_date,
    face_unit,
    trading_session
  )
VALUES
  (
    :tech$load_id,
    :tech$load_dttm,
    :board_id,
    :trade_date,
    :short_name,
    :security_id,
    :num_trades,
    :value,
    :low,
    :high,
    :close,
    :legal_close_price,
    :acc_int,
    :wa_price,
    :yield_close,
    :open,
    :volume,
    :market_price_2,
    :market_price_3,
    :admitted_quote,
    :mp2_val_trd,
    :market_price_3_trades_value,
    :admitted_value,
    :mat_date,
    :duration,
    :yield_at_map,
    :iri_cpi_close,
    :bei_close,
    :coupon_percent,
    :coupon_value,
    :buy_back_date,
    :last_trade_date,
    :face_value,
    :currency_id,
    :cbr_close,
    :yield_to_offer,
    :yield_last_coupon,
    :offer_date,
    :face_unit,
    :trading_session
  )