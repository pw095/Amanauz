INSERT
  INTO security_rate_bonds
  (
    tech$load_id,
    tech$effective_dt,
    tech$expiration_dt,
    tech$last_seen_dt,
    tech$hash_value,
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
SELECT
       :tech$load_id       AS tech$load_id,
       tech$effective_dt,
       tech$expiration_dt,
       tech$last_seen_dt,
       tech$hash_value,
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
  FROM (SELECT
               tech$effective_dt,
               tech$expiration_dt,
               tech$last_seen_dt,
               tech$hash_value,
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
          FROM tech$security_rate_bonds src
         WHERE NOT EXISTS(SELECT
                                 NULL
                            FROM security_rate_bonds sat
                           WHERE
                                 sat.board_id = src.board_id
                             AND sat.trade_date = src.trade_date
                             AND sat.security_id = src.security_id
                             AND sat.tech$expiration_dt = '2999-12-31')
         UNION ALL
        SELECT
               CASE mrg.flg
                    WHEN 'INSERT' THEN
                        tech$effective_dt
                    WHEN 'UPDATE' THEN
                        tech$sat$effective_dt
               END AS tech$effective_dt,
               CASE mrg.flg
                    WHEN 'INSERT' THEN
                        tech$expiration_dt
                    WHEN 'UPDATE' THEN
                        CASE upsert_flg
                             WHEN 'UPSERT' THEN
                                 tech$sat$expiration_dt
                             ELSE
                                 tech$expiration_dt
                        END
               END AS tech$expiration_dt,
               tech$last_seen_dt,
               tech$hash_value,
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
          FROM (SELECT
                       tech$effective_dt,
                       tech$expiration_dt,
                       tech$last_seen_dt,
                       tech$sat$effective_dt,
                       tech$sat$expiration_dt,
                       tech$hash_value,
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
                       trading_session,
                       CASE
                            WHEN rn = 1
                             AND fv_equal_flag = 'NON_EQUAL'
                             AND tech$effective_dt = tech$sat$effective_dt THEN
                                'UPDATE'
                            WHEN rn = 1
                              OR rn = 2 AND fv_equal_flag = 'EQUAL' THEN
                                'UPSERT'
                            ELSE
                                'INSERT'
                       END AS upsert_flg
                  FROM (SELECT
                               src.tech$effective_dt,
                               src.tech$expiration_dt,
                               src.tech$last_seen_dt,
                               sat.tech$effective_dt                 AS tech$sat$effective_dt,
                               DATE(src.tech$effective_dt, '-1 DAY') AS tech$sat$expiration_dt,
                               src.tech$hash_value,
                               src.board_id,
                               src.trade_date,
                               src.short_name,
                               src.security_id,
                               src.num_trades,
                               src.value,
                               src.low,
                               src.high,
                               src.close,
                               src.legal_close_price,
                               src.acc_int,
                               src.wa_price,
                               src.yield_close,
                               src.open,
                               src.volume,
                               src.market_price_2,
                               src.market_price_3,
                               src.admitted_quote,
                               src.mp2_val_trd,
                               src.market_price_3_trades_value,
                               src.admitted_value,
                               src.mat_date,
                               src.duration,
                               src.yield_at_map,
                               src.iri_cpi_close,
                               src.bei_close,
                               src.coupon_percent,
                               src.coupon_value,
                               src.buy_back_date,
                               src.last_trade_date,
                               src.face_value,
                               src.currency_id,
                               src.cbr_close,
                               src.yield_to_offer,
                               src.yield_last_coupon,
                               src.offer_date,
                               src.face_unit,
                               src.trading_session,
                               FIRST_VALUE(CASE
                                                WHEN src.tech$hash_value != sat.tech$hash_value THEN
                                                    'NON_EQUAL'
                                                ELSE
                                                    'EQUAL'
                                           END) OVER (wnd) AS fv_equal_flag,
                               ROW_NUMBER() OVER (wnd)     AS rn
                          FROM tech$security_rate_bonds src
                               JOIN
                               security_rate_bonds sat
                                   ON
                                      sat.board_id = src.board_id
                                  AND sat.trade_date = src.trade_date
                                  AND sat.security_id = src.security_id
                                  AND sat.tech$effective_dt <= src.tech$effective_dt
                                  AND sat.tech$expiration_dt = '2999-12-31'
                        WINDOW wnd AS (PARTITION BY src.board_id,
                                                    src.trade_date,
                                                    src.security_id
                                           ORDER BY src.tech$effective_dt))
                 WHERE rn = 1 AND fv_equal_flag = 'NON_EQUAL'
                    OR rn > 1) src
               CROSS JOIN
               (SELECT 'INSERT' AS flg
                 UNION ALL
                SELECT 'UPDATE' AS flg) mrg
    WHERE src.upsert_flg = 'UPSERT'
       OR src.upsert_flg = mrg.flg)
 WHERE 1 = 1
 ON CONFLICT(board_id, trade_date, security_id, tech$effective_dt)
 DO UPDATE
       SET tech$expiration_dt = excluded.tech$expiration_dt,
           tech$last_seen_dt = excluded.tech$last_seen_dt,
           tech$load_id = excluded.tech$load_id,
           tech$hash_value = CASE
                                  WHEN tech$expiration_dt = '2999-12-31'
                                   AND excluded.tech$expiration_dt = '2999-12-31' THEN
                                      excluded.tech$hash_value
                                  ELSE
                                      tech$hash_value
                             END
