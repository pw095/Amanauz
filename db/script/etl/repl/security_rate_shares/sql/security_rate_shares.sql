INSERT
  INTO security_rate_shares
  (
    tech$load_id,
    tech$effective_dt,
    tech$expiration_dt,
    tech$hash_value,
    board_id,
    trade_date,
    short_name,
    security_id,
    num_trades,
    value,
    open,
    low,
    high,
    legal_close_price,
    wa_price,
    close,
    volume,
    market_price_2,
    market_price_3,
    admitted_quote,
    mp2_val_trd,
    market_price_3_trades_value,
    admitted_value,
    wa_val,
    trading_session
  )
SELECT
       :tech$load_id       AS tech$load_id,
       tech$effective_dt,
       tech$expiration_dt,
       tech$hash_value,
       board_id,
       trade_date,
       short_name,
       security_id,
       num_trades,
       value,
       open,
       low,
       high,
       legal_close_price,
       wa_price,
       close,
       volume,
       market_price_2,
       market_price_3,
       admitted_quote,
       mp2_val_trd,
       market_price_3_trades_value,
       admitted_value,
       wa_val,
       trading_session
  FROM (SELECT
               tech$effective_dt,
               tech$expiration_dt,
               tech$hash_value,
               board_id,
               trade_date,
               short_name,
               security_id,
               num_trades,
               value,
               open,
               low,
               high,
               legal_close_price,
               wa_price,
               close,
               volume,
               market_price_2,
               market_price_3,
               admitted_quote,
               mp2_val_trd,
               market_price_3_trades_value,
               admitted_value,
               wa_val,
               trading_session
          FROM tech$security_rate_shares src
         WHERE NOT EXISTS(SELECT
                                 NULL
                            FROM security_rate_shares sat
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
               tech$hash_value,
               board_id,
               trade_date,
               short_name,
               security_id,
               num_trades,
               value,
               open,
               low,
               high,
               legal_close_price,
               wa_price,
               close,
               volume,
               market_price_2,
               market_price_3,
               admitted_quote,
               mp2_val_trd,
               market_price_3_trades_value,
               admitted_value,
               wa_val,
               trading_session
          FROM (SELECT
                       tech$effective_dt,
                       tech$expiration_dt,
                       tech$sat$effective_dt,
                       tech$sat$expiration_dt,
                       tech$hash_value,
                       board_id,
                       trade_date,
                       short_name,
                       security_id,
                       num_trades,
                       value,
                       open,
                       low,
                       high,
                       legal_close_price,
                       wa_price,
                       close,
                       volume,
                       market_price_2,
                       market_price_3,
                       admitted_quote,
                       mp2_val_trd,
                       market_price_3_trades_value,
                       admitted_value,
                       wa_val,
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
                               sat.tech$effective_dt                 AS tech$sat$effective_dt,
                               DATE(src.tech$effective_dt, '-1 DAY') AS tech$sat$expiration_dt,
                               src.tech$hash_value,
                               src.board_id,
                               src.trade_date,
                               src.short_name,
                               src.security_id,
                               src.num_trades,
                               src.value,
                               src.open,
                               src.low,
                               src.high,
                               src.legal_close_price,
                               src.wa_price,
                               src.close,
                               src.volume,
                               src.market_price_2,
                               src.market_price_3,
                               src.admitted_quote,
                               src.mp2_val_trd,
                               src.market_price_3_trades_value,
                               src.admitted_value,
                               src.wa_val,
                               src.trading_session,
                               FIRST_VALUE(CASE
                                                WHEN src.tech$hash_value != sat.tech$hash_value THEN
                                                    'NON_EQUAL'
                                                ELSE
                                                    'EQUAL'
                                           END) OVER (wnd) AS fv_equal_flag,
                               ROW_NUMBER() OVER (wnd)     AS rn
                          FROM tech$security_rate_shares src
                               JOIN
                               security_rate_shares sat
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
           tech$load_id = excluded.tech$load_id,
           short_name = CASE WHEN tech$expiration_dt = '2999-12-31' AND excluded.tech$expiration_dt = '2999-12-31' THEN excluded.short_name ELSE short_name END,
           num_trades = CASE WHEN tech$expiration_dt = '2999-12-31' AND excluded.tech$expiration_dt = '2999-12-31' THEN excluded.num_trades ELSE num_trades END,
           value = CASE WHEN tech$expiration_dt = '2999-12-31' AND excluded.tech$expiration_dt = '2999-12-31' THEN excluded.value ELSE value END,
           open = CASE WHEN tech$expiration_dt = '2999-12-31' AND excluded.tech$expiration_dt = '2999-12-31' THEN excluded.open ELSE open END,
           low = CASE WHEN tech$expiration_dt = '2999-12-31' AND excluded.tech$expiration_dt = '2999-12-31' THEN excluded.low ELSE low END,
           high = CASE WHEN tech$expiration_dt = '2999-12-31' AND excluded.tech$expiration_dt = '2999-12-31' THEN excluded.high ELSE high END,
           legal_close_price = CASE WHEN tech$expiration_dt = '2999-12-31' AND excluded.tech$expiration_dt = '2999-12-31' THEN excluded.legal_close_price ELSE legal_close_price END,
           wa_price = CASE WHEN tech$expiration_dt = '2999-12-31' AND excluded.tech$expiration_dt = '2999-12-31' THEN excluded.wa_price ELSE wa_price END,
           close = CASE WHEN tech$expiration_dt = '2999-12-31' AND excluded.tech$expiration_dt = '2999-12-31' THEN excluded.close ELSE close END,
           volume = CASE WHEN tech$expiration_dt = '2999-12-31' AND excluded.tech$expiration_dt = '2999-12-31' THEN excluded.volume ELSE volume END,
           market_price_2 = CASE WHEN tech$expiration_dt = '2999-12-31' AND excluded.tech$expiration_dt = '2999-12-31' THEN excluded.market_price_2 ELSE market_price_2 END,
           market_price_3 = CASE WHEN tech$expiration_dt = '2999-12-31' AND excluded.tech$expiration_dt = '2999-12-31' THEN excluded.market_price_3 ELSE market_price_3 END,
           admitted_quote = CASE WHEN tech$expiration_dt = '2999-12-31' AND excluded.tech$expiration_dt = '2999-12-31' THEN excluded.admitted_quote ELSE admitted_quote END,
           mp2_val_trd = CASE WHEN tech$expiration_dt = '2999-12-31' AND excluded.tech$expiration_dt = '2999-12-31' THEN excluded.mp2_val_trd ELSE mp2_val_trd END,
           market_price_3_trades_value = CASE WHEN tech$expiration_dt = '2999-12-31' AND excluded.tech$expiration_dt = '2999-12-31' THEN excluded.market_price_3_trades_value ELSE market_price_3_trades_value END,
           admitted_value = CASE WHEN tech$expiration_dt = '2999-12-31' AND excluded.tech$expiration_dt = '2999-12-31' THEN excluded.admitted_value ELSE admitted_value END,
           wa_val = CASE WHEN tech$expiration_dt = '2999-12-31' AND excluded.tech$expiration_dt = '2999-12-31' THEN excluded.wa_val ELSE wa_val END,
           trading_session = CASE WHEN tech$expiration_dt = '2999-12-31' AND excluded.tech$expiration_dt = '2999-12-31' THEN excluded.trading_session ELSE trading_session END
           
