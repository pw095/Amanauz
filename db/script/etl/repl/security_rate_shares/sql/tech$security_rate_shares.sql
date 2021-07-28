INSERT
  INTO tech$security_rate_shares
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
WITH
     w_raw AS
     (
         SELECT
                tech$load_dt,
                sha1(concat_value) AS hash_value,
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
                        tech$load_dt,
                        '_' || IFNULL(CAST(short_name                  AS TEXT), '!@#$%^&*') ||
                        '_' || IFNULL(CAST(num_trades                  AS TEXT), '!@#$%^&*') ||
                        '_' || IFNULL(CAST(value                       AS TEXT), '!@#$%^&*') ||
                        '_' || IFNULL(CAST(open                        AS TEXT), '!@#$%^&*') ||
                        '_' || IFNULL(CAST(low                         AS TEXT), '!@#$%^&*') ||
                        '_' || IFNULL(CAST(high                        AS TEXT), '!@#$%^&*') ||
                        '_' || IFNULL(CAST(legal_close_price           AS TEXT), '!@#$%^&*') ||
                        '_' || IFNULL(CAST(wa_price                    AS TEXT), '!@#$%^&*') ||
                        '_' || IFNULL(CAST(close                       AS TEXT), '!@#$%^&*') ||
                        '_' || IFNULL(CAST(volume                      AS TEXT), '!@#$%^&*') ||
                        '_' || IFNULL(CAST(market_price_2              AS TEXT), '!@#$%^&*') ||
                        '_' || IFNULL(CAST(market_price_3              AS TEXT), '!@#$%^&*') ||
                        '_' || IFNULL(CAST(admitted_quote              AS TEXT), '!@#$%^&*') ||
                        '_' || IFNULL(CAST(mp2_val_trd                 AS TEXT), '!@#$%^&*') ||
                        '_' || IFNULL(CAST(market_price_3_trades_value AS TEXT), '!@#$%^&*') ||
                        '_' || IFNULL(CAST(admitted_value              AS TEXT), '!@#$%^&*') ||
                        '_' || IFNULL(CAST(wa_val                      AS TEXT), '!@#$%^&*') ||
                        '_' || IFNULL(CAST(trading_session             AS TEXT), '!@#$%^&*') || '_' AS concat_value,
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
                                tech$load_dt,
                                ROW_NUMBER() OVER (wnd) AS rn,
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
                                        DATE(tech$load_dttm) AS tech$load_dt,
                                        tech$load_dttm       AS tech$load_dttm,
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
                                   FROM src.security_rate_shares
                                  WHERE tech$load_dttm >= :tech$load_dttm)
                         WINDOW wnd AS (PARTITION BY
                                                     board_id,
                                                     trade_date,
                                                     security_id,
                                                     tech$load_dt
                                            ORDER BY tech$load_dttm DESC))
                  WHERE rn = 1)
     )
SELECT
       :tech$load_id                                                  AS tech$load_id,
       tech$load_dt                                                   AS tech$effective_dt,
       LEAD(DATE(tech$load_dt, '-1 DAY'), 1, '2999-12-31') OVER (wnd) AS tech$expiration_dt,
       tech$last_seen_dt,
       hash_value                                                     AS tech$hash_value,
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
               tech$load_dt,
               tech$last_seen_dt,
               hash_value,
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
                       tech$load_dt,
                       MAX(tech$load_dt) OVER (win) AS tech$last_seen_dt,
                       hash_value,
                       LAG(hash_value) OVER (wnd)   AS lag_hash_value,
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
                  FROM w_raw
                WINDOW win AS (PARTITION BY
                                            board_id,
                                            trade_date,
                                            security_id),
                       wnd AS (PARTITION BY
                                            board_id,
                                            trade_date,
                                            security_id
                                   ORDER BY tech$load_dt))
         WHERE hash_value != lag_hash_value
            OR lag_hash_value IS NULL)
WINDOW wnd AS (PARTITION BY
                            board_id,
                            trade_date,
                            security_id
                   ORDER BY tech$load_dt)
