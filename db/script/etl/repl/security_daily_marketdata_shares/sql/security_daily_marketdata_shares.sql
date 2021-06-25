INSERT
  INTO security_daily_marketdata_shares
  (
    tech$load_id,
    tech$effective_dt,
    tech$expiration_dt,
    tech$hash_value,
    security_id,
    board_id,
    bid,
    bid_depth,
    offer,
    offer_depth,
    spread,
    bid_deptht,
    offer_deptht,
    open,
    low,
    high,
    last,
    last_change,
    last_change_prcnt,
    qty,
    value,
    value_usd,
    wa_price,
    last_cng_to_last_wa_price_prcnt,
    wap_to_prev_wa_price_prcnt,
    wap_to_prev_wa_price,
    close_price,
    market_price_to_day,
    market_price,
    last_to_prev_price,
    num_trades,
    vol_to_day,
    val_to_day,
    val_to_day_usd,
    etf_settle_price,
    trading_status,
    update_time,
    admitted_quote,
    last_bid,
    last_offer,
    l_close_price,
    l_current_price,
    market_price_2,
    num_bids,
    num_offers,
    chage,
    time,
    high_bid,
    low_offer,
    price_minus_prev_wa_price,
    open_period_price,
    seq_num,
    sys_time,
    closing_auction_price,
    closing_auction_volume,
    issue_capitalization,
    issue_capitalization_update_time,
    etf_settle_currency,
    val_to_day_rur,
    trading_session
  )
SELECT
       :tech$load_id       AS tech$load_id,
       tech$effective_dt,
       tech$expiration_dt,
       tech$hash_value,
       security_id,
       board_id,
       bid,
       bid_depth,
       offer,
       offer_depth,
       spread,
       bid_deptht,
       offer_deptht,
       open,
       low,
       high,
       last,
       last_change,
       last_change_prcnt,
       qty,
       value,
       value_usd,
       wa_price,
       last_cng_to_last_wa_price_prcnt,
       wap_to_prev_wa_price_prcnt,
       wap_to_prev_wa_price,
       close_price,
       market_price_to_day,
       market_price,
       last_to_prev_price,
       num_trades,
       vol_to_day,
       val_to_day,
       val_to_day_usd,
       etf_settle_price,
       trading_status,
       update_time,
       admitted_quote,
       last_bid,
       last_offer,
       l_close_price,
       l_current_price,
       market_price_2,
       num_bids,
       num_offers,
       chage,
       time,
       high_bid,
       low_offer,
       price_minus_prev_wa_price,
       open_period_price,
       seq_num,
       sys_time,
       closing_auction_price,
       closing_auction_volume,
       issue_capitalization,
       issue_capitalization_update_time,
       etf_settle_currency,
       val_to_day_rur,
       trading_session
  FROM (SELECT
               tech$effective_dt,
               tech$expiration_dt,
               tech$hash_value,
               security_id,
               board_id,
               bid,
               bid_depth,
               offer,
               offer_depth,
               spread,
               bid_deptht,
               offer_deptht,
               open,
               low,
               high,
               last,
               last_change,
               last_change_prcnt,
               qty,
               value,
               value_usd,
               wa_price,
               last_cng_to_last_wa_price_prcnt,
               wap_to_prev_wa_price_prcnt,
               wap_to_prev_wa_price,
               close_price,
               market_price_to_day,
               market_price,
               last_to_prev_price,
               num_trades,
               vol_to_day,
               val_to_day,
               val_to_day_usd,
               etf_settle_price,
               trading_status,
               update_time,
               admitted_quote,
               last_bid,
               last_offer,
               l_close_price,
               l_current_price,
               market_price_2,
               num_bids,
               num_offers,
               chage,
               time,
               high_bid,
               low_offer,
               price_minus_prev_wa_price,
               open_period_price,
               seq_num,
               sys_time,
               closing_auction_price,
               closing_auction_volume,
               issue_capitalization,
               issue_capitalization_update_time,
               etf_settle_currency,
               val_to_day_rur,
               trading_session
          FROM tech$security_daily_marketdata_shares src
         WHERE NOT EXISTS(SELECT
                                 NULL
                            FROM security_daily_marketdata_shares sat
                           WHERE
                                 sat.security_id = src.security_id
                             AND sat.board_id = src.board_id
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
                        tech$sat$expiration_dt
               END AS tech$expiration_dt,
               tech$hash_value,
               security_id,
               board_id,
               bid,
               bid_depth,
               offer,
               offer_depth,
               spread,
               bid_deptht,
               offer_deptht,
               open,
               low,
               high,
               last,
               last_change,
               last_change_prcnt,
               qty,
               value,
               value_usd,
               wa_price,
               last_cng_to_last_wa_price_prcnt,
               wap_to_prev_wa_price_prcnt,
               wap_to_prev_wa_price,
               close_price,
               market_price_to_day,
               market_price,
               last_to_prev_price,
               num_trades,
               vol_to_day,
               val_to_day,
               val_to_day_usd,
               etf_settle_price,
               trading_status,
               update_time,
               admitted_quote,
               last_bid,
               last_offer,
               l_close_price,
               l_current_price,
               market_price_2,
               num_bids,
               num_offers,
               chage,
               time,
               high_bid,
               low_offer,
               price_minus_prev_wa_price,
               open_period_price,
               seq_num,
               sys_time,
               closing_auction_price,
               closing_auction_volume,
               issue_capitalization,
               issue_capitalization_update_time,
               etf_settle_currency,
               val_to_day_rur,
               trading_session
          FROM (SELECT
                       tech$effective_dt,
                       tech$expiration_dt,
                       tech$sat$effective_dt,
                       tech$sat$expiration_dt,
                       tech$hash_value,
                       security_id,
                       board_id,
                       bid,
                       bid_depth,
                       offer,
                       offer_depth,
                       spread,
                       bid_deptht,
                       offer_deptht,
                       open,
                       low,
                       high,
                       last,
                       last_change,
                       last_change_prcnt,
                       qty,
                       value,
                       value_usd,
                       wa_price,
                       last_cng_to_last_wa_price_prcnt,
                       wap_to_prev_wa_price_prcnt,
                       wap_to_prev_wa_price,
                       close_price,
                       market_price_to_day,
                       market_price,
                       last_to_prev_price,
                       num_trades,
                       vol_to_day,
                       val_to_day,
                       val_to_day_usd,
                       etf_settle_price,
                       trading_status,
                       update_time,
                       admitted_quote,
                       last_bid,
                       last_offer,
                       l_close_price,
                       l_current_price,
                       market_price_2,
                       num_bids,
                       num_offers,
                       chage,
                       time,
                       high_bid,
                       low_offer,
                       price_minus_prev_wa_price,
                       open_period_price,
                       seq_num,
                       sys_time,
                       closing_auction_price,
                       closing_auction_volume,
                       issue_capitalization,
                       issue_capitalization_update_time,
                       etf_settle_currency,
                       val_to_day_rur,
                       trading_session,
                       CASE
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
                               src.security_id,
                               src.board_id,
                               src.bid,
                               src.bid_depth,
                               src.offer,
                               src.offer_depth,
                               src.spread,
                               src.bid_deptht,
                               src.offer_deptht,
                               src.open,
                               src.low,
                               src.high,
                               src.last,
                               src.last_change,
                               src.last_change_prcnt,
                               src.qty,
                               src.value,
                               src.value_usd,
                               src.wa_price,
                               src.last_cng_to_last_wa_price_prcnt,
                               src.wap_to_prev_wa_price_prcnt,
                               src.wap_to_prev_wa_price,
                               src.close_price,
                               src.market_price_to_day,
                               src.market_price,
                               src.last_to_prev_price,
                               src.num_trades,
                               src.vol_to_day,
                               src.val_to_day,
                               src.val_to_day_usd,
                               src.etf_settle_price,
                               src.trading_status,
                               src.update_time,
                               src.admitted_quote,
                               src.last_bid,
                               src.last_offer,
                               src.l_close_price,
                               src.l_current_price,
                               src.market_price_2,
                               src.num_bids,
                               src.num_offers,
                               src.chage,
                               src.time,
                               src.high_bid,
                               src.low_offer,
                               src.price_minus_prev_wa_price,
                               src.open_period_price,
                               src.seq_num,
                               src.sys_time,
                               src.closing_auction_price,
                               src.closing_auction_volume,
                               src.issue_capitalization,
                               src.issue_capitalization_update_time,
                               src.etf_settle_currency,
                               src.val_to_day_rur,
                               src.trading_session,
                               FIRST_VALUE(CASE
                                                WHEN src.tech$hash_value != sat.tech$hash_value THEN
                                                    'NON_EQUAL'
                                                ELSE
                                                    'EQUAL'
                                           END) OVER (wnd) AS fv_equal_flag,
                               ROW_NUMBER() OVER (wnd)     AS rn
                          FROM tech$security_daily_marketdata_shares src
                               JOIN
                               security_daily_marketdata_shares sat
                                   ON
                                      sat.security_id = src.security_id
                                  AND sat.board_id = src.board_id
                                  AND sat.tech$effective_dt < src.tech$effective_dt
                                  AND sat.tech$expiration_dt = '2999-12-31'
                        WINDOW wnd AS (PARTITION BY src.security_id,
                                                    src.board_id
                                           ORDER BY src.tech$effective_dt))
                 WHERE rn = 1 AND fv_equal_flag = 'NON_EQUAL'
                    OR rn > 1) src
               CROSS JOIN
               (SELECT 'INSERT' AS flg
                 UNION ALL
                SELECT 'UPDATE' AS flg) mrg
    WHERE src.upsert_flg = 'UPSERT'
       OR src.upsert_flg = 'INSERT' AND mrg.flg = 'INSERT')
 WHERE 1 = 1
 ON CONFLICT(security_id, board_id, tech$effective_dt)
 DO UPDATE
       SET tech$expiration_dt = excluded.tech$expiration_dt
