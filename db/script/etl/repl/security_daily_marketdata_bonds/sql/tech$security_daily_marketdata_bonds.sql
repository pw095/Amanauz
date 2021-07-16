INSERT
  INTO tech$security_daily_marketdata_bonds
  (
    tech$load_id,
    tech$effective_dt,
    tech$expiration_dt,
    tech$hash_value,
    security_id,
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
    yield,
    value_usd,
    wa_price,
    last_cng_to_last_wa_price_prcnt,
    wap_to_prev_wa_price_prcnt,
    wap_to_prev_wa_price,
    yield_at_wa_price,
    yield_to_prev_yield,
    close_yield,
    close_price,
    market_price_to_day,
    market_price,
    last_to_prev_price,
    num_trades,
    vol_to_day,
    val_to_day,
    val_to_day_usd,
    board_id,
    trading_status,
    update_time,
    duration,
    num_bids,
    num_offers,
    change,
    time,
    high_bid,
    low_offer,
    price_minus_prev_wa_price,
    last_bid,
    last_offer,
    l_current_price,
    l_close_price,
    market_price_2,
    admitted_quote,
    open_period_price,
    seq_num,
    sys_time,
    val_to_day_rur,
    iri_cpi_close,
    bei_close,
    cbr_close,
    yield_to_offer,
    yield_last_coupon,
    trading_session
  )
WITH
     w_raw AS
     (
         SELECT
                tech$load_dt,
                sha1(concat_value) AS hash_value,
                security_id,
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
                yield,
                value_usd,
                wa_price,
                last_cng_to_last_wa_price_prcnt,
                wap_to_prev_wa_price_prcnt,
                wap_to_prev_wa_price,
                yield_at_wa_price,
                yield_to_prev_yield,
                close_yield,
                close_price,
                market_price_to_day,
                market_price,
                last_to_prev_price,
                num_trades,
                vol_to_day,
                val_to_day,
                val_to_day_usd,
                board_id,
                trading_status,
                update_time,
                duration,
                num_bids,
                num_offers,
                change,
                time,
                high_bid,
                low_offer,
                price_minus_prev_wa_price,
                last_bid,
                last_offer,
                l_current_price,
                l_close_price,
                market_price_2,
                admitted_quote,
                open_period_price,
                seq_num,
                sys_time,
                val_to_day_rur,
                iri_cpi_close,
                bei_close,
                cbr_close,
                yield_to_offer,
                yield_last_coupon,
                trading_session
           FROM (SELECT
                        tech$load_dt,
                        '_' || IFNULL(CAST(bid                             AS TEXT), '!@#$%^&*') ||
                        '_' || IFNULL(CAST(bid_depth                       AS TEXT), '!@#$%^&*') ||
                        '_' || IFNULL(CAST(offer                           AS TEXT), '!@#$%^&*') ||
                        '_' || IFNULL(CAST(offer_depth                     AS TEXT), '!@#$%^&*') ||
                        '_' || IFNULL(CAST(spread                          AS TEXT), '!@#$%^&*') ||
                        '_' || IFNULL(CAST(bid_deptht                      AS TEXT), '!@#$%^&*') ||
                        '_' || IFNULL(CAST(offer_deptht                    AS TEXT), '!@#$%^&*') ||
                        '_' || IFNULL(CAST(open                            AS TEXT), '!@#$%^&*') ||
                        '_' || IFNULL(CAST(low                             AS TEXT), '!@#$%^&*') ||
                        '_' || IFNULL(CAST(high                            AS TEXT), '!@#$%^&*') ||
                        '_' || IFNULL(CAST(last                            AS TEXT), '!@#$%^&*') ||
                        '_' || IFNULL(CAST(last_change                     AS TEXT), '!@#$%^&*') ||
                        '_' || IFNULL(CAST(last_change_prcnt               AS TEXT), '!@#$%^&*') ||
                        '_' || IFNULL(CAST(qty                             AS TEXT), '!@#$%^&*') ||
                        '_' || IFNULL(CAST(value                           AS TEXT), '!@#$%^&*') ||
                        '_' || IFNULL(CAST(yield                           AS TEXT), '!@#$%^&*') ||
                        '_' || IFNULL(CAST(value_usd                       AS TEXT), '!@#$%^&*') ||
                        '_' || IFNULL(CAST(wa_price                        AS TEXT), '!@#$%^&*') ||
                        '_' || IFNULL(CAST(last_cng_to_last_wa_price_prcnt AS TEXT), '!@#$%^&*') ||
                        '_' || IFNULL(CAST(wap_to_prev_wa_price_prcnt      AS TEXT), '!@#$%^&*') ||
                        '_' || IFNULL(CAST(wap_to_prev_wa_price            AS TEXT), '!@#$%^&*') ||
                        '_' || IFNULL(CAST(yield_at_wa_price               AS TEXT), '!@#$%^&*') ||
                        '_' || IFNULL(CAST(yield_to_prev_yield             AS TEXT), '!@#$%^&*') ||
                        '_' || IFNULL(CAST(close_yield                     AS TEXT), '!@#$%^&*') ||
                        '_' || IFNULL(CAST(close_price                     AS TEXT), '!@#$%^&*') ||
                        '_' || IFNULL(CAST(market_price_to_day             AS TEXT), '!@#$%^&*') ||
                        '_' || IFNULL(CAST(market_price                    AS TEXT), '!@#$%^&*') ||
                        '_' || IFNULL(CAST(last_to_prev_price              AS TEXT), '!@#$%^&*') ||
                        '_' || IFNULL(CAST(num_trades                      AS TEXT), '!@#$%^&*') ||
                        '_' || IFNULL(CAST(vol_to_day                      AS TEXT), '!@#$%^&*') ||
                        '_' || IFNULL(CAST(val_to_day                      AS TEXT), '!@#$%^&*') ||
                        '_' || IFNULL(CAST(val_to_day_usd                  AS TEXT), '!@#$%^&*') ||
                        '_' || IFNULL(CAST(trading_status                  AS TEXT), '!@#$%^&*') ||
                        '_' || IFNULL(CAST(update_time                     AS TEXT), '!@#$%^&*') ||
                        '_' || IFNULL(CAST(duration                        AS TEXT), '!@#$%^&*') ||
                        '_' || IFNULL(CAST(num_bids                        AS TEXT), '!@#$%^&*') ||
                        '_' || IFNULL(CAST(num_offers                      AS TEXT), '!@#$%^&*') ||
                        '_' || IFNULL(CAST(change                          AS TEXT), '!@#$%^&*') ||
                        '_' || IFNULL(CAST(time                            AS TEXT), '!@#$%^&*') ||
                        '_' || IFNULL(CAST(high_bid                        AS TEXT), '!@#$%^&*') ||
                        '_' || IFNULL(CAST(low_offer                       AS TEXT), '!@#$%^&*') ||
                        '_' || IFNULL(CAST(price_minus_prev_wa_price       AS TEXT), '!@#$%^&*') ||
                        '_' || IFNULL(CAST(last_bid                        AS TEXT), '!@#$%^&*') ||
                        '_' || IFNULL(CAST(last_offer                      AS TEXT), '!@#$%^&*') ||
                        '_' || IFNULL(CAST(l_current_price                 AS TEXT), '!@#$%^&*') ||
                        '_' || IFNULL(CAST(l_close_price                   AS TEXT), '!@#$%^&*') ||
                        '_' || IFNULL(CAST(market_price_2                  AS TEXT), '!@#$%^&*') ||
                        '_' || IFNULL(CAST(admitted_quote                  AS TEXT), '!@#$%^&*') ||
                        '_' || IFNULL(CAST(open_period_price               AS TEXT), '!@#$%^&*') ||
                        '_' || IFNULL(CAST(seq_num                         AS TEXT), '!@#$%^&*') ||
                        '_' || IFNULL(CAST(sys_time                        AS TEXT), '!@#$%^&*') ||
                        '_' || IFNULL(CAST(val_to_day_rur                  AS TEXT), '!@#$%^&*') ||
                        '_' || IFNULL(CAST(iri_cpi_close                   AS TEXT), '!@#$%^&*') ||
                        '_' || IFNULL(CAST(bei_close                       AS TEXT), '!@#$%^&*') ||
                        '_' || IFNULL(CAST(cbr_close                       AS TEXT), '!@#$%^&*') ||
                        '_' || IFNULL(CAST(yield_to_offer                  AS TEXT), '!@#$%^&*') ||
                        '_' || IFNULL(CAST(yield_last_coupon               AS TEXT), '!@#$%^&*') ||
                        '_' || IFNULL(CAST(trading_session                 AS TEXT), '!@#$%^&*') || '_' AS concat_value,
                        security_id,
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
                        yield,
                        value_usd,
                        wa_price,
                        last_cng_to_last_wa_price_prcnt,
                        wap_to_prev_wa_price_prcnt,
                        wap_to_prev_wa_price,
                        yield_at_wa_price,
                        yield_to_prev_yield,
                        close_yield,
                        close_price,
                        market_price_to_day,
                        market_price,
                        last_to_prev_price,
                        num_trades,
                        vol_to_day,
                        val_to_day,
                        val_to_day_usd,
                        board_id,
                        trading_status,
                        update_time,
                        duration,
                        num_bids,
                        num_offers,
                        change,
                        time,
                        high_bid,
                        low_offer,
                        price_minus_prev_wa_price,
                        last_bid,
                        last_offer,
                        l_current_price,
                        l_close_price,
                        market_price_2,
                        admitted_quote,
                        open_period_price,
                        seq_num,
                        sys_time,
                        val_to_day_rur,
                        iri_cpi_close,
                        bei_close,
                        cbr_close,
                        yield_to_offer,
                        yield_last_coupon,
                        trading_session
                   FROM (SELECT
                                tech$load_dt,
                                ROW_NUMBER() OVER (wnd) AS rn,
                                security_id,
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
                                yield,
                                value_usd,
                                wa_price,
                                last_cng_to_last_wa_price_prcnt,
                                wap_to_prev_wa_price_prcnt,
                                wap_to_prev_wa_price,
                                yield_at_wa_price,
                                yield_to_prev_yield,
                                close_yield,
                                close_price,
                                market_price_to_day,
                                market_price,
                                last_to_prev_price,
                                num_trades,
                                vol_to_day,
                                val_to_day,
                                val_to_day_usd,
                                board_id,
                                trading_status,
                                update_time,
                                duration,
                                num_bids,
                                num_offers,
                                change,
                                time,
                                high_bid,
                                low_offer,
                                price_minus_prev_wa_price,
                                last_bid,
                                last_offer,
                                l_current_price,
                                l_close_price,
                                market_price_2,
                                admitted_quote,
                                open_period_price,
                                seq_num,
                                sys_time,
                                val_to_day_rur,
                                iri_cpi_close,
                                bei_close,
                                cbr_close,
                                yield_to_offer,
                                yield_last_coupon,
                                trading_session
                           FROM (SELECT
                                        DATE(tech$load_dttm) AS tech$load_dt,
                                        tech$load_dttm       AS tech$load_dttm,
                                        security_id,
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
                                        yield,
                                        value_usd,
                                        wa_price,
                                        last_cng_to_last_wa_price_prcnt,
                                        wap_to_prev_wa_price_prcnt,
                                        wap_to_prev_wa_price,
                                        yield_at_wa_price,
                                        yield_to_prev_yield,
                                        close_yield,
                                        close_price,
                                        market_price_to_day,
                                        market_price,
                                        last_to_prev_price,
                                        num_trades,
                                        vol_to_day,
                                        val_to_day,
                                        val_to_day_usd,
                                        board_id,
                                        trading_status,
                                        update_time,
                                        duration,
                                        num_bids,
                                        num_offers,
                                        change,
                                        time,
                                        high_bid,
                                        low_offer,
                                        price_minus_prev_wa_price,
                                        last_bid,
                                        last_offer,
                                        l_current_price,
                                        l_close_price,
                                        market_price_2,
                                        admitted_quote,
                                        open_period_price,
                                        seq_num,
                                        sys_time,
                                        val_to_day_rur,
                                        iri_cpi_close,
                                        bei_close,
                                        cbr_close,
                                        yield_to_offer,
                                        yield_last_coupon,
                                        trading_session
                                   FROM src.security_daily_marketdata_bonds
                                  WHERE tech$load_dttm >= :tech$load_dttm)
                         WINDOW wnd AS (PARTITION BY
                                                     security_id,
                                                     board_id,
                                                     tech$load_dt
                                            ORDER BY tech$load_dttm DESC))
                  WHERE rn = 1)
     )
SELECT
       :tech$load_id                                                  AS tech$load_id,
       tech$load_dt                                                   AS tech$effective_dt,
       LEAD(DATE(tech$load_dt, '-1 DAY'), 1, '2999-12-31') OVER (wnd) AS tech$expiration_dt,
       hash_value                                                     AS tech$hash_value,
       security_id,
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
       yield,
       value_usd,
       wa_price,
       last_cng_to_last_wa_price_prcnt,
       wap_to_prev_wa_price_prcnt,
       wap_to_prev_wa_price,
       yield_at_wa_price,
       yield_to_prev_yield,
       close_yield,
       close_price,
       market_price_to_day,
       market_price,
       last_to_prev_price,
       num_trades,
       vol_to_day,
       val_to_day,
       val_to_day_usd,
       board_id,
       trading_status,
       update_time,
       duration,
       num_bids,
       num_offers,
       change,
       time,
       high_bid,
       low_offer,
       price_minus_prev_wa_price,
       last_bid,
       last_offer,
       l_current_price,
       l_close_price,
       market_price_2,
       admitted_quote,
       open_period_price,
       seq_num,
       sys_time,
       val_to_day_rur,
       iri_cpi_close,
       bei_close,
       cbr_close,
       yield_to_offer,
       yield_last_coupon,
       trading_session
  FROM (SELECT
               tech$load_dt,
               hash_value,
               security_id,
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
               yield,
               value_usd,
               wa_price,
               last_cng_to_last_wa_price_prcnt,
               wap_to_prev_wa_price_prcnt,
               wap_to_prev_wa_price,
               yield_at_wa_price,
               yield_to_prev_yield,
               close_yield,
               close_price,
               market_price_to_day,
               market_price,
               last_to_prev_price,
               num_trades,
               vol_to_day,
               val_to_day,
               val_to_day_usd,
               board_id,
               trading_status,
               update_time,
               duration,
               num_bids,
               num_offers,
               change,
               time,
               high_bid,
               low_offer,
               price_minus_prev_wa_price,
               last_bid,
               last_offer,
               l_current_price,
               l_close_price,
               market_price_2,
               admitted_quote,
               open_period_price,
               seq_num,
               sys_time,
               val_to_day_rur,
               iri_cpi_close,
               bei_close,
               cbr_close,
               yield_to_offer,
               yield_last_coupon,
               trading_session
          FROM (SELECT
                       tech$load_dt,
                       hash_value,
                       LAG(hash_value) OVER (wnd) AS lag_hash_value,
                       security_id,
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
                       yield,
                       value_usd,
                       wa_price,
                       last_cng_to_last_wa_price_prcnt,
                       wap_to_prev_wa_price_prcnt,
                       wap_to_prev_wa_price,
                       yield_at_wa_price,
                       yield_to_prev_yield,
                       close_yield,
                       close_price,
                       market_price_to_day,
                       market_price,
                       last_to_prev_price,
                       num_trades,
                       vol_to_day,
                       val_to_day,
                       val_to_day_usd,
                       board_id,
                       trading_status,
                       update_time,
                       duration,
                       num_bids,
                       num_offers,
                       change,
                       time,
                       high_bid,
                       low_offer,
                       price_minus_prev_wa_price,
                       last_bid,
                       last_offer,
                       l_current_price,
                       l_close_price,
                       market_price_2,
                       admitted_quote,
                       open_period_price,
                       seq_num,
                       sys_time,
                       val_to_day_rur,
                       iri_cpi_close,
                       bei_close,
                       cbr_close,
                       yield_to_offer,
                       yield_last_coupon,
                       trading_session
                  FROM w_raw
                WINDOW wnd AS (PARTITION BY
                                            security_id,
                                            board_id
                                   ORDER BY tech$load_dt))
         WHERE hash_value != lag_hash_value
            OR lag_hash_value IS NULL)
WINDOW wnd AS (PARTITION BY
                            security_id,
                            board_id
                   ORDER BY tech$load_dt)
