INSERT
  INTO tech$security_markets
  (
    tech$load_id,
    tech$effective_dt,
    tech$expiration_dt,
    tech$last_seen_dt,
    tech$hash_value,
    id,
    trade_engine_id,
    trade_engine_name,
    trade_engine_title,
    market_name,
    market_title,
    market_id,
    market_place
  )
WITH
     w_raw AS
     (
         SELECT
                tech$load_dt,
                sha1(concat_value) AS hash_value,
                id,
                trade_engine_id,
                trade_engine_name,
                trade_engine_title,
                market_name,
                market_title,
                market_id,
                market_place
           FROM (SELECT
                        tech$load_dt,
                        '_' || IFNULL(CAST(id                 AS TEXT), '!@#$%^&*') ||
                        '_' || IFNULL(CAST(trade_engine_id    AS TEXT), '!@#$%^&*') ||
                        '_' || IFNULL(CAST(trade_engine_name  AS TEXT), '!@#$%^&*') ||
                        '_' || IFNULL(CAST(trade_engine_title AS TEXT), '!@#$%^&*') ||
                        '_' || IFNULL(CAST(market_title       AS TEXT), '!@#$%^&*') ||
                        '_' || IFNULL(CAST(market_id          AS TEXT), '!@#$%^&*') ||
                        '_' || IFNULL(CAST(market_place       AS TEXT), '!@#$%^&*') || '_' AS concat_value,
                        id,
                        trade_engine_id,
                        trade_engine_name,
                        trade_engine_title,
                        market_name,
                        market_title,
                        market_id,
                        market_place
                   FROM (SELECT
                                tech$load_dt,
                                ROW_NUMBER() OVER (wnd) AS rn,
                                id,
                                trade_engine_id,
                                trade_engine_name,
                                trade_engine_title,
                                market_name,
                                market_title,
                                market_id,
                                market_place
                           FROM (SELECT
                                        DATE(tech$load_dttm) AS tech$load_dt,
                                        tech$load_dttm       AS tech$load_dttm,
                                        id,
                                        trade_engine_id,
                                        trade_engine_name,
                                        trade_engine_title,
                                        market_name,
                                        market_title,
                                        market_id,
                                        market_place
                                   FROM src.security_markets
                                  WHERE tech$load_dttm >= :tech$load_dttm)
                         WINDOW wnd AS (PARTITION BY
                                                     market_name,
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
       id,
       trade_engine_id,
       trade_engine_name,
       trade_engine_title,
       market_name,
       market_title,
       market_id,
       market_place
  FROM (SELECT
               tech$load_dt,
               tech$last_seen_dt,
               hash_value,
               id,
               trade_engine_id,
               trade_engine_name,
               trade_engine_title,
               market_name,
               market_title,
               market_id,
               market_place
          FROM (SELECT
                       tech$load_dt,
                       MAX(tech$load_dt) OVER (win) AS tech$last_seen_dt,
                       hash_value,
                       LAG(hash_value) OVER (wnd)   AS lag_hash_value,
                       id,
                       trade_engine_id,
                       trade_engine_name,
                       trade_engine_title,
                       market_name,
                       market_title,
                       market_id,
                       market_place
                  FROM w_raw
                WINDOW win AS (PARTITION BY
                                            market_name),
                       wnd AS (PARTITION BY
                                            market_name
                                   ORDER BY tech$load_dt))
         WHERE hash_value != lag_hash_value
            OR lag_hash_value IS NULL)
WINDOW wnd AS (PARTITION BY
                            market_name
                   ORDER BY tech$load_dt)
