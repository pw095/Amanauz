INSERT
  INTO tech$foreign_currency_rate
  (
    tech$load_id,
    tech$effective_dt,
    tech$expiration_dt,
    tech$last_seen_dt,
    tech$hash_value,
    trade_date,
    id,
    nominal,
    value
  )
WITH
     w_raw AS
     (
         SELECT
                tech$load_dt,
                sha1(concat_value) AS hash_value,
                trade_date,
                id,
                nominal,
                value
           FROM (SELECT
                        tech$load_dt,
                        '_' || IFNULL(CAST(nominal AS TEXT), '!@#$%^&*') ||
                        '_' || IFNULL(CAST(value   AS TEXT), '!@#$%^&*') || '_' AS concat_value,
                        trade_date,
                        id,
                        nominal,
                        value
                   FROM (SELECT
                                tech$load_dt,
                                ROW_NUMBER() OVER (wnd) AS rn,
                                trade_date,
                                id,
                                nominal,
                                value
                           FROM (SELECT
                                        DATE(tech$load_dttm) AS tech$load_dt,
                                        tech$load_dttm       AS tech$load_dttm,
                                        trade_date,
                                        id,
                                        nominal,
                                        value
                                   FROM src.foreign_currency_rate
                                  WHERE tech$load_dttm >= :tech$load_dttm)
                         WINDOW wnd AS (PARTITION BY
                                                     trade_date,
                                                     id,
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
       trade_date,
       id,
       nominal,
       value
  FROM (SELECT
               tech$load_dt,
               tech$last_seen_dt,
               hash_value,
               trade_date,
               id,
               nominal,
               value
          FROM (SELECT
                       tech$load_dt,
                       MAX(tech$load_dt) OVER (win) AS tech$last_seen_dt,
                       hash_value,
                       LAG(hash_value) OVER (wnd)   AS lag_hash_value,
                       trade_date,
                       id,
                       nominal,
                       value
                  FROM w_raw
                WINDOW win AS (PARTITION BY
                                            trade_date,
                                            id),
                       wnd AS (PARTITION BY
                                            trade_date,
                                            id
                                   ORDER BY tech$load_dt))
         WHERE hash_value != lag_hash_value
            OR lag_hash_value IS NULL)
WINDOW wnd AS (PARTITION BY
                            trade_date,
                            id
                   ORDER BY tech$load_dt)
