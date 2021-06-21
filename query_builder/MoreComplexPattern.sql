INSERT
  INTO repl.tech$index_security_weight
  (
    tech$load_id,
    tech$effective_dt,
    tech$expiration_dt,
    tech$hash_value,
    %businessFields%
  )
WITH
     w_raw AS
     (
         SELECT
                tech$load_dt,
                sha1(concat_value) AS hash_value,
                %businessFields%
           FROM (SELECT
                        tech$load_dt,
                        '_' || IFNULL(CAST(ticker          AS TEXT), '!@#$%^&*') ||
                        '_' || IFNULL(CAST(short_name      AS TEXT), '!@#$%^&*') ||
                        '_' || IFNULL(CAST(weight          AS TEXT), '!@#$%^&*') ||
                        '_' || IFNULL(CAST(trading_session AS TEXT), '!@#$%^&*') || '_' AS concat_value,
                        %businessFields%
                   FROM (SELECT
                                tech$load_dt,
                                ROW_NUMBER() OVER (wnd) AS rn,
                                %businessFields%
                           FROM (SELECT
                                        DATE(tech$load_dttm) AS tech$load_dt,
                                        tech$load_dttm       AS tech$load_dttm,
                                        %businessFields%
                                   FROM index_security_weight
                                  WHERE tech$load_id >= :tech$load_id)
                         WINDOW wnd AS (PARTITION BY
                                                     %businessFieldsInKey%,
                                                     tech$load_dt
                                            ORDER BY tech$load_dttm DESC))
                  WHERE rn = 1)
     )
SELECT
       1                                                              AS tech$load_id,
       tech$load_dt                                                   AS tech$effective_dt,
       LEAD(DATE(tech$load_dt, '-1 DAY'), 1, '2999-12-31') OVER (wnd) AS tech$expiration_dt,
       hash_value                                                     AS tech$hash_value,
       %businessFields%
  FROM (SELECT
               tech$load_dt,
               hash_value,
          FROM (SELECT
                       tech$load_dt,
                       hash_value,
                       LAG(hash_value) OVER (wnd) AS lag_hash_value,
                       %businessFields%
                  FROM w_raw
                WINDOW wnd AS (PARTITION BY
                                   ORDER BY tech$load_dt))
         WHERE hash_value != lag_hash_value
            OR lag_hash_value IS NULL)
WINDOW wnd AS (PARTITION BY index_id,
                            trade_date,
                            security_id
                   ORDER BY tech$load_dt)