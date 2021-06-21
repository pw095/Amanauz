SELECT
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