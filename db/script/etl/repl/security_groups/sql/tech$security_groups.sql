INSERT
  INTO tech$security_groups
  (
    tech$load_id,
    tech$effective_dt,
    tech$expiration_dt,
    tech$hash_value,
    id,
    name,
    title,
    is_hidden
  )
WITH
     w_raw AS
     (
         SELECT
                tech$load_dt,
                sha1(concat_value) AS hash_value,
                id,
                name,
                title,
                is_hidden
           FROM (SELECT
                        tech$load_dt,
                        '_' || IFNULL(CAST(id        AS TEXT), '!@#$%^&*') ||
                        '_' || IFNULL(CAST(title     AS TEXT), '!@#$%^&*') ||
                        '_' || IFNULL(CAST(is_hidden AS TEXT), '!@#$%^&*') || '_' AS concat_value,
                        id,
                        name,
                        title,
                        is_hidden
                   FROM (SELECT
                                tech$load_dt,
                                ROW_NUMBER() OVER (wnd) AS rn,
                                id,
                                name,
                                title,
                                is_hidden
                           FROM (SELECT
                                        DATE(tech$load_dttm) AS tech$load_dt,
                                        tech$load_dttm       AS tech$load_dttm,
                                        id,
                                        name,
                                        title,
                                        is_hidden
                                   FROM src.security_groups
                                  WHERE tech$load_dttm > :tech$load_dttm)
                         WINDOW wnd AS (PARTITION BY
                                                     name,
                                                     tech$load_dt
                                            ORDER BY tech$load_dttm DESC))
                  WHERE rn = 1)
     )
SELECT
       :tech$load_id                                                  AS tech$load_id,
       tech$load_dt                                                   AS tech$effective_dt,
       LEAD(DATE(tech$load_dt, '-1 DAY'), 1, '2999-12-31') OVER (wnd) AS tech$expiration_dt,
       hash_value                                                     AS tech$hash_value,
       id,
       name,
       title,
       is_hidden
  FROM (SELECT
               tech$load_dt,
               hash_value,
               id,
               name,
               title,
               is_hidden
          FROM (SELECT
                       tech$load_dt,
                       hash_value,
                       LAG(hash_value) OVER (wnd) AS lag_hash_value,
                       id,
                       name,
                       title,
                       is_hidden
                  FROM w_raw
                WINDOW wnd AS (PARTITION BY
                                            name
                                   ORDER BY tech$load_dt))
         WHERE hash_value != lag_hash_value
            OR lag_hash_value IS NULL)
WINDOW wnd AS (PARTITION BY
                            name
                   ORDER BY tech$load_dt)