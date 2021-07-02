INSERT
  INTO tech$sat_emitent_moex
  (
    tech$load_id,
    tech$hash_key,
    tech$effective_dt,
    tech$expiration_dt,
    tech$record_source,
    tech$hash_value,
    id,
    title,
    inn,
    okpo
  )
WITH
     w_raw AS
     (
         SELECT
                tech$effective_dt,
                hash_value,
                emitent_id,
                emitent_title,
                emitent_inn,
                emitent_okpo
           FROM (SELECT
                        tech$effective_dt,
                        hash_value,
                        LAG(hash_value) OVER (PARTITION BY wnd) AS lag_hash_value,
                        emitent_id,
                        emitent_title,
                        emitent_inn,
                        emitent_okpo
                   FROM (SELECT
                                tech$effective_dt,
                                sha1(cancat_value) AS hash_value,
                                emitent_id,
                                emitent_title,
                                emitent_inn,
                                emitent_okpo
                           FROM (SELECT
                                        tech$effective_dt,
                                        '_' || IFNULL(CAST(emitent_id   AS TEXT), '!@#$%^&*') ||
                                        '_' || IFNULL(CAST(emitent_inn  AS TEXT), '!@#$%^&*') ||
                                        '_' || IFNULL(CAST(emitent_okpo AS TEXT), '!@#$%^&*') || '_' AS concat_value,
                                        emitent_id,
                                        emitent_title,
                                        emitent_inn,
                                        emitent_okpo
                                   FROM src.security_emitent_map
                                  WHERE tech$effective_dt > :tech$effective_dt
                                    AND emitent_title IS NOT NULL
                                    AND emitent_title != ""))
                 WINDOW wnd AS (PARTITION BY emitent_title
                                    ORDER BY tech$effective_dt))
          WHERE hash_value != lag_hash_value
             OR lag_hash_value IS NULL
     )
SELECT
       :tech$load_id                                                           AS tech$load_id,
       hub.tech$hash_key,
       pre.tech$effective_dt,
       LEAD(DATE(pre.tech$effective_dt, '-1 DAY'), 1, '2999-12-31') OVER (wnd) AS tech$expiration_dt,
       hub.tech$record_source,
       pre.hash_value                                                          AS tech$hash_value,
       pre.emitent_id                                                          AS id,
       pre.emitent_title                                                       AS title,
       pre.emitent_inn                                                         AS inn,
       pre.emitent_okpo                                                        AS okpo
  FROM w_pre pre
       JOIN
       hub_emitent hub
           ON hub.code = pre.emitent_title
          AND hub.tech$record_source = 'moex.com'
WINDOW wnd AS (PARTITION BY pre.emitent_title
                   ORDER BY pre.tech$effective_dt)
