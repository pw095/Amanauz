INSERT
  INTO tech$sat_emitent_moex
  (
    tech$load_id,
    tech$hash_key,
    tech$effective_dt,
    tech$expiration_dt,
    tech$record_source,
    tech$hash_value,
    title,
    inn,
    okpo
  )
WITH
     w_raw AS
     (
         SELECT
                tech$effective_dt,
                emitent_title,
                sha1(concat_value) AS hash_value,
                emitent_inn,
                emitent_okpo,
                security_is_traded
           FROM (SELECT
                        tech$effective_dt,
                        emitent_title,
                        '_' || IFNULL(CAST(emitent_inn  AS TEXT), '!@#$%^&*') ||
                        '_' || IFNULL(CAST(emitent_okpo AS TEXT), '!@#$%^&*') || '_' AS concat_value,
                        emitent_inn,
                        emitent_okpo,
                        security_is_traded
                   FROM (SELECT
                                tech$effective_dt,
                                emitent_title,
                                NULLIF(emitent_inn, '-')  AS emitent_inn,
                                NULLIF(emitent_okpo, '-') AS emitent_okpo,
                                security_is_traded
                           FROM src.security_emitent_map
                          WHERE tech$effective_dt >= :tech$effective_dt
                            AND NULLIF(emitent_title, '') IS NOT NULL))
     ),
     w_cnt AS
     (
         SELECT
                tech$effective_dt,
                emitent_title,
                COUNT(DISTINCT hash_value) AS hash_value_cnt
           FROM w_raw
          GROUP BY
                   tech$effective_dt,
                   emitent_title
     ),
     w_pre AS
     (
         SELECT
                tech$effective_dt,
                hash_value,
                emitent_title,
                emitent_inn,
                emitent_okpo
           FROM (SELECT
                        tech$effective_dt,
                        hash_value,
                        LAG(hash_value) OVER (wnd) AS lag_hash_value,
                        emitent_title,
                        emitent_inn,
                        emitent_okpo
                   FROM (SELECT
                                DISTINCT
                                tech$effective_dt,
                                hash_value,
                                emitent_title,
                                emitent_inn,
                                emitent_okpo
                           FROM w_raw raw
                          WHERE EXISTS(SELECT
                                              NULL
                                         FROM w_cnt cnt
                                        WHERE cnt.tech$effective_dt = raw.tech$effective_dt
                                          AND cnt.emitent_title = raw.emitent_title
                                          AND (   cnt.hash_value_cnt = 1
                                               OR cnt.hash_value_cnt > 1 AND raw.security_is_traded = 1)))
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
