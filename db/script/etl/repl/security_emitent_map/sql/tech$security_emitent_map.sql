INSERT
  INTO tech$security_emitent_map
  (
    tech$load_id,
    tech$effective_dt,
    tech$expiration_dt,
    tech$hash_value,
    id,
    security_id,
    short_name,
    reg_number,
    security_name,
    isin,
    security_is_traded,
    emitent_id,
    emitent_title,
    emitent_inn,
    emitent_okpo,
    emitent_gos_reg,
    security_type,
    security_group,
    primary_board_id,
    market_price_board_id
  )
WITH
     w_raw AS
     (
         SELECT
                tech$load_dt,
                sha1(concat_value) AS hash_value,
                id,
                security_id,
                short_name,
                reg_number,
                security_name,
                isin,
                security_is_traded,
                emitent_id,
                emitent_title,
                emitent_inn,
                emitent_okpo,
                emitent_gos_reg,
                security_type,
                security_group,
                primary_board_id,
                market_price_board_id
           FROM (SELECT
                        tech$load_dt,
                        '_' || IFNULL(CAST(id                    AS TEXT), '!@#$%^&*') ||
                        '_' || IFNULL(CAST(short_name            AS TEXT), '!@#$%^&*') ||
                        '_' || IFNULL(CAST(reg_number            AS TEXT), '!@#$%^&*') ||
                        '_' || IFNULL(CAST(security_name         AS TEXT), '!@#$%^&*') ||
                        '_' || IFNULL(CAST(isin                  AS TEXT), '!@#$%^&*') ||
                        '_' || IFNULL(CAST(security_is_traded    AS TEXT), '!@#$%^&*') ||
                        '_' || IFNULL(CAST(emitent_id            AS TEXT), '!@#$%^&*') ||
                        '_' || IFNULL(CAST(emitent_title         AS TEXT), '!@#$%^&*') ||
                        '_' || IFNULL(CAST(emitent_inn           AS TEXT), '!@#$%^&*') ||
                        '_' || IFNULL(CAST(emitent_okpo          AS TEXT), '!@#$%^&*') ||
                        '_' || IFNULL(CAST(emitent_gos_reg       AS TEXT), '!@#$%^&*') ||
                        '_' || IFNULL(CAST(security_type         AS TEXT), '!@#$%^&*') ||
                        '_' || IFNULL(CAST(security_group        AS TEXT), '!@#$%^&*') ||
                        '_' || IFNULL(CAST(primary_board_id      AS TEXT), '!@#$%^&*') ||
                        '_' || IFNULL(CAST(market_price_board_id AS TEXT), '!@#$%^&*') || '_' AS concat_value,
                        id,
                        security_id,
                        short_name,
                        reg_number,
                        security_name,
                        isin,
                        security_is_traded,
                        emitent_id,
                        emitent_title,
                        emitent_inn,
                        emitent_okpo,
                        emitent_gos_reg,
                        security_type,
                        security_group,
                        primary_board_id,
                        market_price_board_id
                   FROM (SELECT
                                tech$load_dt,
                                ROW_NUMBER() OVER (wnd) AS rn,
                                id,
                                security_id,
                                short_name,
                                reg_number,
                                security_name,
                                isin,
                                security_is_traded,
                                emitent_id,
                                emitent_title,
                                emitent_inn,
                                emitent_okpo,
                                emitent_gos_reg,
                                security_type,
                                security_group,
                                primary_board_id,
                                market_price_board_id
                           FROM (SELECT
                                        DATE(tech$load_dttm) AS tech$load_dt,
                                        tech$load_dttm       AS tech$load_dttm,
                                        id,
                                        security_id,
                                        short_name,
                                        reg_number,
                                        security_name,
                                        isin,
                                        security_is_traded,
                                        emitent_id,
                                        emitent_title,
                                        emitent_inn,
                                        emitent_okpo,
                                        emitent_gos_reg,
                                        security_type,
                                        security_group,
                                        primary_board_id,
                                        market_price_board_id
                                   FROM src.security_emitent_map
                                  WHERE tech$load_dttm >= :tech$load_dttm)
                         WINDOW wnd AS (PARTITION BY
                                                     security_id,
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
       security_id,
       short_name,
       reg_number,
       security_name,
       isin,
       security_is_traded,
       emitent_id,
       emitent_title,
       emitent_inn,
       emitent_okpo,
       emitent_gos_reg,
       security_type,
       security_group,
       primary_board_id,
       market_price_board_id
  FROM (SELECT
               tech$load_dt,
               hash_value,
               id,
               security_id,
               short_name,
               reg_number,
               security_name,
               isin,
               security_is_traded,
               emitent_id,
               emitent_title,
               emitent_inn,
               emitent_okpo,
               emitent_gos_reg,
               security_type,
               security_group,
               primary_board_id,
               market_price_board_id
          FROM (SELECT
                       tech$load_dt,
                       hash_value,
                       LAG(hash_value) OVER (wnd) AS lag_hash_value,
                       id,
                       security_id,
                       short_name,
                       reg_number,
                       security_name,
                       isin,
                       security_is_traded,
                       emitent_id,
                       emitent_title,
                       emitent_inn,
                       emitent_okpo,
                       emitent_gos_reg,
                       security_type,
                       security_group,
                       primary_board_id,
                       market_price_board_id
                  FROM w_raw
                WINDOW wnd AS (PARTITION BY
                                            security_id
                                   ORDER BY tech$load_dt))
         WHERE hash_value != lag_hash_value
            OR lag_hash_value IS NULL)
WINDOW wnd AS (PARTITION BY
                            security_id
                   ORDER BY tech$load_dt)
