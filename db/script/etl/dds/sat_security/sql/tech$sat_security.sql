INSERT
  INTO tech$sat_security
  (
    tech$load_id,
    tech$hash_key,
    tech$effective_dt,
    tech$expiration_dt,
    tech$record_source,
    tech$hash_value,
    security_id,
    full_name,
    short_name,
    isin,
    reg_number,
    issue_size,
    face_value,
    face_crnc,
    list_level
  )
WITH
     w_pre AS
     (
         SELECT
                tech$effective_dt,
                hash_value,
                security_id,
                full_name,
                short_name,
                isin,
                reg_number,
                issue_size,
                face_value,
                face_crnc,
                list_level,
                ROW_NUMBER() OVER (wnd) AS rn
           FROM (SELECT
                        tech$effective_dt,
                        hash_value,
                        LAG(hash_value) OVER (wnd) AS lag_hash_value,
                        security_id,
                        full_name,
                        short_name,
                        isin,
                        reg_number,
                        issue_size,
                        face_value,
                        face_crnc,
                        list_level
                   FROM (SELECT
                                tech$effective_dt,
                                sha1(concat_value) AS hash_value,
                                security_id,
                                full_name,
                                short_name,
                                isin,
                                reg_number,
                                issue_size,
                                face_value,
                                face_crnc,
                                list_level
                           FROM (SELECT
                                        tech$effective_dt,
                                        '_' || IFNULL(CAST(security_id AS TEXT), '!@#$%^&*') ||
                                        '_' || IFNULL(CAST(full_name   AS TEXT), '!@#$%^&*') ||
                                        '_' || IFNULL(CAST(short_name  AS TEXT), '!@#$%^&*') ||
                                        '_' || IFNULL(CAST(isin        AS TEXT), '!@#$%^&*') ||
                                        '_' || IFNULL(CAST(reg_number  AS TEXT), '!@#$%^&*') ||
                                        '_' || IFNULL(CAST(issue_size  AS TEXT), '!@#$%^&*') ||
                                        '_' || IFNULL(CAST(face_value  AS TEXT), '!@#$%^&*') ||
                                        '_' || IFNULL(CAST(face_crnc   AS TEXT), '!@#$%^&*') ||
                                        '_' || IFNULL(CAST(list_level  AS TEXT), '!@#$%^&*') || '_' AS concat_value,
                                        security_id,
                                        full_name,
                                        short_name,
                                        isin,
                                        reg_number,
                                        issue_size,
                                        face_value,
                                        face_crnc,
                                        list_level
                                   FROM (SELECT
                                                tech$effective_dt,
                                                security_id,
                                                full_name,
                                                short_name,
                                                isin,
                                                reg_number,
                                                issue_size,
                                                face_value,
                                                CASE face_crnc
                                                     WHEN 'SUR' THEN
                                                         'RUB'
                                                     ELSE
                                                         face_crnc
                                                END face_crnc,
                                                list_level
                                           FROM (SELECT
                                                        tech$effective_dt,
                                                        security_id,
                                                        security_name     AS full_name,
                                                        short_name,
                                                        isin,
                                                        reg_number,
                                                        issue_size,
                                                        face_value,
                                                        face_unit         AS face_crnc,
                                                        list_level
                                                   FROM src.security_daily_info_shares
                                                  UNION ALL
                                                 SELECT
                                                        tech$effective_dt,
                                                        security_id,
                                                        security_name     AS full_name,
                                                        short_name,
                                                        isin,
                                                        reg_number,
                                                        issue_size,
                                                        face_value,
                                                        face_unit         AS face_crnc,
                                                        list_level
                                                   FROM src.security_daily_info_bonds)
                                          WHERE tech$effective_dt >= :tech$effective_dt)))
                 WINDOW wnd AS (PARTITION BY security_id
                                    ORDER BY tech$effective_dt))
          WHERE hash_value != lag_hash_value
             OR lag_hash_value IS NULL
         WINDOW wnd AS (PARTITION BY security_id,
                                     tech$effective_dt
                            ORDER BY face_value DESC)
     )
SELECT
       :tech$load_id                                                           AS tech$load_id,
       hub.tech$hash_key,
       pre.tech$effective_dt,
       LEAD(DATE(pre.tech$effective_dt, '-1 DAY'), 1, '2999-12-31') OVER (wnd) AS tech$expiration_dt,
       hub.tech$record_source,
       pre.hash_value                                                          AS tech$hash_value,
       pre.security_id,
       pre.full_name,
       pre.short_name,
       pre.isin,
       pre.reg_number,
       pre.issue_size,
       pre.face_value,
       pre.face_crnc,
       pre.list_level
  FROM w_pre pre
       JOIN
       hub_security hub
           ON hub.security_id = pre.security_id
          AND hub.tech$record_source = 'moex.com'
 WHERE pre.rn = 1 -- Брать наибольший номинал, если у ЦБ он отличается в различных режимах торгов
                  -- Пока это относится только к индексируемым облигациям на этапе размещения
WINDOW wnd AS (PARTITION BY pre.security_id
                   ORDER BY pre.tech$effective_dt)
