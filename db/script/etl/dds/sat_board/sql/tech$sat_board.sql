INSERT
  INTO tech$sat_board
  (
    tech$load_id,
    tech$hash_key,
    tech$effective_dt,
    tech$expiration_dt,
    tech$record_source,
    tech$hash_value,
    board_id,
    title,
    primary_flag
  )
WITH
     w_pre AS
     (
         SELECT
                tech$effective_dt,
                hash_value,
                board_id,
                board_title,
                primary_flag
           FROM (SELECT
                        tech$effective_dt,
                        hash_value,
                        LAG(hash_value) OVER (wnd) AS lag_hash_value,
                        board_id,
                        board_title,
                        primary_flag
                   FROM (SELECT
                                tech$effective_dt,
                                sha1(concat_value) AS hash_value,
                                board_id,
                                board_title,
                                primary_flag
                           FROM (SELECT
                                        tech$effective_dt,
                                        '_' || IFNULL(CAST(board_title  AS TEXT), '!@#$%^&*') ||
                                        '_' || IFNULL(CAST(board_title  AS TEXT), '!@#$%^&*') ||
                                        '_' || IFNULL(CAST(primary_flag AS TEXT), '!@#$%^&*') || '_' AS concat_value,
                                        board_id,
                                        board_title,
                                        primary_flag
                                   FROM (SELECT
                                                tech$effective_dt,
                                                board_id,
                                                board_title,
                                                CASE is_primary
                                                     WHEN 1 THEN
                                                         'PRIMARY'
                                                     ELSE
                                                         'NOT_PRIMARY'
                                                END AS primary_flag
                                           FROM src.security_boards
                                          WHERE tech$effective_dt > :tech$effective_dt
                                            AND NULLIF(board_id, "") IS NOT NULL
                                            AND NULLIF(board_title, "") IS NOT NULL)))
                         WINDOW wnd AS (PARTITION BY board_id
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
       pre.board_id,
       pre.board_title                                                         AS title,
       pre.primary_flag                                                        AS primary_flag
  FROM w_pre pre
       JOIN
       hub_board hub
           ON hub.board_id = pre.board_id
          AND hub.tech$record_source = 'moex.com'
WINDOW wnd AS (PARTITION BY pre.board_id
                   ORDER BY pre.tech$effective_dt)
