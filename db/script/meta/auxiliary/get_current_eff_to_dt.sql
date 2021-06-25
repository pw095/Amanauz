SELECT
       IFNULL(rll.rll_effective_to_dt, flow_start_dttm) AS rll_effective_to_dt
  FROM (SELECT
               flow_id,
               elm_id,
               ell_id,
               flow_start_dttm,
               COUNT(*)             OVER (wnd) AS cnt,
               COUNT(succeeded_flg) OVER (wnd) AS succeeded_cnt,
               SUM(ell_iteration_insert_row_count) OVER (wnd) AS insert_row_cnt
          FROM (SELECT
                       flow.flow_id,
                       elm.elm_id,
                       ell.ell_id,
                       flow.flow_start_dttm,
                       CASE ell.ell_status
                            WHEN 'succeeded' THEN
                                1
                       END AS succeeded_flg,
                       ell.ell_iteration_insert_row_count
                  FROM tbl_entity_layer_map elm
                       JOIN
                       tbl_elm_dependency elmd
                           ON elmd.elmd_elm_id = elm.elm_id
                       JOIN
                       tbl_entity_load_log ell
                           ON ell.ell_elm_id = elmd.elmd_parent_elm_id
                       JOIN
                       tbl_flow_log flow
                           ON flow.flow_id = ell.ell_flow_log_id
                          AND flow.flow_status IN ('running', 'succeeded')
                 WHERE elm.elm_id = :elm_id)
        WINDOW wnd AS (PARTITION BY flow_id)) t
       LEFT JOIN
       tbl_relation_load_log rll
           ON rll.rll_ell_id = t.ell_id
 WHERE succeeded_cnt = cnt
   AND insert_row_cnt > 0
ORDER BY flow_id DESC
LIMIT 1
